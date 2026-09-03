// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! HTTP order submission and cancellation facade for the Polymarket execution client.
//!
//! Accepts Nautilus-native types, handles conversion to Polymarket types,
//! order building, signing, and HTTP posting, following the dYdX OrderSubmitter pattern.
//!
//! Uses [`RetryManager`] from `nautilus-network` with exponential backoff for
//! transient HTTP failures (timeouts, 5xx, rate limits).

use std::{
    collections::HashSet,
    error::Error as StdError,
    fmt::Display,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, TimeInForce},
    identifiers::VenueOrderId,
    types::Quantity,
};
use nautilus_network::retry::{RetryConfig, RetryManager};
use rust_decimal::Decimal;

use super::{
    order_builder::PolymarketOrderBuilder,
    parse::{adjust_market_buy_amount, calculate_market_price},
    prepared::{
        PreparedOrderKind, PreparedOrderRequest, PreparedPolymarketOrder, requires_prepared_order,
        take_prepared_market_order, take_prepared_order,
    },
    types::{LimitOrderSubmitRequest, SignedLimitOrderSubmission},
};
use crate::{
    common::enums::{PolymarketOrderSide, PolymarketOrderType},
    http::{
        clob::PolymarketClobHttpClient,
        error::{Error, Result as HttpResult},
        models::{PolymarketOpenOrder, PolymarketOrder},
        query::{CancelResponse, OrderResponse},
    },
};

/// Fee-adjustment context for market BUYs sized to the user's pUSD balance.
///
/// When supplied to [`OrderSubmitter::submit_market_order`] alongside
/// `OrderSide::Buy`, the submitter shrinks `amount` so `amount + fees`
/// fits within `user_pusd_balance`, mirroring the SDK behaviour. SELL
/// orders ignore this context.
#[derive(Debug, Clone)]
pub(crate) struct MarketBuyFeeContext {
    pub user_pusd_balance: Decimal,
    pub fee_rate: Decimal,
    pub fee_exponent: f64,
    pub builder_taker_fee_rate: Decimal,
}

#[derive(Debug, Clone)]
pub(crate) struct MarketOrderSubmitRequest {
    pub(crate) client_order_id: String,
    pub(crate) account_id: String,
    pub(crate) profile_id: String,
    pub(crate) token_id: String,
    pub(crate) side: OrderSide,
    pub(crate) amount: Quantity,
    pub(crate) time_in_force: TimeInForce,
    pub(crate) neg_risk: bool,
    pub(crate) tick_decimals: u32,
    pub(crate) fee_context: Option<MarketBuyFeeContext>,
}

#[derive(Debug, Clone)]
pub(crate) struct MarketOrderSubmitResult {
    pub response: OrderResponse,
    pub expected_base_qty: Decimal,
    pub expected_venue_order_id: VenueOrderId,
}

#[derive(Debug, Clone)]
pub(crate) struct UnknownSubmitError {
    pub reason: String,
    pub expected_venue_order_id: VenueOrderId,
    pub expected_base_qty: Option<Decimal>,
}

impl Display for UnknownSubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "submit outcome unknown for {}: {}",
            self.expected_venue_order_id, self.reason
        )
    }
}

impl StdError for UnknownSubmitError {}

/// HTTP order submission and cancellation facade.
///
/// Provides a clean API accepting Nautilus-native types, internally handling:
/// - Side/TIF conversion to Polymarket types
/// - Order building and EIP-712 signing (via [`PolymarketOrderBuilder`])
/// - HTTP posting to the CLOB API with automatic retry on transient failures
///
/// Fees are set by the protocol at match time in CLOB V2 (no longer embedded
/// in the signed order), so the submitter does not pre-fetch fee rates.
#[derive(Debug, Clone)]
pub(crate) struct OrderSubmitter {
    http_client: PolymarketClobHttpClient,
    order_builder: Arc<PolymarketOrderBuilder>,
    retry_manager: Arc<RetryManager<Error>>,
    durable_retry_manager: Arc<RetryManager<Error>>,
    cancelled_submissions: Arc<Mutex<HashSet<String>>>,
}

impl OrderSubmitter {
    pub(crate) fn new(
        http_client: PolymarketClobHttpClient,
        order_builder: Arc<PolymarketOrderBuilder>,
        retry_config: RetryConfig,
    ) -> Self {
        let mut durable_retry_config = retry_config.clone();
        // Prepared strategy envelopes are durable and have an exact venue hash.
        // Keep reconciling/reposting that one authorization until acceptance or
        // operator cancellation; never fall back to signing a replacement.
        durable_retry_config.max_retries = u32::MAX;
        durable_retry_config.max_elapsed_ms = None;
        durable_retry_config.jitter_ms = 0;
        Self {
            http_client,
            order_builder,
            retry_manager: Arc::new(RetryManager::new(retry_config)),
            durable_retry_manager: Arc::new(RetryManager::new(durable_retry_config)),
            cancelled_submissions: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Fetches order book, calculates crossing price, builds and posts a market order.
    ///
    /// Converts Nautilus side to Polymarket side, walks the appropriate book side
    /// to find the crossing price, then builds and submits a FAK or FOK order.
    /// The book fetch is not retried (stale on retry); only the final POST is retried.
    ///
    /// The second return value is the order's signed base quantity (shares for
    /// BUY, the original `amount` for SELL). For BUY this is derived from the
    /// signed `taker_amount` so quote-to-base conversion matches what the venue
    /// can fill (single crossing price), not the multi-level book walk total.
    ///
    /// `request.fee_context`, when supplied with `OrderSide::Buy`, is used to shrink
    /// `amount` for taker fees before signing so balance-sized BUYs are not
    /// rejected by the venue. SELL ignores the context.
    pub(crate) async fn submit_market_order(
        &self,
        request: MarketOrderSubmitRequest,
    ) -> anyhow::Result<MarketOrderSubmitResult> {
        let MarketOrderSubmitRequest {
            client_order_id,
            account_id,
            profile_id,
            token_id,
            side,
            amount,
            time_in_force,
            neg_risk,
            tick_decimals,
            fee_context,
        } = request;
        let poly_side = PolymarketOrderSide::try_from(side)
            .map_err(|e| anyhow::anyhow!("Invalid order side: {e}"))?;
        let order_type = PolymarketOrderType::from_market_time_in_force(time_in_force)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let amount_dec = amount.as_decimal();
        let prepared_request = PreparedOrderRequest {
            client_order_id,
            account_id,
            profile_id,
            kind: PreparedOrderKind::Market,
            token_id: token_id.clone(),
            side,
            price: Decimal::ZERO,
            amount: amount_dec,
            quote_quantity: side == OrderSide::Buy,
            time_in_force,
            post_only: false,
            neg_risk,
            tick_decimals,
            expiration: "0".to_string(),
        };
        if let Some(prepared) = take_prepared_market_order(&prepared_request)? {
            return self.post_prepared_market_order(prepared).await;
        }
        if requires_prepared_order(&prepared_request.client_order_id) {
            anyhow::bail!("required prepared market order is unavailable");
        }

        let book = self
            .http_client
            .get_book(&token_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch order book: {e}"))?;

        let levels = match poly_side {
            PolymarketOrderSide::Buy => &book.asks,
            PolymarketOrderSide::Sell => &book.bids,
        };

        let result = calculate_market_price(levels, amount_dec, poly_side)
            .map_err(|e| anyhow::anyhow!("Market price calculation failed: {e}"))?;

        // Fee-aware sizing applies to BUY only and only when a context is
        // provided. Run before signing so the on-chain `taker_amount` and
        // the emitted base quantity both reflect the venue-fillable amount.
        let signed_amount = match (poly_side, fee_context) {
            (PolymarketOrderSide::Buy, Some(ctx)) => adjust_market_buy_amount(
                amount_dec,
                ctx.user_pusd_balance,
                result.crossing_price,
                ctx.fee_rate,
                ctx.fee_exponent,
                ctx.builder_taker_fee_rate,
            )?,
            _ => amount_dec,
        };

        let poly_order = self
            .order_builder
            .build_market_order(
                &token_id,
                poly_side,
                result.crossing_price,
                signed_amount,
                neg_risk,
                tick_decimals,
            )
            .map_err(|e| anyhow::anyhow!("Failed to build market order: {e}"))?;

        // Wire amounts are mantissas at USDC_DECIMALS (10^6) scale. For BUY,
        // the signed taker_amount is the exact share quantity the venue will
        // fill against; for SELL, the original `amount` is already in base
        // shares (book walk total is irrelevant since SELL is never quote-qty).
        let usdc_scale = Decimal::from(1_000_000u32);
        let signed_base_qty = match poly_side {
            PolymarketOrderSide::Buy => poly_order.taker_amount / usdc_scale,
            PolymarketOrderSide::Sell => amount_dec,
        };
        let expected_venue_order_id = self
            .order_builder
            .expected_order_id(&poly_order, neg_risk)?;

        let response = match self
            .post_order_with_retry(
                "submit_market_order",
                poly_order,
                order_type,
                false,
                expected_venue_order_id,
                false,
                false,
            )
            .await
        {
            Ok(response) => response,
            Err(e) if e.is_submit_outcome_unknown() => {
                return Err(UnknownSubmitError {
                    reason: e.to_string(),
                    expected_venue_order_id,
                    expected_base_qty: Some(signed_base_qty),
                }
                .into());
            }
            Err(e) => anyhow::bail!("{e}"),
        };

        Ok(MarketOrderSubmitResult {
            response,
            expected_base_qty: signed_base_qty,
            expected_venue_order_id,
        })
    }

    async fn post_prepared_market_order(
        &self,
        prepared: PreparedPolymarketOrder,
    ) -> anyhow::Result<MarketOrderSubmitResult> {
        let expected_venue_order_id = VenueOrderId::from(prepared.expected_venue_order_id.as_str());
        let expected_base_qty = prepared.expected_base_quantity;
        let order_type = prepared.order_type;
        let reconcile_before_post = prepared.reconcile_before_post;
        let poly_order = prepared.order;
        let response = match self
            .post_order_with_retry(
                "submit_prepared_market_order",
                poly_order,
                order_type,
                false,
                expected_venue_order_id,
                true,
                reconcile_before_post,
            )
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_submit_outcome_unknown() => {
                return Err(UnknownSubmitError {
                    reason: error.to_string(),
                    expected_venue_order_id,
                    expected_base_qty: Some(expected_base_qty),
                }
                .into());
            }
            Err(error) => anyhow::bail!("{error}"),
        };
        Ok(MarketOrderSubmitResult {
            response,
            expected_base_qty,
            expected_venue_order_id,
        })
    }

    /// Cancels a single order with retry on transient failures.
    pub(crate) async fn cancel_order(&self, venue_order_id: &str) -> HttpResult<CancelResponse> {
        if let Ok(mut cancelled) = self.cancelled_submissions.lock() {
            cancelled.insert(venue_order_id.to_string());
        }
        let http_client = self.http_client.clone();
        let order_id = venue_order_id.to_string();
        self.retry_manager
            .execute_with_retry(
                "cancel_order",
                || {
                    let http_client = http_client.clone();
                    let order_id = order_id.clone();
                    async move { http_client.cancel_order(&order_id).await }
                },
                |e| e.is_retryable(),
                Error::transport,
            )
            .await
    }

    /// Cancels multiple orders with retry on transient failures.
    pub(crate) async fn cancel_orders(
        &self,
        venue_order_ids: &[&str],
    ) -> HttpResult<CancelResponse> {
        if let Ok(mut cancelled) = self.cancelled_submissions.lock() {
            cancelled.extend(venue_order_ids.iter().map(|value| (*value).to_string()));
        }
        let http_client = self.http_client.clone();
        let order_ids: Vec<String> = venue_order_ids.iter().map(|s| s.to_string()).collect();

        self.retry_manager
            .execute_with_retry(
                "cancel_orders",
                || {
                    let http_client = http_client.clone();
                    let order_ids = order_ids.clone();
                    async move {
                        let refs: Vec<&str> = order_ids.iter().map(String::as_str).collect();
                        http_client.cancel_orders(&refs).await
                    }
                },
                |e| e.is_retryable(),
                Error::transport,
            )
            .await
    }

    /// Fetches a single order by its venue order ID from the CLOB REST API.
    ///
    /// Returns `Ok(None)` if the API returns an empty or `null` body (order not found / settled).
    pub(crate) async fn get_order(
        &self,
        order_id: &str,
    ) -> anyhow::Result<Option<PolymarketOpenOrder>> {
        let http_client = self.http_client.clone();
        let oid = order_id.to_string();

        self.retry_manager
            .execute_with_retry(
                "get_order",
                || {
                    let http_client = http_client.clone();
                    let oid = oid.clone();
                    async move { http_client.get_order_optional(&oid).await }
                },
                |e| e.is_retryable(),
                Error::transport,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch order status: {e}"))
    }

    /// Prepares multiple limit order submissions in parallel.
    pub(crate) async fn prepare_limit_order_submissions(
        &self,
        requests: &[LimitOrderSubmitRequest],
    ) -> Vec<anyhow::Result<SignedLimitOrderSubmission>> {
        let futures = requests
            .iter()
            .map(|request| self.prepare_limit_order_submission(request));
        futures_util::future::join_all(futures).await
    }

    pub(crate) async fn prepare_limit_order_submission(
        &self,
        request: &LimitOrderSubmitRequest,
    ) -> anyhow::Result<SignedLimitOrderSubmission> {
        let expiration = limit_order_expiration(request.expire_time);
        let prepared_request = PreparedOrderRequest {
            client_order_id: request.client_order_id.clone(),
            account_id: request.account_id.clone(),
            profile_id: request.profile_id.clone(),
            kind: PreparedOrderKind::Limit,
            token_id: request.token_id.clone(),
            side: request.side,
            price: request.price.as_decimal(),
            amount: request.quantity.as_decimal(),
            quote_quantity: false,
            time_in_force: request.time_in_force,
            post_only: request.post_only,
            neg_risk: request.neg_risk,
            tick_decimals: request.tick_decimals,
            expiration: expiration.clone(),
        };
        if let Some(prepared) = take_prepared_order(&prepared_request)? {
            return Ok(SignedLimitOrderSubmission {
                order: prepared.order,
                order_type: prepared.order_type,
                post_only: prepared.request.post_only,
                expected_venue_order_id: VenueOrderId::from(
                    prepared.expected_venue_order_id.as_str(),
                ),
                prepared: true,
                reconcile_before_post: prepared.reconcile_before_post,
            });
        }
        if requires_prepared_order(&prepared_request.client_order_id) {
            anyhow::bail!("required prepared limit order is unavailable");
        }
        let order_type = PolymarketOrderType::try_from(request.time_in_force)
            .map_err(|e| anyhow::anyhow!("Unsupported time in force: {e}"))?;
        let side = PolymarketOrderSide::try_from(request.side)
            .map_err(|e| anyhow::anyhow!("Invalid order side: {e}"))?;

        let order = self
            .order_builder
            .build_limit_order(
                &request.token_id,
                side,
                request.price.as_decimal(),
                request.quantity.as_decimal(),
                &expiration,
                request.neg_risk,
                request.tick_decimals,
            )
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let expected_venue_order_id = self
            .order_builder
            .expected_order_id(&order, request.neg_risk)?;

        Ok(SignedLimitOrderSubmission {
            order,
            order_type,
            post_only: request.post_only,
            expected_venue_order_id,
            prepared: false,
            reconcile_before_post: false,
        })
    }

    pub(crate) async fn post_limit_order_submission(
        &self,
        submission: SignedLimitOrderSubmission,
    ) -> crate::http::error::Result<OrderResponse> {
        self.post_order_with_retry(
            "submit_limit_order",
            submission.order,
            submission.order_type,
            submission.post_only,
            submission.expected_venue_order_id,
            submission.prepared,
            submission.reconcile_before_post,
        )
        .await
    }

    /// Retries one already-signed payload while preserving ambiguity across the
    /// entire operation. A later definitive refusal cannot prove an earlier
    /// transport/generic-5xx/parse attempt was not accepted by the venue.
    async fn post_order_with_retry(
        &self,
        operation: &'static str,
        order: PolymarketOrder,
        order_type: PolymarketOrderType,
        post_only: bool,
        expected_venue_order_id: VenueOrderId,
        durable: bool,
        recovered: bool,
    ) -> crate::http::error::Result<OrderResponse> {
        let http_client = self.http_client.clone();
        let ambiguous_reason = Arc::new(Mutex::new(None::<String>));
        let reconcile_before_post = Arc::new(AtomicBool::new(durable && recovered));
        let taint = Arc::clone(&ambiguous_reason);
        let reconcile = Arc::clone(&reconcile_before_post);
        let cancelled_submissions = Arc::clone(&self.cancelled_submissions);
        let retry_manager = if durable {
            &self.durable_retry_manager
        } else {
            &self.retry_manager
        };
        let result = retry_manager
            .execute_with_retry(
                operation,
                || {
                    let http_client = http_client.clone();
                    let order = order.clone();
                    let taint = Arc::clone(&taint);
                    let reconcile = Arc::clone(&reconcile);
                    let cancelled_submissions = Arc::clone(&cancelled_submissions);
                    async move {
                        if cancelled_submissions.lock().is_ok_and(|cancelled| {
                            cancelled.contains(expected_venue_order_id.as_str())
                        }) {
                            return Err(Error::exchange("durable submit cancelled"));
                        }
                        // After any ambiguous POST (including malformed success
                        // bodies), query the exact signed hash before reposting.
                        if reconcile.load(Ordering::Acquire) {
                            match http_client
                                .get_order_optional(expected_venue_order_id.as_str())
                                .await
                            {
                                Ok(Some(found)) if found.id == expected_venue_order_id.as_str() => {
                                    return Ok(OrderResponse {
                                        success: true,
                                        order_id: Some(expected_venue_order_id.to_string()),
                                        error_msg: None,
                                        reconciled_order: Some(found),
                                    });
                                }
                                Ok(Some(found)) => {
                                    return Err(Error::decode(format!(
                                        "exact-order query returned mismatched id {}",
                                        found.id
                                    )));
                                }
                                Ok(None) => {}
                                Err(error) => return Err(error),
                            }
                        }

                        let mut result =
                            http_client.post_order(&order, order_type, post_only).await;
                        if let Ok(response) = &result {
                            let missing_id = response.success
                                && response.order_id.as_deref().is_none_or(str::is_empty);
                            let duplicate = response.error_msg.as_deref().is_some_and(|reason| {
                                let reason = reason.to_ascii_lowercase();
                                reason.contains("already exists") || reason.contains("duplicate")
                            });
                            if missing_id || duplicate {
                                result = Err(Error::decode(if missing_id {
                                    "successful submit response omitted orderID"
                                } else {
                                    "venue reported an already-existing order"
                                }));
                            }
                        }
                        if let Err(error) = &result
                            && error.is_submit_outcome_unknown()
                        {
                            reconcile.store(true, Ordering::Release);
                            if let Ok(mut reason) = taint.lock()
                                && reason.is_none()
                            {
                                *reason = Some(error.to_string());
                            }
                        }
                        result
                    }
                },
                |error| {
                    error.is_retryable()
                        && (!durable
                            || error.is_submit_outcome_unknown()
                            || ambiguous_reason.lock().is_ok_and(|reason| reason.is_some()))
                },
                Error::transport,
            )
            .await;

        match result {
            Ok(response) => Ok(response),
            Err(error) => {
                let earlier_ambiguity = ambiguous_reason
                    .lock()
                    .ok()
                    .and_then(|reason| reason.clone());
                if let Some(reason) = earlier_ambiguity {
                    Err(Error::transport(format!(
                        "submit outcome unknown after an earlier ambiguous attempt ({reason}); final attempt: {error}"
                    )))
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(crate) async fn post_limit_order_submissions(
        &self,
        submissions: Vec<SignedLimitOrderSubmission>,
    ) -> crate::http::error::Result<Vec<OrderResponse>> {
        let order_refs: Vec<(&PolymarketOrder, PolymarketOrderType, bool)> = submissions
            .iter()
            .map(|submission| {
                (
                    &submission.order,
                    submission.order_type,
                    submission.post_only,
                )
            })
            .collect();

        // Do not retry batch submits automatically.
        // A transport timeout can race with server-side acceptance and resubmit
        // the whole batch without an idempotency key we can verify here.
        self.http_client.post_orders(&order_refs).await
    }
}

// Converts a nanos expire time to the unix-seconds string expected by the
// Polymarket API. Returns `"0"` when there is no expiration.
fn limit_order_expiration(expire_time: Option<UnixNanos>) -> String {
    match expire_time {
        Some(ns) if ns.as_u64() > 0 => (ns.as_u64() / 1_000_000_000).to_string(),
        _ => "0".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::none(None, "0")]
    #[case::zero(Some(UnixNanos::from(0u64)), "0")]
    #[case::one_second(Some(UnixNanos::from(1_000_000_000u64)), "1")]
    #[case::sub_second_truncates(Some(UnixNanos::from(1_500_000_000u64)), "1")]
    #[case::typical(Some(UnixNanos::from(1_735_689_600_000_000_000u64)), "1735689600")]
    fn test_limit_order_expiration(#[case] expire_time: Option<UnixNanos>, #[case] expected: &str) {
        assert_eq!(limit_order_expiration(expire_time), expected);
    }
}
