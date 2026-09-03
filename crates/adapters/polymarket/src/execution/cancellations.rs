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

use anyhow::Context;
use nautilus_common::{
    messages::execution::{BatchCancelOrders, CancelAllOrders, CancelOrder},
    msgbus,
    msgbus::switchboard,
};
use nautilus_core::{UUID4, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{OrderCanceled, OrderEventAny},
    identifiers::{AccountId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::OrderStatusReport,
    types::{Price, Quantity},
};

use super::{
    PolymarketExecutionClient, pending::PendingCancelTracker,
    responses::send_terminal_confirmation_report,
};
use crate::{execution::types::CancelOutcome, http::query::CancelResponse};

impl PolymarketExecutionClient {
    pub(super) fn cancel_order_command(&self, cmd: &CancelOrder) {
        let order = self
            .core
            .cache()
            .order(&cmd.client_order_id)
            .map(|o| o.clone());
        let order_ref = match &order {
            Some(o) => o,
            None => {
                let Some(venue_order_id) = cmd.venue_order_id else {
                    log::warn!(
                        "order.cancel_failed_closed client_order_id={} reason=cache_missing_and_no_exact_venue_id",
                        cmd.client_order_id
                    );
                    return;
                };
                self.cancel_cache_free(cmd.clone(), venue_order_id);
                return;
            }
        };

        if !order_ref.is_open() {
            log::warn!(
                "Cannot cancel order that is not open: {}",
                cmd.client_order_id
            );
            return;
        }

        let venue_order_id = match order_ref.venue_order_id() {
            Some(id) => id,
            None => match self
                .core
                .cache()
                .venue_order_id(&cmd.client_order_id)
                .copied()
                .or_else(|| self.pending_submits.venue_order_id(&cmd.client_order_id))
            {
                Some(id) => id,
                None => {
                    log::debug!(
                        "Cancel for {} deferred, expected venue_order_id not yet available",
                        cmd.client_order_id
                    );
                    self.pending_cancels.insert(cmd.client_order_id);
                    return;
                }
            },
        };

        let order_id_str = venue_order_id.to_string();
        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let order_clone = order.unwrap();

        self.spawn_task("cancel_order", async move {
            let started = std::time::Instant::now();
            log::info!(
                "order.cancel_http_dispatch client_order_id={} venue_order_id={} cache_present=true",
                order_clone.client_order_id(),
                venue_order_id,
            );
            match submitter.cancel_order(&order_id_str).await {
                Ok(response) => {
                    let status = process_cancel_result(
                        &response,
                        &order_id_str,
                        &order_clone,
                        venue_order_id,
                        &emitter,
                        clock,
                    );
                    log::info!(
                        "order.cancel_http_response client_order_id={} venue_order_id={} outcome={:?} elapsed_ms={}",
                        order_clone.client_order_id(),
                        venue_order_id,
                        status,
                        started.elapsed().as_millis(),
                    );
                    if status != CancelResponseStatus::ConfirmedCanceled {
                        confirm_cached_terminal(
                            &submitter,
                            &order_clone,
                            venue_order_id,
                            &emitter,
                            clock,
                            started,
                        )
                        .await;
                    }
                }
                Err(e) => {
                    log::warn!(
                        "Cancel outcome unknown for {} ({}), awaiting reconciliation: {e}",
                        order_clone.client_order_id(),
                        venue_order_id,
                    );
                    log::warn!(
                        "order.cancel_http_response client_order_id={} venue_order_id={} outcome=unknown elapsed_ms={}",
                        order_clone.client_order_id(),
                        venue_order_id,
                        started.elapsed().as_millis(),
                    );
                    confirm_cached_terminal(
                        &submitter,
                        &order_clone,
                        venue_order_id,
                        &emitter,
                        clock,
                        started,
                    )
                    .await;
                    return Err(anyhow::Error::new(e).context("cancel order failed"));
                }
            }
            Ok(())
        });
    }

    fn cancel_cache_free(&self, cmd: CancelOrder, venue_order_id: VenueOrderId) {
        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let account_id = self.core.account_id;
        self.spawn_task("cancel_order_exact_identity", async move {
            let started = std::time::Instant::now();
            log::info!(
                "order.cancel_http_dispatch client_order_id={} venue_order_id={} cache_present=false",
                cmd.client_order_id,
                venue_order_id,
            );
            match submitter.cancel_order(venue_order_id.as_str()).await {
                Ok(response)
                    if response
                        .canceled
                        .iter()
                        .any(|order_id| order_id == venue_order_id.as_str()) =>
                {
                    emit_cache_free_canceled(&cmd, venue_order_id, account_id, clock);
                    log::info!(
                        "order.cancel_http_response client_order_id={} venue_order_id={} outcome=cancelled elapsed_ms={}",
                        cmd.client_order_id,
                        venue_order_id,
                        started.elapsed().as_millis(),
                    );
                }
                Ok(response) => {
                    let reason = response
                        .not_canceled
                        .get(venue_order_id.as_str())
                        .and_then(|reason| reason.as_deref())
                        .unwrap_or("missing_per_order_result");
                    log::warn!(
                        "order.cancel_http_response client_order_id={} venue_order_id={} outcome=needs_reconciliation reason={} elapsed_ms={}",
                        cmd.client_order_id,
                        venue_order_id,
                        reason,
                        started.elapsed().as_millis(),
                    );
                    confirm_cache_free_terminal(
                        &submitter,
                        &cmd,
                        venue_order_id,
                        account_id,
                        &emitter,
                        clock,
                        started,
                    )
                    .await;
                }
                Err(error) => {
                    log::warn!(
                        "order.cancel_http_response client_order_id={} venue_order_id={} outcome=unknown elapsed_ms={} error={}",
                        cmd.client_order_id,
                        venue_order_id,
                        started.elapsed().as_millis(),
                        error,
                    );
                    confirm_cache_free_terminal(
                        &submitter,
                        &cmd,
                        venue_order_id,
                        account_id,
                        &emitter,
                        clock,
                        started,
                    )
                    .await;
                }
            }
            Ok(())
        });
    }

    pub(super) fn cancel_all_orders_command(&self, cmd: &CancelAllOrders) {
        let cache = self.core.cache();
        let open_orders = cache.orders_open(
            Some(&self.core.venue),
            Some(&cmd.instrument_id),
            Some(&cmd.strategy_id),
            None,
            Some(cmd.order_side),
        );

        if open_orders.is_empty() {
            log::debug!("No open orders to cancel for {}", cmd.instrument_id);
            return;
        }

        let venue_order_ids: Vec<String> = open_orders
            .iter()
            .filter_map(|o| o.venue_order_id().map(|v| v.to_string()))
            .collect();

        if venue_order_ids.is_empty() {
            log::warn!("No venue order IDs found for cancel all");
            return;
        }

        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let orders: Vec<OrderAny> = open_orders.into_iter().map(|o| o.clone()).collect();

        self.spawn_task("cancel_all_orders", async move {
            let order_id_refs: Vec<&str> = venue_order_ids.iter().map(String::as_str).collect();
            let response = submitter
                .cancel_orders(&order_id_refs)
                .await
                .context("failed to cancel all orders")?;

            for order in &orders {
                if let Some(vid) = order.venue_order_id() {
                    let vid_str = vid.to_string();
                    process_cancel_result(&response, &vid_str, order, vid, &emitter, clock);
                }
            }

            log::debug!("Canceled {} orders", response.canceled.len());
            Ok(())
        });
    }

    pub(super) fn batch_cancel_orders_command(&self, cmd: &BatchCancelOrders) {
        if cmd.cancels.is_empty() {
            return;
        }

        let mut venue_to_order: Vec<(String, OrderAny)> = Vec::new();

        for c in &cmd.cancels {
            if let Some(order) = self.core.cache().order(&c.client_order_id)
                && let Some(vid) = order.venue_order_id()
            {
                venue_to_order.push((vid.to_string(), order.clone()));
            }
        }

        if venue_to_order.is_empty() {
            log::warn!("No venue order IDs found for batch cancel");
            return;
        }

        let order_ids: Vec<String> = venue_to_order.iter().map(|(id, _)| id.clone()).collect();
        let submitter = self.submitter.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;

        self.spawn_task("batch_cancel_orders", async move {
            let order_id_refs: Vec<&str> = order_ids.iter().map(String::as_str).collect();
            let response = submitter
                .cancel_orders(&order_id_refs)
                .await
                .context("failed to batch cancel orders")?;

            for (venue_id_str, order) in &venue_to_order {
                let vid = VenueOrderId::from(venue_id_str.as_str());
                process_cancel_result(&response, venue_id_str, order, vid, &emitter, clock);
            }

            log::debug!("Batch canceled {} orders", response.canceled.len());
            Ok(())
        });
    }
}

async fn confirm_cached_terminal(
    submitter: &super::submitter::OrderSubmitter,
    order: &OrderAny,
    venue_order_id: VenueOrderId,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    started: std::time::Instant,
) {
    const DELAYS: [std::time::Duration; 5] = [
        std::time::Duration::from_millis(100),
        std::time::Duration::from_millis(200),
        std::time::Duration::from_millis(400),
        std::time::Duration::from_millis(800),
        std::time::Duration::from_millis(1_600),
    ];
    for (attempt, delay) in DELAYS.iter().enumerate() {
        tokio::time::sleep(*delay).await;
        let venue_order = match submitter.get_order(venue_order_id.as_str()).await {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(_) => continue,
        };
        let status = OrderStatus::from(venue_order.status);
        if !matches!(
            status,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Rejected
                | OrderStatus::Expired
        ) {
            continue;
        }
        let ts_now = clock.get_time_ns();
        let size_precision = order.quantity().precision;
        let mut report = OrderStatusReport::new(
            order
                .account_id()
                .unwrap_or(AccountId::from("POLYMARKET-UNKNOWN")),
            order.instrument_id(),
            Some(order.client_order_id()),
            venue_order_id,
            order.order_side(),
            order.order_type(),
            order.time_in_force(),
            status,
            Quantity::new(
                venue_order.original_size.to_string().parse().unwrap_or(0.0),
                size_precision,
            ),
            Quantity::new(
                venue_order.size_matched.to_string().parse().unwrap_or(0.0),
                size_precision,
            ),
            ts_now,
            ts_now,
            ts_now,
            None,
        );
        report.price = order.price();
        send_terminal_confirmation_report(emitter, report);
        log::info!(
            "order.cancel_confirmation client_order_id={} venue_order_id={} source=rest_fallback status={:?} attempt={} total_ms={} size_matched={}",
            order.client_order_id(),
            venue_order_id,
            status,
            attempt + 1,
            started.elapsed().as_millis(),
            venue_order.size_matched,
        );
        return;
    }
    log::warn!(
        "order.cancel_confirmation_timeout client_order_id={} venue_order_id={} attempts={} total_ms={}",
        order.client_order_id(),
        venue_order_id,
        DELAYS.len(),
        started.elapsed().as_millis(),
    );
}

fn emit_cache_free_canceled(
    cmd: &CancelOrder,
    venue_order_id: VenueOrderId,
    account_id: AccountId,
    clock: &'static AtomicTime,
) {
    let event = cache_free_canceled_event(cmd, venue_order_id, account_id, clock);
    let topic = switchboard::get_event_order_topic(event.strategy_id());
    msgbus::publish_order_event(topic, &event);
}

fn cache_free_canceled_event(
    cmd: &CancelOrder,
    venue_order_id: VenueOrderId,
    account_id: AccountId,
    clock: &'static AtomicTime,
) -> OrderEventAny {
    let ts_now = clock.get_time_ns();
    OrderEventAny::Canceled(OrderCanceled::new(
        cmd.trader_id,
        cmd.strategy_id,
        cmd.instrument_id,
        cmd.client_order_id,
        UUID4::new(),
        ts_now,
        ts_now,
        false,
        Some(venue_order_id),
        Some(account_id),
    ))
}

async fn confirm_cache_free_terminal(
    submitter: &super::submitter::OrderSubmitter,
    cmd: &CancelOrder,
    venue_order_id: VenueOrderId,
    account_id: AccountId,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    started: std::time::Instant,
) {
    const DELAYS: [std::time::Duration; 5] = [
        std::time::Duration::from_millis(100),
        std::time::Duration::from_millis(200),
        std::time::Duration::from_millis(400),
        std::time::Duration::from_millis(800),
        std::time::Duration::from_millis(1_600),
    ];
    for (attempt, delay) in DELAYS.iter().enumerate() {
        tokio::time::sleep(*delay).await;
        let venue_order = match submitter.get_order(venue_order_id.as_str()).await {
            Ok(Some(order)) => order,
            Ok(None) => continue,
            Err(error) => {
                log::warn!(
                    "order.cancel_confirmation_check_failed client_order_id={} venue_order_id={} attempt={} error={}",
                    cmd.client_order_id,
                    venue_order_id,
                    attempt + 1,
                    error,
                );
                continue;
            }
        };
        let status = OrderStatus::from(venue_order.status);
        match status {
            OrderStatus::Filled => {
                send_terminal_confirmation_report(
                    emitter,
                    cache_free_status_report(cmd, &venue_order, account_id, status, clock),
                );
            }
            OrderStatus::Canceled if venue_order.size_matched == rust_decimal::Decimal::ZERO => {
                emit_cache_free_canceled(cmd, venue_order_id, account_id, clock);
            }
            OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Expired => {
                // Reconcile matched quantity before the terminal event so fill evidence wins.
                send_terminal_confirmation_report(
                    emitter,
                    cache_free_status_report(cmd, &venue_order, account_id, status, clock),
                );
            }
            _ => continue,
        }
        log::info!(
            "order.cancel_confirmation client_order_id={} venue_order_id={} source=rest_fallback status={:?} attempt={} total_ms={} size_matched={}",
            cmd.client_order_id,
            venue_order_id,
            status,
            attempt + 1,
            started.elapsed().as_millis(),
            venue_order.size_matched,
        );
        return;
    }
    log::warn!(
        "order.cancel_confirmation_timeout client_order_id={} venue_order_id={} attempts={} total_ms={}",
        cmd.client_order_id,
        venue_order_id,
        DELAYS.len(),
        started.elapsed().as_millis(),
    );
}

fn cache_free_status_report(
    cmd: &CancelOrder,
    venue_order: &crate::http::models::PolymarketOpenOrder,
    account_id: AccountId,
    status: OrderStatus,
    clock: &'static AtomicTime,
) -> OrderStatusReport {
    let ts_now = clock.get_time_ns();
    let size_precision = u8::try_from(venue_order.original_size.scale()).unwrap_or(6);
    let price_precision = u8::try_from(venue_order.price.scale()).unwrap_or(6);
    let mut report = OrderStatusReport::new(
        account_id,
        cmd.instrument_id,
        Some(cmd.client_order_id),
        VenueOrderId::from(venue_order.id.as_str()),
        OrderSide::from(venue_order.side),
        OrderType::Limit,
        TimeInForce::from(venue_order.order_type),
        status,
        Quantity::new(
            venue_order.original_size.to_string().parse().unwrap_or(0.0),
            size_precision,
        ),
        Quantity::new(
            venue_order.size_matched.to_string().parse().unwrap_or(0.0),
            size_precision,
        ),
        ts_now,
        ts_now,
        ts_now,
        None,
    );
    report.price = Some(Price::new(
        venue_order.price.to_string().parse().unwrap_or(0.0),
        price_precision,
    ));
    report
}

pub(super) fn process_cancel_result(
    response: &CancelResponse,
    venue_order_id_str: &str,
    order: &OrderAny,
    venue_order_id: VenueOrderId,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
) -> CancelResponseStatus {
    if let Some(reason_opt) = response.not_canceled.get(venue_order_id_str) {
        let reason = reason_opt.as_deref().unwrap_or("unknown reason");
        match CancelOutcome::classify(reason) {
            CancelOutcome::AlreadyDone => {
                log::debug!(
                    "Cancel rejected for {}: {reason} - awaiting WS for terminal state",
                    order.client_order_id()
                );
            }
            CancelOutcome::Rejected(msg) => {
                let ts_now = clock.get_time_ns();
                emitter.emit_order_cancel_rejected(order, Some(venue_order_id), &msg, ts_now);
            }
        }
        return CancelResponseStatus::NeedsReconciliation;
    }

    if response
        .canceled
        .iter()
        .any(|order_id| order_id == venue_order_id_str)
    {
        let ts_now = clock.get_time_ns();
        emitter.emit_order_canceled(order, Some(venue_order_id), ts_now);
        return CancelResponseStatus::ConfirmedCanceled;
    }

    log::warn!(
        "Cancel response for {} did not include per-order result for {}",
        order.client_order_id(),
        venue_order_id
    );
    CancelResponseStatus::NeedsReconciliation
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CancelResponseStatus {
    ConfirmedCanceled,
    NeedsReconciliation,
}

pub(super) async fn execute_deferred_cancel(
    submitter: &super::submitter::OrderSubmitter,
    order: &OrderAny,
    order_id_str: &str,
    venue_order_id: VenueOrderId,
    emitter: &ExecutionEventEmitter,
    pending_cancels: &PendingCancelTracker,
    clock: &'static AtomicTime,
) {
    match submitter.cancel_order(order_id_str).await {
        Ok(response) => {
            let status = process_cancel_result(
                &response,
                order_id_str,
                order,
                venue_order_id,
                emitter,
                clock,
            );

            if status == CancelResponseStatus::ConfirmedCanceled {
                pending_cancels.remove(&order.client_order_id());
            }
        }
        Err(e) => {
            log::warn!(
                "Deferred cancel outcome unknown for {} ({}), awaiting reconciliation: {e}",
                order.client_order_id(),
                venue_order_id,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId};

    use super::*;

    fn command() -> CancelOrder {
        CancelOrder::new(
            TraderId::from("TESTER-001"),
            None,
            StrategyId::from("SIDECAR-ORDER-GATEWAY"),
            InstrumentId::from("TOKEN.POLYMARKET"),
            ClientOrderId::from("strategy:stop-1:stop:entry"),
            Some(VenueOrderId::from("venue-1")),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    #[test]
    fn cache_free_cancel_emits_original_exact_identity() {
        let cmd = command();
        let venue_order_id = VenueOrderId::from("venue-1");
        let event = cache_free_canceled_event(
            &cmd,
            venue_order_id,
            AccountId::from("POLYMARKET-12"),
            nautilus_core::time::get_atomic_clock_realtime(),
        );
        let OrderEventAny::Canceled(canceled) = event else {
            panic!("expected canceled event");
        };
        assert_eq!(canceled.client_order_id, cmd.client_order_id);
        assert_eq!(canceled.venue_order_id, Some(venue_order_id));
        assert_eq!(canceled.instrument_id, cmd.instrument_id);
    }

    #[test]
    fn canceled_status_report_preserves_partial_fill_quantity() {
        let cmd = command();
        let mut venue_order: crate::http::models::PolymarketOpenOrder = {
            let content = std::fs::read_to_string("test_data/http_open_order_sell_fok.json")
                .expect("fixture");
            serde_json::from_str(&content).expect("open order")
        };
        venue_order.status = crate::common::enums::PolymarketOrderStatus::Canceled;
        venue_order.size_matched = rust_decimal::Decimal::new(125, 2);
        let report = cache_free_status_report(
            &cmd,
            &venue_order,
            AccountId::from("POLYMARKET-12"),
            OrderStatus::Canceled,
            nautilus_core::time::get_atomic_clock_realtime(),
        );

        assert_eq!(report.client_order_id, Some(cmd.client_order_id));
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(report.filled_qty.as_f64(), 1.25);
    }
}
