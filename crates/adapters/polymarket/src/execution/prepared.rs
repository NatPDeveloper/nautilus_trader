// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
// -------------------------------------------------------------------------------------------------

//! Exact pre-signed Polymarket orders consumed by the normal execution client.
//!
//! Preparation is intentionally separate from submission. The eventual submit still travels
//! through [`PolymarketExecutionClient`](super::PolymarketExecutionClient), so its authenticated
//! user WebSocket, pending-submit tracker, cache, and lifecycle emitter remain authoritative.

use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};

use alloy_primitives::keccak256;
use nautilus_model::enums::{OrderSide, TimeInForce};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::order_builder::PolymarketOrderBuilder;
use crate::{
    common::enums::{PolymarketOrderSide, PolymarketOrderType},
    http::models::PolymarketOrder,
    signing::eip712::order_hash,
};

const PREPARED_SCHEMA_VERSION: u32 = 1;
static PREPARED: LazyLock<Mutex<HashMap<String, PreparedPolymarketOrder>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PreparedOrderKind {
    Limit,
    Market,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedOrderRequest {
    pub client_order_id: String,
    pub kind: PreparedOrderKind,
    pub token_id: String,
    pub side: OrderSide,
    pub price: Decimal,
    pub amount: Decimal,
    pub quote_quantity: bool,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub neg_risk: bool,
    pub tick_decimals: u32,
    pub expiration: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedPolymarketOrder {
    pub schema_version: u32,
    pub request: PreparedOrderRequest,
    pub order: PolymarketOrder,
    pub order_type: PolymarketOrderType,
    pub expected_venue_order_id: String,
    pub expected_base_quantity: Decimal,
    pub fingerprint: String,
}

impl PreparedPolymarketOrder {
    pub fn prepare(
        builder: &PolymarketOrderBuilder,
        request: PreparedOrderRequest,
    ) -> anyhow::Result<Self> {
        validate_request(&request)?;
        let side = PolymarketOrderSide::try_from(request.side)
            .map_err(|error| anyhow::anyhow!("invalid prepared order side: {error}"))?;
        let order_type = match request.kind {
            PreparedOrderKind::Limit => PolymarketOrderType::try_from(request.time_in_force),
            PreparedOrderKind::Market => {
                PolymarketOrderType::from_market_time_in_force(request.time_in_force)
            }
        }
        .map_err(|error| anyhow::anyhow!("invalid prepared order time in force: {error}"))?;
        let order = match request.kind {
            PreparedOrderKind::Limit => builder.build_limit_order(
                &request.token_id,
                side,
                request.price,
                request.amount,
                &request.expiration,
                request.neg_risk,
                request.tick_decimals,
            )?,
            PreparedOrderKind::Market => builder.build_market_order(
                &request.token_id,
                side,
                request.price,
                request.amount,
                request.neg_risk,
                request.tick_decimals,
            )?,
        };
        let expected_venue_order_id = builder
            .expected_order_id(&order, request.neg_risk)?
            .to_string();
        let scale = Decimal::from(1_000_000u32);
        let expected_base_quantity = match (request.kind, side) {
            (PreparedOrderKind::Market, PolymarketOrderSide::Buy) => order.taker_amount / scale,
            _ => request.amount,
        };
        let fingerprint = fingerprint(&request, &expected_venue_order_id);
        Ok(Self {
            schema_version: PREPARED_SCHEMA_VERSION,
            request,
            order,
            order_type,
            expected_venue_order_id,
            expected_base_quantity,
            fingerprint,
        })
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.schema_version != PREPARED_SCHEMA_VERSION {
            anyhow::bail!(
                "unsupported prepared order schema version {}",
                self.schema_version
            );
        }
        validate_request(&self.request)?;
        let actual_order_id = format!("{:#x}", order_hash(&self.order, self.request.neg_risk)?);
        if actual_order_id != self.expected_venue_order_id {
            anyhow::bail!("prepared order payload does not match its expected venue order ID");
        }
        let expected = fingerprint(&self.request, &self.expected_venue_order_id);
        if self.fingerprint != expected {
            anyhow::bail!("prepared order fingerprint mismatch");
        }
        Ok(())
    }
}

pub fn register_prepared_order(prepared: PreparedPolymarketOrder) -> anyhow::Result<()> {
    prepared.validate()?;
    let id = prepared.request.client_order_id.clone();
    let mut registry = PREPARED
        .lock()
        .map_err(|_| anyhow::anyhow!("prepared order registry poisoned"))?;
    if let Some(existing) = registry.get(&id) {
        if existing.fingerprint == prepared.fingerprint {
            return Ok(());
        }
        anyhow::bail!("prepared order identity conflict for {id}");
    }
    registry.insert(id, prepared);
    Ok(())
}

pub fn discard_prepared_order(client_order_id: &str) -> bool {
    PREPARED
        .lock()
        .map(|mut registry| registry.remove(client_order_id).is_some())
        .unwrap_or(false)
}

pub(crate) fn take_prepared_order(
    request: &PreparedOrderRequest,
) -> anyhow::Result<Option<PreparedPolymarketOrder>> {
    let mut registry = PREPARED
        .lock()
        .map_err(|_| anyhow::anyhow!("prepared order registry poisoned"))?;
    let Some(prepared) = registry.get(&request.client_order_id) else {
        return Ok(None);
    };
    prepared.validate()?;
    let expected_fingerprint = fingerprint(request, &prepared.expected_venue_order_id);
    if prepared.request != *request || prepared.fingerprint != expected_fingerprint {
        anyhow::bail!(
            "prepared order execution fields changed for {}",
            request.client_order_id
        );
    }
    Ok(registry.remove(&request.client_order_id))
}

pub(crate) fn take_prepared_market_order(
    request_without_cap: &PreparedOrderRequest,
) -> anyhow::Result<Option<PreparedPolymarketOrder>> {
    let mut registry = PREPARED
        .lock()
        .map_err(|_| anyhow::anyhow!("prepared order registry poisoned"))?;
    let Some(prepared) = registry.get(&request_without_cap.client_order_id) else {
        return Ok(None);
    };
    prepared.validate()?;
    let mut expected = request_without_cap.clone();
    expected.price = prepared.request.price;
    let expected_fingerprint = fingerprint(&expected, &prepared.expected_venue_order_id);
    if prepared.request != expected || prepared.fingerprint != expected_fingerprint {
        anyhow::bail!(
            "prepared market order execution fields changed for {}",
            request_without_cap.client_order_id
        );
    }
    Ok(registry.remove(&request_without_cap.client_order_id))
}

fn validate_request(request: &PreparedOrderRequest) -> anyhow::Result<()> {
    if request.client_order_id.trim().is_empty() || request.token_id.trim().is_empty() {
        anyhow::bail!("prepared order identity is incomplete");
    }
    if request.amount <= Decimal::ZERO
        || request.price <= Decimal::ZERO
        || request.price >= Decimal::ONE
    {
        anyhow::bail!("prepared order amount or price is outside protocol bounds");
    }
    if request.kind == PreparedOrderKind::Market
        && !matches!(request.time_in_force, TimeInForce::Ioc | TimeInForce::Fok)
    {
        anyhow::bail!("prepared market orders require IOC or FOK");
    }
    Ok(())
}

fn fingerprint(request: &PreparedOrderRequest, expected_venue_order_id: &str) -> String {
    let canonical = format!(
        "{}|{}|{:?}|{}|{:?}|{}|{}|{}|{:?}|{}|{}|{}|{}|{}",
        PREPARED_SCHEMA_VERSION,
        request.client_order_id,
        request.kind,
        request.token_id,
        request.side,
        request.price.normalize(),
        request.amount.normalize(),
        request.quote_quantity,
        request.time_in_force,
        request.post_only,
        request.neg_risk,
        request.tick_decimals,
        request.expiration,
        expected_venue_order_id,
    );
    format!("{:#x}", keccak256(canonical.as_bytes()))
}
