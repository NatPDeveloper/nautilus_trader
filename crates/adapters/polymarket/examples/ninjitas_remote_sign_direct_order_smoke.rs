use std::sync::Arc;

use alloy_primitives::Address;
use nautilus_model::identifiers::VenueOrderId;
use nautilus_polymarket::{
    common::{
        credential::Credential,
        enums::{PolymarketOrderSide, PolymarketOrderType, SignatureType},
    },
    execution::order_builder::PolymarketOrderBuilder,
    http::clob::PolymarketClobHttpClient,
    signing::eip712::{RemoteEip712Signer, RemoteEip712SignerConfig},
};
use rust_decimal_macros::dec;

const TOKEN_ID: &str = "18812649149814341758733697580460697418474693998558159483117100240528657629879";

fn env_required(key: &str) -> anyhow::Result<String> {
    std::env::var(key).map_err(|_| anyhow::anyhow!("{key} is required"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let signer_address = env_required("POLYMARKET_SIGNER_ADDRESS")?;
    let api_owner_address = signer_address.clone();
    let funder = env_required("POLYMARKET_FUNDER")?;
    let signing_url = env_required("POLYMARKET_SIGNING_URL")?;
    let credential = Credential::resolve(
        Some(env_required("POLYMARKET_API_KEY")?),
        Some(env_required("POLYMARKET_API_SECRET")?),
        Some(env_required("POLYMARKET_PASSPHRASE")?),
    )?;

    let remote_signer = RemoteEip712Signer::new(RemoteEip712SignerConfig {
        url: signing_url,
        auth_token: std::env::var("POLYMARKET_SIGNING_TOKEN")
            .ok()
            .or_else(|| std::env::var("SIDECAR_INTERNAL_TOKEN").ok()),
        signer_address: signer_address.parse::<Address>()?,
        funder_address: Some(funder.parse::<Address>()?),
        account_id: std::env::var("POLYMARKET_SIGNING_ACCOUNT_ID").ok().and_then(|v| v.parse().ok()),
        privy_user_id: std::env::var("POLYMARKET_SIGNING_PRIVY_USER_ID").ok(),
        strategy_id: std::env::var("POLYMARKET_SIGNING_STRATEGY_ID").ok(),
        profile_id: std::env::var("POLYMARKET_SIGNING_PROFILE_ID").ok(),
    })?;
    let builder = PolymarketOrderBuilder::new(
        Arc::new(remote_signer),
        signer_address,
        funder,
        SignatureType::Poly1271,
    );
    let order = builder.build_limit_order(
        TOKEN_ID,
        PolymarketOrderSide::Buy,
        dec!(0.01),
        dec!(5),
        "0",
        false,
        2,
    )?;
    let expected_order_id: VenueOrderId = builder.expected_order_id(&order, false)?;
    let http = PolymarketClobHttpClient::new(
        credential,
        api_owner_address,
        std::env::var("POLYMARKET_CLOB_BASE_URL").ok(),
        30,
    )?;

    println!("posting remote-signed order expected_order_id={expected_order_id}");
    let response = http.post_order(&order, PolymarketOrderType::GTC, true).await?;
    println!("post_response={response:?}");
    let venue_order_id = response.order_id.clone().unwrap_or_else(|| expected_order_id.to_string());
    let after_post = http.get_order_optional(&venue_order_id).await?;
    println!("after_post={after_post:?}");
    let cancel = http.cancel_order(&venue_order_id).await?;
    println!("cancel_response={cancel:?}");
    let after_cancel = http.get_order_optional(&venue_order_id).await?;
    println!("after_cancel={after_cancel:?}");
    Ok(())
}
