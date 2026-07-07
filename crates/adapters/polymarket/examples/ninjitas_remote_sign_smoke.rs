use std::sync::Arc;

use log::LevelFilter;
use nautilus_common::{enums::Environment, logging::logger::LoggerConfig};
use nautilus_live::{config::LiveExecEngineConfig, node::LiveNode};
use nautilus_model::{identifiers::{AccountId, InstrumentId, StrategyId, TraderId}, types::Quantity};
use nautilus_polymarket::{common::{consts::POLYMARKET_CLIENT_ID, enums::SignatureType}, config::{PolymarketDataClientConfig, PolymarketExecClientConfig}, factories::{PolymarketDataClientFactory, PolymarketExecutionClientFactory}, filters::EventSlugFilter};
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::strategy::StrategyConfig;

const TRADER_ID: &str = "NINJA-REMOTE-SIGN-001";
const ACCOUNT_ID: &str = "POLYMARKET-001";
const NODE_NAME: &str = "NINJITAS-REMOTE-SIGN-SMOKE";
const STRATEGY_ID: &str = "REMOTE_SIGN_SMOKE-001";
const EVENT_SLUG: &str = "fifwc-usa-bel-2026-07-06-team-to-advance";
const INSTRUMENT_ID: &str = "0x83d646ac5646bf847f2dc0ce9c18c4d8909bbb7b050b31075afb9b67d3802b33-39213684548901066937591254517097057808881530423341112968547596122921810511702.POLYMARKET";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let trader_id = TraderId::from(TRADER_ID);
    let account_id = AccountId::from(ACCOUNT_ID);
    let client_id = *POLYMARKET_CLIENT_ID;
    let instrument_id = InstrumentId::from(INSTRUMENT_ID);

    let data_config = PolymarketDataClientConfig {
        filters: vec![Arc::new(EventSlugFilter::from_slugs(vec![EVENT_SLUG.to_string()]))],
        ..Default::default()
    };
    let exec_config = PolymarketExecClientConfig {
        trader_id,
        account_id,
        signature_type: SignatureType::Poly1271,
        ..Default::default()
    };

    let mut node = LiveNode::builder(trader_id, Environment::Live)?
        .with_name(NODE_NAME.to_string())
        .with_logging(LoggerConfig { stdout_level: LevelFilter::Info, ..Default::default() })
        .with_exec_engine_config(LiveExecEngineConfig { open_check_interval_secs: Some(10.0), position_check_interval_secs: Some(30.0), ..Default::default() })
        .add_data_client(None, Box::new(PolymarketDataClientFactory), Box::new(data_config))?
        .add_exec_client(None, Box::new(PolymarketExecutionClientFactory), Box::new(exec_config))?
        .with_reconciliation(true)
        .with_reconciliation_lookback_mins(120)
        .with_timeout_reconciliation(60)
        .with_delay_post_stop_secs(3)
        .build()?;

    let tester_config = ExecTesterConfig::builder()
        .base(StrategyConfig { strategy_id: Some(StrategyId::from(STRATEGY_ID)), external_order_claims: Some(vec![instrument_id]), ..Default::default() })
        .instrument_id(instrument_id)
        .client_id(client_id)
        .order_qty(Quantity::from("5"))
        .use_post_only(true)
        .tob_offset_ticks(51)
        .enable_limit_buys(true)
        .enable_limit_sells(false)
        .enable_stop_buys(false)
        .enable_stop_sells(false)
        .reduce_only_on_stop(false)
        .log_data(false)
        .build()?;

    node.add_strategy(ExecTester::new(tester_config))?;
    tokio::select! {
        result = node.run() => result?,
        () = tokio::time::sleep(std::time::Duration::from_secs(35)) => {
            node.stop().await?;
        }
    }
    Ok(())
}
