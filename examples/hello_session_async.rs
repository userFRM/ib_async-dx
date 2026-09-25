//! The same session from inside a runtime, through the async twins.
//!
//! Usage: IB_USERNAME=... IB_PASSWORD=... cargo run --example hello_session_async

use std::env;
use std::time::Duration;

use ib_async_dx::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ib = IB::new()?;
    let mut opts = ConnectOptions::default();
    opts.config.username = env::var("IB_USERNAME")?;
    opts.config.password = env::var("IB_PASSWORD")?;
    ib.connect_async(opts).await?;

    let mut contracts = [Contract::stock("SPY", "SMART", "USD")];
    ib.qualify_contracts_async(&mut contracts, false).await?;
    let [spy] = contracts;

    // A handler holds a handle, which does not keep the session open.
    let handle = ib.handle();
    ib.pending_tickers_event().connect(move |tickers| {
        for t in tickers {
            let t = t.read();
            let symbol = t.contract.as_ref().map_or("", |c| c.symbol.as_str());
            println!(
                "{symbol} bid {} ask {} ({} tickers)",
                t.bid,
                t.ask,
                handle.tickers().len()
            );
        }
    });

    for t in ib
        .req_tickers_async(std::slice::from_ref(&spy), false)
        .await?
    {
        println!("snapshot: last {}", t.read().last);
    }
    let _ticker = ib.req_mkt_data(&spy, "", false, false, &[])?;

    // An event is also a stream of its values.
    let mut updates = ib.update_event().subscribe();
    for _ in 0..3 {
        updates.recv_async().await?;
    }

    let bars = ib
        .req_historical_data_async(
            &spy,
            "",
            "1 D",
            "1 hour",
            "TRADES",
            true,
            1,
            false,
            &[],
            None,
        )
        .await?;
    println!("{} bars", bars.read().bars.len());

    let state = ib
        .what_if_order_async(&spy, &Order::limit("BUY", 1.0, 1.00))
        .await?;
    println!("initial margin change {}", state.init_margin_change);
    IB::wait_until_async(jiff::Timestamp::now() + Duration::from_secs(1)).await?;

    // Dropping the IB logs out and waits for the engine, so it is done
    // where blocking is allowed.
    tokio::task::spawn_blocking(move || drop(ib)).await?;
    Ok(())
}
