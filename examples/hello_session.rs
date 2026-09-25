//! A session, as an ib_async program holds one.
//!
//! Usage: IB_USERNAME=... IB_PASSWORD=... cargo run --example hello_session

use std::env;
use std::time::Duration;

use ib_async_dx::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ib = IB::new()?;
    let mut opts = ConnectOptions::default();
    opts.config.username = env::var("IB_USERNAME")?;
    opts.config.password = env::var("IB_PASSWORD")?;
    ib.connect(opts)?;

    let mut contracts = [Contract::stock("SPY", "SMART", "USD")];
    ib.qualify_contracts(&mut contracts)?;
    let [spy] = contracts;

    // A handler holds a handle, which does not keep the session open: the
    // session ends when `ib` is dropped, whatever the handlers hold.
    let handle = ib.handle();
    ib.order_status_event().connect(move |trade| {
        let t = trade.read();
        let open = handle.open_trades().len();
        println!(
            "order {} is {} ({open} open)",
            t.order.read().order_id,
            t.order_status.status
        );
    });

    // A ticker fills as its ticks arrive; reading it waits on nothing.
    let ticker = ib.req_mkt_data(&spy, "", false, false, &[])?;
    IB::sleep(Duration::from_secs(3))?;
    let t = ticker.read();
    println!("bid {} ask {} last {}", t.bid, t.ask, t.last);

    let bars = ib.req_historical_data(
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
    )?;
    for bar in bars.read().bars.iter().take(3) {
        println!("{:?} close {}", bar.date, bar.close);
    }

    // The order is the value both sides hold: the trade carries this handle.
    let order = Live::new(Order::limit("BUY", 1.0, 1.00));
    let trade = ib.place_order(&spy, &order)?;
    ib.wait_on_update(Some(Duration::from_secs(5)))?;
    if !trade.read().is_done() {
        ib.cancel_order(&order, "")?;
    }

    for p in ib.positions("") {
        println!("{:<8} {:>10}", p.contract.symbol, p.position);
    }
    for v in ib
        .account_values("")
        .iter()
        .filter(|v| v.tag == "NetLiquidation")
    {
        println!("{:<20} {} {}", v.tag, v.value, v.currency);
    }
    println!("{} trades, {} fills", ib.trades().len(), ib.fills().len());

    // Dropping the IB logs out and returns once the engine has stopped.
    drop(ib);
    Ok(())
}
