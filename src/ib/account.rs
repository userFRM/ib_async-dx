//! `IB`'s account methods: account updates, the account summary, positions
//! and P&L subscriptions.

use std::any::Any;
use std::sync::{Arc, Weak};

use super::IBHandle;
use crate::engine::EClient;
use crate::error::{Error, Result};
use crate::live::{Holder, Live, Observed};
use crate::objects::{PnL, PnLSingle, Position};
use crate::owner::{Class, LOG_IB, Shared};
use crate::pending::{Pending, Reply};
use crate::requests::{Ask, ReqKey};

impl IBHandle {
    /// Asks for `account`'s values and portfolio, kept up to date from then
    /// on, and returns once both are in: ib_async's `reqAccountUpdates`.
    /// `connect` already asks for them.
    pub fn req_account_updates(&self, account: &str) -> Result<()> {
        self.req_account_updates_async(account)
            .wait(self.config().request_timeout)
    }

    /// `req_account_updates`' async form: `reqAccountUpdatesAsync`.
    pub fn req_account_updates_async(&self, account: &str) -> Pending<()> {
        let ask = Ask::AccountUpdates {
            account: account.to_owned(),
        };
        question(&self.shared, ask, None)
    }

    /// Asks for the values of `account` and `model_code`, `""` for any,
    /// kept up to date from then on, and returns once they are in:
    /// ib_async's `reqAccountUpdatesMulti`. They reach `account_values()`.
    pub fn req_account_updates_multi(&self, account: &str, model_code: &str) -> Result<()> {
        self.req_account_updates_multi_async(account, model_code)
            .wait(self.config().request_timeout)
    }

    /// `req_account_updates_multi`'s async form:
    /// `reqAccountUpdatesMultiAsync`.
    pub fn req_account_updates_multi_async(&self, account: &str, model_code: &str) -> Pending<()> {
        let (account, model_code) = (account.to_owned(), model_code.to_owned());
        self.shared.request(move |ib, token, reply: Reply<()>| {
            let (id, client) = match req_id(ib) {
                Ok(v) => v,
                Err(e) => {
                    reply.send(Err(e));
                    return;
                }
            };
            let mut x = ib.core().requests.exec_as(ReqKey::Id(id), token);
            x.waiter = Some(Box::new(reply));
            // ib_async's future holds the list `startReq` made, so an error
            // that ends it fails the call only with `RaiseRequestErrors`.
            x.acc = Some(Box::new(()));
            ib.core().requests.insert(x);
            client.req_account_updates_multi(id, &account, &model_code, false);
            ib.queue.sent(1);
        })
    }

    /// Asks for every account's summary, kept up to date from then on, and
    /// returns once it is in: ib_async's `reqAccountSummary`. Nothing
    /// cancels it; `account_summary()` asks for it when none has arrived.
    pub fn req_account_summary(&self) -> Result<()> {
        self.req_account_summary_async()
            .wait(self.config().request_timeout)
    }

    /// `req_account_summary`'s async form: `reqAccountSummaryAsync`.
    pub fn req_account_summary_async(&self) -> Pending<()> {
        self.shared.account_summary_request()
    }

    /// Every account's positions, as the account states them now:
    /// ib_async's `reqPositions`. They also reach `positions()`, which is
    /// kept up to date.
    pub fn req_positions(&self) -> Result<Vec<Position>> {
        self.req_positions_async()
            .wait(self.config().request_timeout)
    }

    /// `req_positions`' async form: `reqPositionsAsync`.
    pub fn req_positions_async(&self) -> Pending<Vec<Position>> {
        question(
            &self.shared,
            Ask::Positions,
            Some(Box::new(Vec::<Position>::new())),
        )
    }

    /// Subscribes to the P&L of `account` and `model_code`, kept up to date
    /// in the object given, which `pnl()` also lists: ib_async's `reqPnL`.
    /// A second subscription of the same pair is `Err(Value)`.
    pub fn req_pnl(&self, account: &str, model_code: &str) -> Result<Live<PnL>> {
        let (account, model_code) = (account.to_owned(), model_code.to_owned());
        self.shared.step(Class::Request, move |ib| {
            let key = (account.clone(), model_code.clone());
            if ib.core().state.pnl_key_to_req_id.contains_key(&key) {
                return Err(Error::Value(format!(
                    "reqPnL: already subscribed for account {account}, modelCode {model_code}"
                )));
            }
            let (id, client) = req_id(ib)?;
            let pnl = bound(
                ib,
                PnL {
                    account: account.clone(),
                    model_code: model_code.clone(),
                    ..PnL::default()
                },
            );
            {
                let mut c = ib.core();
                c.state.pnl_key_to_req_id.insert(key, id);
                c.state.req_id_to_pnl.insert(id, pnl.clone());
            }
            client.req_pnl(id, &account, &model_code);
            ib.queue.sent(1);
            Ok(pnl)
        })
    }

    /// Ends the P&L subscription of `account` and `model_code`: ib_async's
    /// `cancelPnL`. One that does not exist is logged.
    pub fn cancel_pnl(&self, account: &str, model_code: &str) -> Result<()> {
        let (account, model_code) = (account.to_owned(), model_code.to_owned());
        self.shared.step(Class::Control, move |ib| {
            let key = (account.clone(), model_code.clone());
            let Some(id) = ib.core().state.pnl_key_to_req_id.remove(&key) else {
                log::error!(
                    target: LOG_IB,
                    "cancelPnL: No subscription for account {account}, modelCode {model_code}"
                );
                return Ok(());
            };
            session(ib)?.cancel_pnl(id);
            let gone = ib.core().state.req_id_to_pnl.shift_remove(&id);
            drop(gone);
            Ok(())
        })
    }

    /// Subscribes to the P&L of one position, `con_id` in `account` and
    /// `model_code`, kept up to date in the object given, which
    /// `pnl_single()` also lists: ib_async's `reqPnLSingle`. A second
    /// subscription of the same three is `Err(Value)`.
    pub fn req_pnl_single(
        &self,
        account: &str,
        model_code: &str,
        con_id: i64,
    ) -> Result<Live<PnLSingle>> {
        let (account, model_code) = (account.to_owned(), model_code.to_owned());
        self.shared.step(Class::Request, move |ib| {
            let key = (account.clone(), model_code.clone(), con_id);
            if ib.core().state.pnl_single_key_to_req_id.contains_key(&key) {
                return Err(Error::Value(format!(
                    "reqPnLSingle: already subscribed for account {account}, \
                     modelCode {model_code}, conId {con_id}"
                )));
            }
            let (id, client) = req_id(ib)?;
            let pnl = bound(
                ib,
                PnLSingle {
                    account: account.clone(),
                    model_code: model_code.clone(),
                    con_id,
                    ..PnLSingle::default()
                },
            );
            {
                let mut c = ib.core();
                c.state.pnl_single_key_to_req_id.insert(key, id);
                c.state.req_id_to_pnl_single.insert(id, pnl.clone());
            }
            client.req_pnl_single(id, &account, &model_code, con_id);
            ib.queue.sent(1);
            Ok(pnl)
        })
    }

    /// Ends the P&L subscription of `con_id` in `account` and `model_code`:
    /// ib_async's `cancelPnLSingle`. One that does not exist is logged.
    pub fn cancel_pnl_single(&self, account: &str, model_code: &str, con_id: i64) -> Result<()> {
        let (account, model_code) = (account.to_owned(), model_code.to_owned());
        self.shared.step(Class::Control, move |ib| {
            let key = (account.clone(), model_code.clone(), con_id);
            let Some(id) = ib.core().state.pnl_single_key_to_req_id.remove(&key) else {
                log::error!(
                    target: LOG_IB,
                    "cancelPnLSingle: No subscription for account {account}, \
                     modelCode {model_code}, conId {con_id}"
                );
                return Ok(());
            };
            session(ib)?.cancel_pnl_single(id);
            let gone = ib.core().state.req_id_to_pnl_single.shift_remove(&id);
            drop(gone);
            Ok(())
        })
    }
}

/// The published session, for a send: ib_async's `send` raises
/// `ConnectionError` without one.
fn session(ib: &Shared) -> Result<Arc<EClient>> {
    ib.connected().map(|c| c.1).ok_or(Error::NotConnected)
}

/// ib_async's `getReqId` (cl:162-169): the next id of the session, which
/// must be up.
fn req_id(ib: &Shared) -> Result<(i64, Arc<EClient>)> {
    let client = session(ib)?;
    if client.session_over() {
        return Err(Error::NotConnected);
    }
    let floor = client.order_id_floor();
    let id = ib.core().ids.allocate(floor, 1)?;
    Ok((id, client))
}

/// `v`, held by the IB `ib`, so every write of it is the owner's.
fn bound<T: Observed>(ib: &Arc<Shared>, v: T) -> Live<T> {
    let live = Live::new(v);
    let holder: Weak<Shared> = Arc::downgrade(ib);
    live.bind(holder as Weak<dyn Holder>);
    live
}

/// Asks an unnumbered question in its lane, its answer assembled in `acc`
/// when it is a list.
fn question<T: Send + 'static>(
    ib: &Arc<Shared>,
    ask: Ask,
    acc: Option<Box<dyn Any + Send>>,
) -> Pending<T> {
    ib.request(move |ib, token, reply: Reply<T>| {
        // Dropped unsent, the reply fails `NotConnected`.
        let Ok(client) = session(ib) else {
            return;
        };
        let send = {
            let mut c = ib.core();
            let mut x = c.requests.exec_as(ask.key(), token);
            x.waiter = Some(Box::new(reply));
            x.acc = acc;
            c.requests.ask(ask, Some(x))
        };
        if let Some(ask) = send {
            ib.send_ask(&client, &ask);
        }
    })
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::mpsc;
    use std::task::{Context, Poll, Waker};
    use std::thread;

    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::*;
    use crate::contract::Contract;
    use crate::engine::{ControlCommand, ErrorOrigin, SharedState};
    use crate::event::set_on_owner;
    use crate::ib::{ConnectOptions, IBConfig, StartupFetch};
    use crate::objects::{AccountValue, IBDefaults};
    use crate::owner::Via;
    use crate::record::{Callback, Capture};
    use crate::tests::{capture_logs, errors_here};
    use crate::timer::Clock;

    /// Makes this test's thread the owner while it lives, so each method
    /// runs inline and each read is applied here.
    struct AsOwner;

    impl AsOwner {
        fn new() -> Self {
            set_on_owner(true);
            AsOwner
        }
    }

    impl Drop for AsOwner {
        fn drop(&mut self) {
            set_on_owner(false);
        }
    }

    /// An IB no owner serves: the test is its owner.
    fn ib() -> IBHandle {
        let shared = Shared::new(
            IBDefaults::default(),
            IBConfig::default(),
            Clock::manual(Timestamp::UNIX_EPOCH),
        );
        IBHandle { shared }
    }

    /// Connects `ib` on an engine for `DU123` whose loop never runs, and
    /// gives the channel the engine's commands arrive on.
    fn connect(ib: &IBHandle) -> mpsc::Receiver<ControlCommand> {
        let (tx, rx) = mpsc::channel();
        let client = EClient::from_parts(
            Arc::new(SharedState::new()),
            tx,
            thread::spawn(|| {}),
            "DU123".into(),
        );
        let opts = ConnectOptions {
            fetch_fields: StartupFetch::NONE,
            ..ConnectOptions::default()
        };
        let via = Via::Test(Some(Arc::new(client)));
        let (mut p, _) = ib.begin_connect(opts, true, Some(via)).unwrap();
        ib.shared.lap(&mut Capture::new(TimeZone::UTC));
        assert!(matches!(published(&mut p), Some(Ok(()))));
        sent(&rx);
        rx
    }

    fn published<T>(p: &mut Pending<T>) -> Option<Result<T>> {
        match Pin::new(p).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    }

    /// The commands sent since the last look, as the engine writes them.
    fn sent(rx: &mpsc::Receiver<ControlCommand>) -> Vec<String> {
        rx.try_iter().map(|c| format!("{c:?}")).collect()
    }

    /// The request id a sent command carries.
    fn id_in(command: &str) -> i64 {
        let (_, rest) = command.split_once("req_id: ").unwrap();
        rest.split(',').next().unwrap().parse().unwrap()
    }

    /// Applies `callbacks` as one read of the first generation.
    fn read(ib: &IBHandle, callbacks: Vec<Callback>) {
        ib.shared.apply_read(1, callbacks);
    }

    #[test]
    fn account_requests_are_sent_as_ib_asyncs_and_end_at_their_own_end() {
        let _o = AsOwner::new();
        let ib = ib();
        let rx = connect(&ib);
        let value = AccountValue {
            account: "DU123".into(),
            tag: "NetLiquidation".into(),
            value: "1000".into(),
            currency: "USD".into(),
            model_code: String::new(),
        };

        // The account's question, answered at its download's end.
        let mut p = ib.req_account_updates_async("DU123");
        assert_eq!(sent(&rx), [r#"Ask(AccountUpdates { account: "DU123" })"#]);
        read(&ib, vec![Callback::UpdateAccountValue(value.clone())]);
        assert!(published(&mut p).is_none());
        read(&ib, vec![Callback::AccountDownloadEnd("DU123".into())]);
        assert!(matches!(published(&mut p), Some(Ok(()))));

        // Numbered, without the ledger-only subset (ib:896), and ended only
        // under its own id.
        let mut p = ib.req_account_updates_multi_async("DU123", "");
        let commands = sent(&rx);
        let id = id_in(&commands[0]);
        assert_eq!(
            commands,
            [format!(
                r#"Ask(AccountUpdatesMulti {{ req_id: {id}, account: "DU123", model_code: "", ledger_and_nlv: false }})"#
            )]
        );
        let update = Callback::AccountUpdateMulti { req_id: id, value };
        read(&ib, vec![update, Callback::AccountUpdateMultiEnd(id + 1)]);
        assert!(published(&mut p).is_none());
        read(&ib, vec![Callback::AccountUpdateMultiEnd(id)]);
        assert!(matches!(published(&mut p), Some(Ok(()))));

        // The positions that arrived before its end.
        let position = Position {
            account: "DU123".into(),
            contract: Contract {
                con_id: 8314,
                ..Contract::default()
            },
            position: 10.0,
            avg_cost: 150.0,
        };
        let mut p = ib.req_positions_async();
        assert_eq!(sent(&rx), ["Ask(Positions)"]);
        read(&ib, vec![Callback::Position(position.clone())]);
        assert!(published(&mut p).is_none());
        read(&ib, vec![Callback::PositionEnd]);
        assert_eq!(published(&mut p).map(Result::unwrap), Some(vec![position]));
    }

    #[test]
    fn an_error_ends_account_updates_multi_as_raise_request_errors_says() {
        let _o = AsOwner::new();
        let ib = ib();
        let rx = connect(&ib);
        for raise in [false, true] {
            ib.set_config(IBConfig {
                raise_request_errors: raise,
                ..IBConfig::default()
            });
            let mut p = ib.req_account_updates_multi_async("DU999", "");
            let id = id_in(&sent(&rx)[0]);
            let refused = Callback::Error {
                origin: ErrorOrigin::Request { id, ends: true },
                code: 322,
                message: "refused".into(),
                advanced_order_reject_json: String::new(),
            };
            read(&ib, vec![refused]);
            let r = published(&mut p);
            if raise {
                let want = matches!(r, Some(Err(Error::Request { req_id, code: 322, .. })) if req_id == id);
                assert!(want, "{r:?}");
            } else {
                assert!(matches!(r, Some(Ok(()))), "{r:?}");
            }
        }
    }

    #[test]
    fn pnl_subscriptions_are_kept_by_key_and_cancelled_by_their_id() {
        capture_logs();
        let _o = AsOwner::new();
        let ib = ib();
        let rx = connect(&ib);
        let pnl = ib.req_pnl("DU123", "").unwrap();
        let single = ib.req_pnl_single("DU123", "", 8314).unwrap();
        let commands = sent(&rx);
        let (a, b) = (id_in(&commands[0]), id_in(&commands[1]));
        assert_eq!(
            commands,
            [
                format!(r#"SubscribePnl {{ req_id: {a}, single: false, account: "DU123" }}"#),
                format!(r#"SubscribePnl {{ req_id: {b}, single: true, account: "DU123" }}"#),
            ]
        );
        // The same key again is ib_async's assert, and sends nothing.
        assert!(matches!(ib.req_pnl("DU123", ""), Err(Error::Value(_))));
        let r = ib.req_pnl_single("DU123", "", 8314);
        assert!(matches!(r, Err(Error::Value(_))));
        assert!(sent(&rx).is_empty());

        // Each update reaches the object given, which the reads list.
        read(
            &ib,
            vec![
                Callback::Pnl {
                    req_id: a,
                    daily_pnl: 1.0,
                    unrealized_pnl: 2.0,
                    realized_pnl: 3.0,
                },
                Callback::PnlSingle {
                    req_id: b,
                    pos: 10.0,
                    daily_pnl: 4.0,
                    unrealized_pnl: 5.0,
                    realized_pnl: 6.0,
                    value: 7.0,
                },
            ],
        );
        assert_eq!(pnl.read().daily_pnl, 1.0);
        assert_eq!(single.read().position, 10.0);
        assert!(Live::ptr_eq(&ib.pnl("", "")[0], &pnl));
        assert!(Live::ptr_eq(&ib.pnl_single("", "", 0)[0], &single));

        // A cancel ends the id its key names; a key with none is logged.
        ib.cancel_pnl("DU123", "").unwrap();
        ib.cancel_pnl_single("DU123", "", 8314).unwrap();
        assert_eq!(
            sent(&rx),
            [
                format!("CancelPnl {{ req_id: {a}, single: false }}"),
                format!("CancelPnl {{ req_id: {b}, single: true }}"),
            ]
        );
        assert!(ib.pnl("", "").is_empty() && ib.pnl_single("", "", 0).is_empty());
        ib.cancel_pnl("DU123", "").unwrap();
        ib.cancel_pnl_single("DU123", "", 8314).unwrap();
        assert!(sent(&rx).is_empty());
        let logged = errors_here();
        let want = [
            "cancelPnL: No subscription for account DU123, modelCode ",
            "cancelPnLSingle: No subscription for account DU123, modelCode , conId 8314",
        ]
        .map(|m| (LOG_IB.to_owned(), m.to_owned()));
        assert!(logged.ends_with(&want), "{logged:?}");
    }
}
