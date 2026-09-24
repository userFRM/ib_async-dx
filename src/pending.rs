//! A request's answer, still to come: the waiter a request returns, the
//! slot the owner decides and then publishes, and the guard it replies
//! through.
//!
//! The owner decides a slot inside a unit of its work, and the result
//! becomes visible when the outermost unit ends, whether it returned or
//! panicked. A slot decided outside any unit is published at once.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the owner and the request methods use these")
)]

use std::any::Any;
use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::event::{lock, on_owner, panic_message};
use crate::util::global_error_event;

/// An execution's identity, unique for its IB's life: what a waiter that
/// leaves reports to its IB.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Token(pub(crate) u64);

/// An IB, as the place a departed waiter's token goes: its list of
/// abandoned executions, which the owner retires each lap.
pub(crate) trait Abandon: Send + Sync {
    /// Records that the waiter of `token` is gone. It takes only a leaf
    /// lock.
    fn abandon(&self, token: Token);
}

/// A command still waiting for room in its IB's queue, held by the
/// [`Pending`] a request returned. Dropping it drops the command, whose
/// reply guard then fails the slot, and removes the waker it keeps.
pub(crate) trait Unadmitted: Send {
    /// Admits the command when its queue has room, and gives `None`. A
    /// closed queue gives `None` too, the command dropped. Otherwise it keeps
    /// `waker`, in place of the one it kept before, to wake when room
    /// appears or the queue closes, and gives the command back.
    fn poll_admit(self: Box<Self>, waker: &Waker) -> Option<Box<dyn Unadmitted>>;
}

/// Where a waiter reports that it left: its IB and its execution's token.
#[derive(Clone)]
pub(crate) struct Registration {
    shared: Weak<dyn Abandon>,
    token: Token,
}

impl Registration {
    /// The execution `token` of the IB `shared`.
    pub(crate) fn new(shared: Weak<dyn Abandon>, token: Token) -> Self {
        Registration { shared, token }
    }

    fn abandon(&self) {
        if let Some(ib) = self.shared.upgrade() {
            ib.abandon(self.token);
        }
    }
}

enum SlotState<T> {
    Waiting,
    /// The owner's winner, not yet published.
    Decided(Result<T>),
    Ready(Result<T>),
    Taken,
    Abandoned,
}

struct SlotInner<T> {
    state: SlotState<T>,
    /// The latest poller's, kept apart from the result: stored and replaced
    /// in `Waiting` and `Decided` alike, and taken at publication.
    waker: Option<Waker>,
    /// The owner has run the command.
    ran: bool,
}

impl<T> SlotInner<T> {
    /// The published result, taken; `None` while none is published.
    fn take(&mut self) -> Option<Result<T>> {
        match std::mem::replace(&mut self.state, SlotState::Taken) {
            SlotState::Ready(r) => Some(r),
            s @ (SlotState::Taken | SlotState::Abandoned) => {
                self.state = s;
                Some(Err(Error::Value("the result was already taken".to_owned())))
            }
            s => {
                self.state = s;
                None
            }
        }
    }
}

struct Slot<T> {
    inner: Mutex<SlotInner<T>>,
    ready: Condvar,
}

impl<T> Slot<T> {
    fn new(state: SlotState<T>) -> Arc<Self> {
        Arc::new(Slot {
            inner: Mutex::new(SlotInner {
                state,
                waker: None,
                ran: false,
            }),
            ready: Condvar::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, SlotInner<T>> {
        lock(&self.inner)
    }

    /// The waiter's deadline: `Waiting` becomes `Abandoned` and gives
    /// `true`. A slot the owner has decided stays the owner's.
    fn expire(&self) -> bool {
        let mut g = self.lock();
        let waiting = matches!(g.state, SlotState::Waiting);
        if waiting {
            g.state = SlotState::Abandoned;
        }
        waiting
    }

    /// The published result, or `Pending` with `waker` kept. A waker that
    /// `will_wake` the kept one leaves it in place; otherwise `waker` is
    /// cloned before the lock and the one it displaces dropped after it.
    fn poll(&self, waker: &Waker) -> Poll<Result<T>> {
        {
            let mut g = self.lock();
            if let Some(r) = g.take() {
                return Poll::Ready(r);
            }
            if g.waker.as_ref().is_some_and(|w| w.will_wake(waker)) {
                return Poll::Pending;
            }
        }
        let waker = waker.clone();
        let displaced = {
            let mut g = self.lock();
            if let Some(r) = g.take() {
                return Poll::Ready(r);
            }
            g.waker.replace(waker)
        };
        drop(displaced);
        Poll::Pending
    }

    /// Blocks until the result is published. At `deadline` a slot still
    /// waiting is abandoned and gives `Timeout`; a decided one is waited for
    /// until its unit ends.
    fn wait(&self, deadline: Option<Instant>) -> Result<T> {
        let mut g = self.lock();
        loop {
            if let Some(r) = g.take() {
                return r;
            }
            g = match deadline.filter(|_| matches!(g.state, SlotState::Waiting)) {
                None => self.ready.wait(g).unwrap_or_else(PoisonError::into_inner),
                Some(d) => match d.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => {
                        self.ready
                            .wait_timeout(g, left)
                            .unwrap_or_else(PoisonError::into_inner)
                            .0
                    }
                    _ => {
                        g.state = SlotState::Abandoned;
                        return Err(Error::Timeout);
                    }
                },
            };
        }
    }
}

impl<T: Send + 'static> Slot<T> {
    /// `Waiting` becomes `Decided(r)`, published when the current unit ends,
    /// or at once outside any unit. Gives `false` when the waiter left or
    /// the slot was decided first, and drops `r` after the lock.
    fn decide(self: &Arc<Self>, r: Result<T>) -> bool {
        let lost = {
            let mut g = self.lock();
            if matches!(g.state, SlotState::Waiting) {
                g.state = SlotState::Decided(r);
                None
            } else {
                Some(r)
            }
        };
        let won = lost.is_none();
        drop(lost);
        if won {
            record(self.clone());
        }
        won
    }
}

/// A decided slot, as a unit's lists hold it.
trait Decision: Send + Sync {
    /// `Decided` becomes `Ready` under the lock, and the waker kept then is
    /// taken out; the condvar is notified after the lock. Gives the waker,
    /// to wake once every slot of the unit is published.
    fn publish(&self) -> Option<Waker>;
    /// Decides the slot with `e`.
    fn fail(self: Arc<Self>, e: Error);
}

impl<T: Send + 'static> Decision for Slot<T> {
    fn publish(&self) -> Option<Waker> {
        let waker = {
            let mut g = self.lock();
            match std::mem::replace(&mut g.state, SlotState::Taken) {
                SlotState::Decided(r) => {
                    g.state = SlotState::Ready(r);
                    g.waker.take()
                }
                s => {
                    g.state = s;
                    None
                }
            }
        };
        self.ready.notify_all();
        waker
    }

    fn fail(self: Arc<Self>, e: Error) {
        self.decide(Err(e));
    }
}

/// The decisions of the unit this thread runs.
#[derive(Default)]
struct Unit {
    /// Slots decided, published when the outermost unit ends.
    decided: Vec<Arc<dyn Decision>>,
    /// Slots whose reply guard a panic dropped, failed when the unit that
    /// caught it ends.
    orphans: Vec<Arc<dyn Decision>>,
}

thread_local! {
    /// `Some` while this thread runs a unit.
    static UNIT: RefCell<Option<Unit>> = const { RefCell::new(None) };
}

/// Holds `d` for the end of the current unit, or publishes it now outside
/// any unit.
fn record(d: Arc<dyn Decision>) {
    let mut d = Some(d);
    let _ = UNIT.try_with(|u| {
        if let Some(u) = u.borrow_mut().as_mut() {
            u.decided.extend(d.take());
        }
    });
    if let Some(d) = d {
        wake(d.publish());
    }
}

/// Wakes a published slot's waiter. A panic in the waker is caught and
/// logged at ERROR, as asyncio logs an exception raised in a callback.
fn wake(waker: Option<Waker>) {
    if let Some(w) = waker
        && let Err(p) = catch_unwind(AssertUnwindSafe(|| w.wake()))
    {
        let msg = panic_message(&*p);
        log::error!(target: "asyncio", "Exception in callback: {msg}");
    }
}

/// Holds `d` for the current unit to fail with its panic's error; gives it
/// back outside any unit.
fn orphan(d: Arc<dyn Decision>) -> Option<Arc<dyn Decision>> {
    let mut d = Some(d);
    let _ = UNIT.try_with(|u| {
        if let Some(u) = u.borrow_mut().as_mut() {
            u.orphans.extend(d.take());
        }
    });
    d
}

/// Runs `f` as one unit of the owner's work, under `catch_unwind`. Slots
/// decided inside it are published when the outermost unit ends, whether
/// `f` returned or panicked. A reply guard that `f`'s panic dropped fails its
/// slot with the error `on_panic` makes of that panic.
pub(crate) fn unit<R>(
    f: impl FnOnce() -> R,
    on_panic: impl FnOnce(&(dyn Any + Send)) -> Error,
) -> thread::Result<R> {
    let (outer, mark) =
        UNIT.with_borrow_mut(|u| (u.is_some(), u.get_or_insert_default().orphans.len()));
    let r = catch_unwind(AssertUnwindSafe(f));
    let orphans = UNIT.with_borrow_mut(|u| {
        u.as_mut()
            .map(|u| u.orphans.split_off(mark))
            .unwrap_or_default()
    });
    if !orphans.is_empty() {
        let e = match &r {
            Err(p) => on_panic(&**p),
            Ok(_) => Error::NotConnected,
        };
        for o in orphans {
            o.fail(e.clone());
        }
    }
    if !outer {
        let decided = UNIT.take().map(|u| u.decided).unwrap_or_default();
        let wakers: Vec<_> = decided.iter().map(|d| d.publish()).collect();
        drop(decided);
        wakers.into_iter().for_each(wake);
    }
    r
}

/// The owner's side of a waiter's slot: a command's reply guard. Dropped
/// without a reply, it fails the slot with `NotConnected`, or, when a unit's
/// panic drops it, with the error that unit makes of the panic.
pub(crate) struct Reply<T: Send + 'static>(Option<Arc<Slot<T>>>);

impl<T: Send + 'static> Reply<T> {
    /// Marks the command run and gives whether its waiter is still there.
    /// Both happen under the slot's lock, so a waiter leaving meanwhile
    /// either sees the mark and reports its token, or is seen gone here.
    pub(crate) fn start(&self) -> bool {
        self.0.as_ref().is_some_and(|s| {
            let mut g = s.lock();
            g.ran = true;
            matches!(g.state, SlotState::Waiting)
        })
    }

    /// Decides the slot with `r`: published when the current unit ends, or
    /// at once outside any unit. Gives `false`, dropping `r`, when the
    /// waiter left or the slot was decided first.
    pub(crate) fn send(mut self, r: Result<T>) -> bool {
        self.0.take().is_some_and(|s| s.decide(r))
    }
}

impl<T: Send + 'static> Drop for Reply<T> {
    fn drop(&mut self) {
        let Some(s) = self.0.take() else {
            return;
        };
        let s: Arc<dyn Decision> = s;
        let s = if thread::panicking() {
            orphan(s)
        } else {
            Some(s)
        };
        if let Some(s) = s {
            s.fail(Error::NotConnected);
        }
    }
}

/// Wakes the thread parked in [`Pending::wait`] or `util::block_on`.
pub(crate) struct Unpark(pub(crate) Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// The handler a blocking wait connects to `global_error_event`: it decides
/// the waiter's slot with the event's error and, when that wins, reports the
/// waiter's token to its IB.
fn listener<T: Send + 'static>(
    slot: Arc<Slot<T>>,
    reg: Option<Registration>,
) -> impl Fn(&Error) + Send + Sync + 'static {
    move |e| {
        if slot.decide(Err(e.clone()))
            && let Some(reg) = &reg
        {
            reg.abandon();
        }
    }
}

/// A request's answer, still to come: what ib_async's `*Async` methods that
/// return an `asyncio.Future` return.
///
/// Await it on any executor, or block on it with [`Pending::wait`].
/// Dropping it leaves the request as dropping ib_async's future does:
/// nothing is cancelled with IB, and what still arrives updates the IB's
/// state.
#[must_use]
pub struct Pending<T> {
    slot: Arc<Slot<T>>,
    reg: Option<Registration>,
    /// A command still waiting for room, admitted at the first poll or
    /// `wait`. Held here, not in the slot its reply guard completes, so no
    /// cycle forms and a drop takes no slot lock.
    unadmitted: Option<Box<dyn Unadmitted>>,
}

impl<T: Send + 'static> Pending<T> {
    /// A waiter and the reply guard the owner completes it through.
    pub(crate) fn new(reg: Option<Registration>) -> (Self, Reply<T>) {
        let slot = Slot::new(SlotState::Waiting);
        let reply = Reply(Some(slot.clone()));
        let pending = Pending {
            slot,
            reg,
            unadmitted: None,
        };
        (pending, reply)
    }

    /// A waiter already failed with `e`, where ib_async raises at the call.
    pub(crate) fn failed(e: Error) -> Self {
        Pending {
            slot: Slot::new(SlotState::Ready(Err(e))),
            reg: None,
            unadmitted: None,
        }
    }

    /// The waiter's deadline: a slot still waiting is abandoned, and this
    /// gives `true`; a slot the owner has decided stays the owner's.
    pub(crate) fn expire(&self) -> bool {
        self.slot.expire()
    }

    /// Holds `command`, which found no room, to be admitted at the first
    /// poll or `wait`.
    pub(crate) fn hold(&mut self, command: Box<dyn Unadmitted>) {
        self.unadmitted = Some(command);
    }

    /// Blocks until the answer comes: ib_async's `util.run(future)`, with
    /// `timeout` as its `wait_for`. `None` waits without limit.
    ///
    /// At the deadline the waiter leaves and gets `Err(Timeout)`; a request
    /// still waiting for room is then never sent. An answer the owner has
    /// already decided is waited for instead. A peer or internal close of
    /// any IB fails the wait with that close's error, as `util.run` raises
    /// `globalErrorEvent`'s value. Called inside a handler, on the thread
    /// that runs every IB, it gives `Err(Value)` at once, as asyncio refuses
    /// to run a loop that is already running.
    pub fn wait(mut self, timeout: Option<Duration>) -> Result<T> {
        if on_owner() {
            return Err(Error::Value(
                "This event loop is already running".to_owned(),
            ));
        }
        let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
        let event = global_error_event();
        let id = event.connect(listener(self.slot.clone(), self.reg.clone()));
        let r = match self.admit(deadline) {
            Some(r) => r,
            None => self.slot.wait(deadline),
        };
        event.disconnect(id);
        r
    }

    /// Admits a held command, parked until there is room or `deadline`.
    /// Gives the wait's end when it comes first: the slot's result, or
    /// `Timeout` with nothing admitted.
    fn admit(&mut self, deadline: Option<Instant>) -> Option<Result<T>> {
        let mut command = self.unadmitted.take()?;
        // The slot keeps the same waker, so a decision published while the
        // command waits for room wakes this thread too.
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        loop {
            if let Poll::Ready(r) = self.slot.poll(&waker) {
                return Some(r);
            }
            if !matches!(self.slot.lock().state, SlotState::Waiting) {
                // Decided, by the `global_error_event` listener: nobody
                // waits for the command, so it is never sent, and the
                // decision is waited for until its unit ends.
                drop(command);
                break;
            }
            command = match command.poll_admit(&waker) {
                Some(back) => back,
                None => break,
            };
            match deadline.map(|d| d.checked_duration_since(Instant::now())) {
                None => thread::park(),
                Some(Some(left)) if !left.is_zero() => thread::park_timeout(left),
                Some(_) if self.slot.expire() => return Some(Err(Error::Timeout)),
                // Decided since the check: the next turn sees it.
                Some(_) => {}
            }
        }
        // Admitted, or dropped: the answer is waited for on the condvar.
        let parked = self.slot.lock().waker.take();
        drop(parked);
        None
    }
}

impl<T> fmt::Debug for Pending<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pending").finish_non_exhaustive()
    }
}

impl<T> Future for Pending<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T>> {
        let this = self.get_mut();
        if let Some(command) = this.unadmitted.take() {
            this.unadmitted = command.poll_admit(cx.waker());
            if this.unadmitted.is_some() {
                return Poll::Pending;
            }
        }
        this.slot.poll(cx.waker())
    }
}

impl<T> Drop for Pending<T> {
    /// A held command is dropped first, outside any lock. A waiting slot is
    /// marked abandoned; once the command has run, the token is reported
    /// whatever the state.
    fn drop(&mut self) {
        drop(self.unadmitted.take());
        let ran = {
            let mut g = self.slot.lock();
            if matches!(g.state, SlotState::Waiting) {
                g.state = SlotState::Abandoned;
            }
            g.ran
        };
        if ran && let Some(reg) = &self.reg {
            reg.abandon();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use std::sync::mpsc;

    use super::*;
    use crate::event::set_on_owner;
    use crate::tests::GLOBAL_ERRORS;

    #[derive(Default)]
    struct Ib(Mutex<Vec<Token>>);

    impl Abandon for Ib {
        fn abandon(&self, token: Token) {
            lock(&self.0).push(token);
        }
    }

    impl Ib {
        fn tokens(&self) -> Vec<Token> {
            lock(&self.0).clone()
        }
    }

    fn registered<T: Send + 'static>(ib: &Arc<Ib>, n: u64) -> (Pending<T>, Reply<T>) {
        let shared: Weak<dyn Abandon> = Arc::downgrade(ib) as Weak<dyn Abandon>;
        Pending::new(Some(Registration::new(shared, Token(n))))
    }

    /// An IB's queue as a held command sees it.
    #[derive(Default)]
    struct Queue {
        room: bool,
        closed: bool,
        admitted: Vec<Reply<i32>>,
        waker: Option<Waker>,
    }

    struct Command {
        queue: Arc<Mutex<Queue>>,
        reply: Option<Reply<i32>>,
    }

    impl Unadmitted for Command {
        fn poll_admit(mut self: Box<Self>, waker: &Waker) -> Option<Box<dyn Unadmitted>> {
            let queue = self.queue.clone();
            let mut q = lock(&queue);
            if q.closed {
                return None;
            }
            if q.room {
                q.admitted.extend(self.reply.take());
                return None;
            }
            q.waker = Some(waker.clone());
            drop(q);
            Some(self)
        }
    }

    impl Drop for Command {
        fn drop(&mut self) {
            let w = lock(&self.queue).waker.take();
            drop(w);
        }
    }

    fn held(queue: &Arc<Mutex<Queue>>, reply: Reply<i32>) -> Box<dyn Unadmitted> {
        Box::new(Command {
            queue: queue.clone(),
            reply: Some(reply),
        })
    }

    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, SeqCst);
        }
    }

    fn counted() -> (Arc<Count>, Waker) {
        let c = Arc::new(Count::default());
        (c.clone(), Waker::from(c))
    }

    fn woken(c: &Count) -> usize {
        c.0.load(SeqCst)
    }

    fn poll<T>(p: &mut Pending<T>, w: &Waker) -> Poll<Result<T>> {
        Pin::new(p).poll(&mut Context::from_waker(w))
    }

    fn state<T>(p: &Pending<T>) -> &'static str {
        match p.slot.lock().state {
            SlotState::Waiting => "waiting",
            SlotState::Decided(_) => "decided",
            SlotState::Ready(_) => "ready",
            SlotState::Taken => "taken",
            SlotState::Abandoned => "abandoned",
        }
    }

    fn in_unit<R>(f: impl FnOnce() -> R) -> R {
        unit(f, |_| Error::NotConnected).unwrap()
    }

    #[test]
    fn the_owner_wins_and_the_deadline_waits_for_publication() {
        let _w = lock(&GLOBAL_ERRORS);
        let (p, reply) = Pending::<i32>::new(None);
        in_unit(|| {
            assert!(reply.send(Ok(1)));
            // The waiter's deadline, reached now, leaves a decided slot to
            // the owner.
            assert!(!p.slot.expire());
            assert_eq!(state(&p), "decided");
        });
        assert_eq!(state(&p), "ready");
        assert_eq!(p.wait(Some(Duration::ZERO)).unwrap(), 1);
    }

    /// A value that records, when dropped, whether its slot's lock was free.
    struct Probe(Weak<Slot<Probe>>, Arc<AtomicBool>);

    impl Drop for Probe {
        fn drop(&mut self) {
            if let Some(s) = self.0.upgrade() {
                self.1.store(s.inner.try_lock().is_ok(), SeqCst);
            }
        }
    }

    #[test]
    fn the_deadline_wins_and_the_owners_value_is_dropped_after_the_lock() {
        let _w = lock(&GLOBAL_ERRORS);
        let (p, reply) = Pending::<Probe>::new(None);
        let slot = p.slot.clone();
        let r = p.wait(Some(Duration::from_millis(5)));
        assert!(matches!(r, Err(Error::Timeout)));
        assert!(matches!(slot.lock().state, SlotState::Abandoned));
        let free = Arc::new(AtomicBool::new(false));
        assert!(!reply.send(Ok(Probe(Arc::downgrade(&slot), free.clone()))));
        assert!(free.load(SeqCst));
    }

    #[test]
    fn a_decided_slot_is_seen_only_when_its_unit_ends() {
        let _w = lock(&GLOBAL_ERRORS);
        let (mut polled, reply1) = Pending::<i32>::new(None);
        let (waited, reply2) = Pending::<i32>::new(None);
        let (count, w) = counted();
        assert!(poll(&mut polled, &w).is_pending());
        let t = in_unit(|| {
            assert!(reply1.send(Ok(1)) && reply2.send(Ok(2)));
            assert!(poll(&mut polled, &w).is_pending());
            // The waiter comes after the decision, so its deadline, passed
            // at once, finds the slot decided.
            let t = thread::spawn(move || waited.wait(Some(Duration::ZERO)));
            thread::sleep(Duration::from_millis(30));
            assert!(
                !t.is_finished(),
                "a decided slot's waiter waits past its deadline"
            );
            t
        });
        assert_eq!(woken(&count), 1);
        assert!(matches!(poll(&mut polled, &w), Poll::Ready(Ok(1))));
        assert_eq!(t.join().unwrap().unwrap(), 2);

        // A unit that panics after deciding still publishes.
        let (mut p, reply) = Pending::<i32>::new(None);
        let r = unit(
            || {
                reply.send(Ok(3));
                panic!("after the decision")
            },
            |_| Error::NotConnected,
        );
        assert!(r.is_err());
        assert!(matches!(poll(&mut p, Waker::noop()), Poll::Ready(Ok(3))));
    }

    #[test]
    fn the_waker_kept_while_decided_is_the_one_woken() {
        let (mut first, reply1) = Pending::<i32>::new(None);
        let (mut moved, reply2) = Pending::<i32>::new(None);
        let ((a, wa), (b, wb), (c, wc)) = (counted(), counted(), counted());
        assert!(poll(&mut moved, &wb).is_pending());
        in_unit(|| {
            assert!(reply1.send(Ok(1)) && reply2.send(Ok(2)));
            // First polled while decided.
            assert!(poll(&mut first, &wa).is_pending());
            // Moved to another task while decided.
            assert!(poll(&mut moved, &wc).is_pending());
        });
        assert_eq!((woken(&a), woken(&b), woken(&c)), (1, 0, 1));
        assert!(matches!(poll(&mut first, &wa), Poll::Ready(Ok(1))));
        assert!(matches!(poll(&mut moved, &wc), Poll::Ready(Ok(2))));
    }

    /// A waker that records whether another slot was published when it ran.
    struct Sees(Weak<Slot<i32>>, AtomicBool);

    impl Wake for Sees {
        fn wake(self: Arc<Self>) {
            if let Some(s) = self.0.upgrade() {
                let ready = matches!(s.lock().state, SlotState::Ready(_));
                self.1.store(ready, SeqCst);
            }
        }
    }

    #[test]
    fn a_unit_publishes_every_slot_before_it_wakes_a_waiter() {
        let (mut first, reply1) = Pending::<i32>::new(None);
        let (second, reply2) = Pending::<i32>::new(None);
        let sees = Arc::new(Sees(Arc::downgrade(&second.slot), AtomicBool::new(false)));
        assert!(poll(&mut first, &Waker::from(sees.clone())).is_pending());
        in_unit(|| assert!(reply1.send(Ok(1)) && reply2.send(Ok(2))));
        assert!(sees.1.load(SeqCst));
    }

    /// A waker that checks, when woken and when dropped, that its slot's
    /// lock is free.
    struct Checked {
        slot: Weak<Slot<i32>>,
        woken: AtomicUsize,
        free: Arc<AtomicBool>,
    }

    impl Checked {
        fn check(&self) {
            if let Some(s) = self.slot.upgrade()
                && s.inner.try_lock().is_err()
            {
                self.free.store(false, SeqCst);
            }
        }
    }

    impl Wake for Checked {
        fn wake(self: Arc<Self>) {
            self.check();
            self.woken.fetch_add(1, SeqCst);
        }
    }

    impl Drop for Checked {
        fn drop(&mut self) {
            self.check();
        }
    }

    #[test]
    fn a_waker_that_will_wake_stays_and_others_move_outside_the_lock() {
        let (mut p, reply) = Pending::<i32>::new(None);
        let free = Arc::new(AtomicBool::new(true));
        let checked = |p: &Pending<i32>| {
            Arc::new(Checked {
                slot: Arc::downgrade(&p.slot),
                woken: AtomicUsize::new(0),
                free: free.clone(),
            })
        };
        let a = checked(&p);
        let wa = Waker::from(a.clone());
        assert!(poll(&mut p, &wa).is_pending());
        assert_eq!(Arc::strong_count(&a), 3);
        assert!(poll(&mut p, &wa).is_pending());
        assert_eq!(Arc::strong_count(&a), 3, "left in place");
        let gone = Arc::downgrade(&a);
        drop((a, wa));
        let b = checked(&p);
        assert!(poll(&mut p, &Waker::from(b.clone())).is_pending());
        assert!(gone.upgrade().is_none(), "the displaced waker is dropped");
        assert!(reply.send(Ok(1)));
        assert_eq!(b.woken.load(SeqCst), 1);
        assert!(free.load(SeqCst));
    }

    #[test]
    fn a_held_command_is_admitted_at_its_first_poll() {
        let queue = Arc::new(Mutex::new(Queue::default()));
        let (mut p, reply) = Pending::<i32>::new(None);
        p.hold(held(&queue, reply));
        let ((a, wa), (b, wb)) = (counted(), counted());
        // No room: the poll keeps its waker for room, replaced on a re-poll.
        assert!(poll(&mut p, &wa).is_pending());
        assert!(poll(&mut p, &wb).is_pending());
        assert!(lock(&queue).admitted.is_empty());
        assert!(lock(&queue).waker.as_ref().unwrap().will_wake(&wb));
        lock(&queue).room = true;
        assert!(poll(&mut p, &wb).is_pending());
        let reply = lock(&queue).admitted.pop().unwrap();
        assert!(lock(&queue).waker.is_none());
        assert!(reply.start() && reply.send(Ok(7)));
        assert_eq!((woken(&a), woken(&b)), (0, 1));
        assert!(matches!(poll(&mut p, &wb), Poll::Ready(Ok(7))));

        // A closed queue drops the command, whose guard fails the slot.
        let (mut p, reply) = Pending::<i32>::new(None);
        p.hold(held(&queue, reply));
        lock(&queue).closed = true;
        let r = poll(&mut p, Waker::noop());
        assert!(matches!(r, Poll::Ready(Err(Error::NotConnected))));
    }

    #[test]
    fn a_pending_dropped_unadmitted_sends_nothing_and_frees_its_slot() {
        let ib = Arc::new(Ib::default());
        let queue = Arc::new(Mutex::new(Queue::default()));
        let (mut p, reply) = registered::<i32>(&ib, 1);
        p.hold(held(&queue, reply));
        assert!(poll(&mut p, Waker::noop()).is_pending());
        let slot = Arc::downgrade(&p.slot);
        drop(p);
        let q = lock(&queue);
        assert!(q.admitted.is_empty() && q.waker.is_none());
        assert!(slot.upgrade().is_none());
        assert!(ib.tokens().is_empty());
    }

    #[test]
    fn a_drop_marks_the_slot_before_its_command_ran_and_reports_after() {
        let ib = Arc::new(Ib::default());
        // Before: the owner, running the command, finds the waiter gone.
        let (p, reply) = registered::<i32>(&ib, 1);
        drop(p);
        assert!(!reply.start());
        assert!(!reply.send(Ok(1)));
        assert!(ib.tokens().is_empty());
        // After: one token, whatever the state.
        let (p, reply) = registered::<i32>(&ib, 2);
        assert!(reply.start());
        drop(p);
        let (p, reply) = registered::<i32>(&ib, 3);
        assert!(reply.start() && reply.send(Ok(1)));
        drop(p);
        let (mut p, reply) = registered::<i32>(&ib, 4);
        assert!(reply.start() && reply.send(Ok(1)));
        assert!(matches!(poll(&mut p, Waker::noop()), Poll::Ready(Ok(1))));
        drop(p);
        assert_eq!(ib.tokens(), [Token(2), Token(3), Token(4)]);
    }

    #[test]
    fn ran_is_set_and_read_under_the_slots_lock() {
        let ib = Arc::new(Ib::default());
        for n in 0..200 {
            let (p, reply) = registered::<i32>(&ib, n);
            let t = thread::spawn(move || drop(p));
            let waiting = reply.start();
            t.join().unwrap();
            // The waiter was there when the command ran exactly when its
            // drop saw the mark and reported its token.
            assert_eq!(waiting, ib.tokens().contains(&Token(n)), "{n}");
        }
    }

    #[test]
    fn a_reply_guard_dropped_unrun_fails_its_slot() {
        let (mut p, reply) = Pending::<i32>::new(None);
        drop(reply);
        let r = poll(&mut p, Waker::noop());
        assert!(matches!(r, Poll::Ready(Err(Error::NotConnected))));

        // Dropped by a step's panic: the error the unit makes of it.
        let (mut p, reply) = Pending::<i32>::new(None);
        let r = unit(
            move || {
                let _held = reply;
                panic!("step failed")
            },
            |p| {
                let msg = p.downcast_ref::<&str>().copied().unwrap_or_default();
                Error::Connection(format!("internal error: {msg}"))
            },
        );
        assert!(r.is_err());
        let r = poll(&mut p, Waker::noop());
        assert!(
            matches!(r, Poll::Ready(Err(Error::Connection(m))) if m == "internal error: step failed")
        );
    }

    #[test]
    fn global_error_event_fails_a_wait_and_reports_its_token() {
        let _w = lock(&GLOBAL_ERRORS);
        let ib = Arc::new(Ib::default());
        let (p, reply) = registered::<i32>(&ib, 9);
        assert!(reply.start());
        let before = global_error_event().len();
        let t = thread::spawn(move || p.wait(None));
        while global_error_event().len() == before {
            thread::yield_now();
        }
        global_error_event().emit(&Error::Connection("Socket disconnect".to_owned()));
        let r = t.join().unwrap();
        assert!(matches!(r, Err(Error::Connection(m)) if m == "Socket disconnect"));
        assert_eq!(global_error_event().len(), before);
        assert!(ib.tokens().contains(&Token(9)));
        assert!(!reply.send(Ok(1)));
    }

    #[test]
    fn a_listeners_decision_inside_a_unit_is_published_when_it_ends() {
        let ib = Arc::new(Ib::default());
        let (mut p, _reply) = registered::<i32>(&ib, 5);
        let listen = listener(p.slot.clone(), p.reg.clone());
        in_unit(|| {
            listen(&Error::Connection("Socket disconnect".to_owned()));
            assert_eq!(state(&p), "decided");
            assert!(poll(&mut p, Waker::noop()).is_pending());
            assert_eq!(ib.tokens(), [Token(5)]);
        });
        let r = poll(&mut p, Waker::noop());
        assert!(matches!(r, Poll::Ready(Err(Error::Connection(_)))));
    }

    #[test]
    fn wait_admits_a_held_command_or_nothing_by_its_deadline() {
        let _w = lock(&GLOBAL_ERRORS);
        let queue = Arc::new(Mutex::new(Queue::default()));
        let (mut p, reply) = Pending::<i32>::new(None);
        p.hold(held(&queue, reply));
        let r = p.wait(Some(Duration::from_millis(20)));
        assert!(matches!(r, Err(Error::Timeout)));
        assert!(lock(&queue).admitted.is_empty() && lock(&queue).waker.is_none());

        let (mut p, reply) = Pending::<i32>::new(None);
        p.hold(held(&queue, reply));
        let t = thread::spawn(move || p.wait(None));
        let waker = loop {
            let mut q = lock(&queue);
            if let Some(w) = q.waker.take() {
                q.room = true;
                break w;
            }
            drop(q);
            thread::yield_now();
        };
        waker.wake();
        let reply = loop {
            if let Some(r) = lock(&queue).admitted.pop() {
                break r;
            }
            thread::yield_now();
        };
        assert!(reply.start() && reply.send(Ok(8)));
        assert_eq!(t.join().unwrap().unwrap(), 8);
    }

    #[test]
    fn wait_on_the_owner_thread_fails_at_once() {
        thread::spawn(|| {
            set_on_owner(true);
            let (p, _reply) = Pending::<i32>::new(None);
            let r = p.wait(None);
            assert!(matches!(r, Err(Error::Value(m)) if m == "This event loop is already running"));
        })
        .join()
        .unwrap();
    }

    #[test]
    fn finished_waiters() {
        let _w = lock(&GLOBAL_ERRORS);
        // A deadline past what an `Instant` holds is no deadline.
        let (p, reply) = Pending::<i32>::new(None);
        assert!(reply.send(Ok(2)));
        assert_eq!(p.wait(Some(Duration::MAX)).unwrap(), 2);
        let r = Pending::<i32>::failed(Error::NotConnected).wait(None);
        assert!(matches!(r, Err(Error::NotConnected)));
        // Polled again after its result.
        let mut p = Pending::<i32>::failed(Error::Timeout);
        assert!(matches!(
            poll(&mut p, Waker::noop()),
            Poll::Ready(Err(Error::Timeout))
        ));
        assert!(matches!(
            poll(&mut p, Waker::noop()),
            Poll::Ready(Err(Error::Value(_)))
        ));
    }

    #[test]
    fn pending_and_reply_are_send() {
        fn send<T: Send>() {}
        send::<Pending<i32>>();
        send::<Reply<i32>>();
    }

    #[test]
    fn a_nested_unit_publishes_when_the_outermost_ends() {
        let (p, reply) = Pending::<i32>::new(None);
        in_unit(|| {
            in_unit(|| assert!(reply.send(Ok(1))));
            assert_eq!(state(&p), "decided");
        });
        assert_eq!(state(&p), "ready");
    }

    #[test]
    fn a_command_waiting_for_room_is_dropped_once_the_listener_decides() {
        let _w = lock(&GLOBAL_ERRORS);
        let queue = Arc::new(Mutex::new(Queue::default()));
        let (mut p, reply) = Pending::<i32>::new(None);
        p.hold(held(&queue, reply));
        let before = global_error_event().len();
        let t = thread::spawn(move || p.wait(None));
        let waker = loop {
            let mut q = lock(&queue);
            if let Some(w) = q.waker.take() {
                break w;
            }
            drop(q);
            thread::yield_now();
        };
        in_unit(|| {
            global_error_event().emit(&Error::Connection("Socket disconnect".to_owned()));
            // Room appears while the decision waits for the unit's end.
            lock(&queue).room = true;
            waker.wake();
            thread::sleep(Duration::from_millis(30));
        });
        let r = t.join().unwrap();
        assert!(matches!(r, Err(Error::Connection(m)) if m == "Socket disconnect"));
        assert!(lock(&queue).admitted.is_empty());
        assert_eq!(global_error_event().len(), before);
    }

    /// A command that finds no room, and whose first try has the owner
    /// decide its slot in a unit that stays open a while.
    struct DecidedMeanwhile(Option<Reply<i32>>);

    impl Unadmitted for DecidedMeanwhile {
        fn poll_admit(mut self: Box<Self>, _: &Waker) -> Option<Box<dyn Unadmitted>> {
            if let Some(reply) = self.0.take() {
                let (tx, rx) = mpsc::channel();
                thread::spawn(move || {
                    in_unit(|| {
                        assert!(reply.send(Ok(4)));
                        tx.send(()).unwrap();
                        thread::sleep(Duration::from_millis(30));
                    })
                });
                rx.recv().unwrap();
            }
            Some(self)
        }
    }

    #[test]
    fn a_deadline_passed_while_waiting_for_room_leaves_a_decided_slot_to_the_owner() {
        let _w = lock(&GLOBAL_ERRORS);
        let (mut p, reply) = Pending::<i32>::new(None);
        p.hold(Box::new(DecidedMeanwhile(Some(reply))));
        assert_eq!(p.wait(Some(Duration::ZERO)).unwrap(), 4);
    }
}
