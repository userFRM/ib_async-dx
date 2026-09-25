//! Deadlines and scheduled callbacks: the heaps the owner fires, the owner's
//! table of `schedule` callbacks and async sleeps, the handle `schedule`
//! returns, and the clock they read.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use jiff::Timestamp;

use crate::event::{lock, panic_message};

/// Wall and monotonic time. The system's, or in tests a manual clock that
/// moves only when told.
#[derive(Clone, Default)]
pub(crate) struct Clock {
    manual: Option<Arc<Mutex<(Instant, Timestamp)>>>,
}

impl Clock {
    /// The system's clock.
    pub(crate) fn system() -> Clock {
        Clock::default()
    }

    /// A clock that starts at `wall` and moves only by [`Clock::advance`].
    #[cfg(test)]
    pub(crate) fn manual(wall: Timestamp) -> Clock {
        Clock {
            manual: Some(Arc::new(Mutex::new((Instant::now(), wall)))),
        }
    }

    /// Moves a manual clock on by `by`.
    #[cfg(test)]
    pub(crate) fn advance(&self, by: Duration) {
        if let Some(m) = &self.manual {
            let mut m = lock(m);
            m.0 += by;
            m.1 = m.1.checked_add(by).unwrap();
        }
    }

    /// The monotonic now, which deadlines are measured in.
    pub(crate) fn now(&self) -> Instant {
        match &self.manual {
            Some(m) => lock(m).0,
            None => Instant::now(),
        }
    }

    /// The wall-clock now.
    pub(crate) fn wall(&self) -> Timestamp {
        match &self.manual {
            Some(m) => lock(m).1,
            None => Timestamp::now(),
        }
    }

    /// The deadline at which the wall clock reads `when`: now if that has
    /// passed, `None` if it lies past what an `Instant` can hold (no
    /// deadline).
    pub(crate) fn instant_at(&self, when: Timestamp) -> Option<Instant> {
        let (now, wall) = (self.now(), self.wall());
        match Duration::try_from(when.duration_since(wall)) {
            Ok(wait) => now.checked_add(wait),
            Err(_) => Some(now),
        }
    }
}

/// A deadline's key in its heap, kept to cancel it. Ids are unique in the
/// process, so the owner's heap takes the timer table's ids as they are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct DeadlineId(u64);

impl DeadlineId {
    fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        DeadlineId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// Deadlines in time order. One per IB holds its request deadlines, polls
/// and idle checks; the owner's holds `schedule` callbacks and async sleeps.
/// A cancelled entry is dropped at once and its place in the order when the
/// order is rebuilt, which happens once more than half of it is cancelled.
pub(crate) struct DeadlineHeap<K> {
    order: BinaryHeap<Reverse<(Instant, u64)>>,
    live: HashMap<u64, K>,
}

impl<K> Default for DeadlineHeap<K> {
    fn default() -> Self {
        DeadlineHeap {
            order: BinaryHeap::new(),
            live: HashMap::new(),
        }
    }
}

impl<K> DeadlineHeap<K> {
    /// Adds `k`, due at `at`.
    pub(crate) fn insert(&mut self, at: Instant, k: K) -> DeadlineId {
        let id = DeadlineId::next();
        self.insert_as(id, at, k);
        id
    }

    fn insert_as(&mut self, id: DeadlineId, at: Instant, k: K) {
        self.order.push(Reverse((at, id.0)));
        self.live.insert(id.0, k);
    }

    /// Removes the entry `id`, if it has not fired, and gives it back.
    pub(crate) fn cancel(&mut self, id: DeadlineId) -> Option<K> {
        let k = self.live.remove(&id.0);
        if self.order.len() > 2 * self.live.len() {
            let live = &self.live;
            self.order.retain(|Reverse((_, id))| live.contains_key(id));
        }
        k
    }

    /// Removes and gives back every entry due at `now`, earliest first. An
    /// entry added while these are handled waits for the next call, so a
    /// callback that reschedules itself at now runs once per call.
    pub(crate) fn take_due(&mut self, now: Instant) -> Vec<K> {
        let mut due = Vec::new();
        while let Some(&Reverse((at, id))) = self.order.peek() {
            if at > now {
                break;
            }
            self.order.pop();
            due.extend(self.live.remove(&id));
        }
        due
    }

    /// When the earliest entry is due.
    pub(crate) fn next_due(&mut self) -> Option<Instant> {
        while let Some(&Reverse((at, id))) = self.order.peek() {
            if self.live.contains_key(&id) {
                return Some(at);
            }
            self.order.pop();
        }
        None
    }

    /// How many entries are pending.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.live.len()
    }
}

impl DeadlineHeap<OwnerTimer> {
    /// Takes in the owner's timer table: what was added since the last call
    /// joins the heap, and what was dropped leaves it.
    pub(crate) fn take_timers(&mut self, changes: TimerChanges) {
        for id in changes.dropped {
            self.cancel(id);
        }
        for (id, (at, timer)) in changes.added {
            self.insert_as(id, at, timer);
        }
    }
}

/// What the idle check armed by `set_timeout` finds (ib_async's `_setTimer`,
/// wr:451-467).
#[derive(Debug, PartialEq)]
pub(crate) enum IdleCheck {
    /// Idle for this long, at least the timeout: `timeout_event` fires.
    Idle(Duration),
    /// Activity since: check again at this deadline, `None` being none.
    Rearm(Option<Instant>),
}

/// The idle check at `now`, the last activity having been at `last`.
pub(crate) fn idle_check(last: Instant, now: Instant, timeout: Duration) -> IdleCheck {
    let idle = now.saturating_duration_since(last);
    if idle >= timeout {
        IdleCheck::Idle(idle)
    } else {
        IdleCheck::Rearm(last.checked_add(timeout))
    }
}

/// The state a timer shares with its handle or sleep: whether it has fired
/// or been cancelled, and the waker to wake when it fires.
#[derive(Default)]
struct Cell(Mutex<(bool, Option<Waker>)>);

impl Cell {
    /// Marks the timer finished; `false` if it already was.
    fn finish(&self) -> bool {
        !std::mem::replace(&mut lock(&self.0).0, true)
    }
}

/// A `schedule` callback or an async sleep, in the owner's heap.
pub(crate) struct OwnerTimer {
    cell: Arc<Cell>,
    callback: Option<Box<dyn FnOnce() + Send>>,
}

impl OwnerTimer {
    /// Runs the callback, or wakes the sleep, unless it was cancelled or
    /// dropped. A panic in either is caught and logged at ERROR, as asyncio
    /// logs an exception raised in a callback.
    pub(crate) fn fire(self) {
        let waker = {
            let mut c = lock(&self.cell.0);
            if std::mem::replace(&mut c.0, true) {
                return;
            }
            c.1.take()
        };
        let run = || {
            if let Some(f) = self.callback {
                f();
            }
            if let Some(w) = waker {
                w.wake();
            }
        };
        if let Err(p) = catch_unwind(AssertUnwindSafe(run)) {
            let msg = panic_message(&*p);
            log::error!(target: "asyncio", "Exception in callback: {msg}");
        }
    }
}

/// The changes to the owner's timers since it last took them.
#[derive(Default)]
pub(crate) struct TimerChanges {
    added: HashMap<DeadlineId, (Instant, OwnerTimer)>,
    dropped: Vec<DeadlineId>,
}

#[derive(Default)]
struct Table {
    changes: TimerChanges,
}

/// The owner's timer table: the `schedule` callbacks and async sleeps
/// added or dropped since the owner last took it, one entry per pending
/// callback or live sleep. Adding one wakes the owner.
#[derive(Clone)]
pub(crate) struct Timers(Arc<TimersInner>);

struct TimersInner {
    table: Mutex<Table>,
    wake_owner: Box<dyn Fn() + Send + Sync>,
}

impl Timers {
    /// An empty table; `wake_owner` is called after each addition.
    pub(crate) fn new(wake_owner: impl Fn() + Send + Sync + 'static) -> Timers {
        Timers(Arc::new(TimersInner {
            table: Mutex::default(),
            wake_owner: Box::new(wake_owner),
        }))
    }

    /// Takes the changes made since the last take.
    pub(crate) fn take(&self) -> TimerChanges {
        std::mem::take(&mut lock(&self.0.table).changes)
    }

    /// Adds `callback`, to run on the owner at `at`; `None` is never
    /// (asyncio's `call_later`).
    pub(crate) fn schedule(
        &self,
        at: Option<Instant>,
        callback: impl FnOnce() + Send + 'static,
    ) -> TimerHandle {
        let cell = Arc::new(Cell::default());
        let id = at.map(|at| self.add(at, cell.clone(), Some(Box::new(callback))));
        TimerHandle {
            id,
            cell,
            timers: self.clone(),
        }
    }

    /// A future that completes at `at`, `None` being never. It joins the
    /// table at its first poll and leaves it when dropped.
    pub(crate) fn sleep(&self, at: Option<Instant>) -> Sleep {
        Sleep {
            at,
            timers: self.clone(),
            reg: None,
        }
    }

    fn add(
        &self,
        at: Instant,
        cell: Arc<Cell>,
        callback: Option<Box<dyn FnOnce() + Send>>,
    ) -> DeadlineId {
        let id = {
            let mut t = lock(&self.0.table);
            let id = DeadlineId::next();
            t.changes
                .added
                .insert(id, (at, OwnerTimer { cell, callback }));
            id
        };
        (self.0.wake_owner)();
        id
    }

    /// Takes `id` out: from the table if the owner has not taken it yet,
    /// else by telling the owner to drop it from its heap.
    fn remove(&self, id: DeadlineId) {
        let removed = {
            let mut t = lock(&self.0.table);
            let removed = t.changes.added.remove(&id);
            if removed.is_none() {
                t.changes.dropped.push(id);
            }
            removed
        };
        drop(removed);
    }
}

/// What `schedule` returns: asyncio's `TimerHandle`, from `call_later`.
pub struct TimerHandle {
    id: Option<DeadlineId>,
    cell: Arc<Cell>,
    timers: Timers,
}

impl fmt::Debug for TimerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerHandle").finish_non_exhaustive()
    }
}

impl TimerHandle {
    /// Cancels the callback: it never runs if cancelled before its time.
    pub fn cancel(&self) {
        if self.cell.finish()
            && let Some(id) = self.id
        {
            self.timers.remove(id);
        }
    }
}

/// An async sleep on the owner's timers.
pub(crate) struct Sleep {
    at: Option<Instant>,
    timers: Timers,
    reg: Option<(DeadlineId, Arc<Cell>)>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let Some(at) = this.at else {
            return Poll::Pending;
        };
        let cell = match &this.reg {
            Some((_, cell)) => cell.clone(),
            None => {
                let cell = Arc::new(Cell::default());
                let id = this.timers.add(at, cell.clone(), None);
                this.reg = Some((id, cell.clone()));
                cell
            }
        };
        {
            let c = lock(&cell.0);
            if c.0 {
                return Poll::Ready(());
            }
            if c.1.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                return Poll::Pending;
            }
        }
        // The new waker is cloned, and the old one dropped, outside the lock.
        let waker = cx.waker().clone();
        let (fired, old) = {
            let mut c = lock(&cell.0);
            if c.0 {
                (true, None)
            } else {
                (false, c.1.replace(waker))
            }
        };
        drop(old);
        if fired {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some((id, cell)) = self.reg.take()
            && cell.finish()
        {
            self.timers.remove(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Wake;

    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn manual() -> Clock {
        Clock::manual(Timestamp::from_second(1_700_000_000).unwrap())
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// One owner lap over its timers: take the table in, then fire what was
    /// due when the lap began.
    fn lap(heap: &mut DeadlineHeap<OwnerTimer>, timers: &Timers, now: Instant) {
        heap.take_timers(timers.take());
        for t in heap.take_due(now) {
            t.fire();
        }
    }

    #[test]
    fn handles_cross_threads() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<TimerHandle>();
        send_sync::<Sleep>();
        send_sync::<Timers>();
    }

    #[test]
    fn clock_manual_and_instant_at() {
        let c = manual();
        let (t0, w0) = (c.now(), c.wall());
        c.advance(secs(5));
        assert_eq!(c.now() - t0, secs(5));
        assert_eq!(c.wall().duration_since(w0).as_secs(), 5);
        let later = c.wall().checked_add(secs(10)).unwrap();
        assert_eq!(c.instant_at(later), Some(c.now() + secs(10)));
        assert_eq!(c.instant_at(w0), Some(c.now()));
        let sys = Clock::system();
        assert!(sys.now() <= Instant::now());
    }

    #[test]
    fn deadlines_fire_in_order_and_cancel() {
        let c = manual();
        let t0 = c.now();
        let mut h = DeadlineHeap::default();
        let b = h.insert(t0 + secs(2), "b");
        h.insert(t0 + secs(1), "a");
        h.insert(t0 + secs(2), "c");
        let d = h.insert(t0 + secs(3), "d");
        assert_eq!(h.next_due(), Some(t0 + secs(1)));
        assert!(h.take_due(c.now()).is_empty());
        assert_eq!(h.cancel(b), Some("b"));
        assert_eq!(h.cancel(b), None);
        c.advance(secs(2));
        assert_eq!(h.take_due(c.now()), vec!["a", "c"]);
        assert_eq!(h.next_due(), Some(t0 + secs(3)));
        assert_eq!(h.cancel(d), Some("d"));
        assert_eq!(h.next_due(), None);
        assert_eq!(h.len(), 0);
    }

    #[test]
    fn rebuilt_past_half_cancelled() {
        let t0 = Instant::now();
        let mut h = DeadlineHeap::default();
        let ids: Vec<_> = (0..10).map(|i| h.insert(t0 + secs(i), i)).collect();
        for id in &ids[..5] {
            h.cancel(*id);
        }
        assert_eq!(h.order.len(), 10);
        h.cancel(ids[5]);
        assert_eq!(h.order.len(), 4);
        assert_eq!(h.len(), 4);
        assert_eq!(h.take_due(t0 + secs(100)), vec![6, 7, 8, 9]);
    }

    #[test]
    fn idle_rearms_for_the_remainder() {
        let c = manual();
        let timeout = secs(60);
        let mut h = DeadlineHeap::default();
        let mut last = c.now();
        h.insert(last + timeout, "idle");
        // Activity at 40 s: the check at 60 s re-arms for 100 s.
        c.advance(secs(40));
        last = c.now();
        c.advance(secs(20));
        assert_eq!(h.take_due(c.now()), vec!["idle"]);
        let IdleCheck::Rearm(Some(at)) = idle_check(last, c.now(), timeout) else {
            panic!("activity since the last check re-arms it")
        };
        assert_eq!(at, last + timeout);
        h.insert(at, "idle");
        c.advance(secs(39));
        assert!(h.take_due(c.now()).is_empty());
        c.advance(secs(1));
        assert_eq!(h.take_due(c.now()), vec!["idle"]);
        assert_eq!(
            idle_check(last, c.now(), timeout),
            IdleCheck::Idle(secs(60))
        );
        assert_eq!(
            idle_check(last, c.now(), Duration::MAX),
            IdleCheck::Rearm(None)
        );
    }

    #[test]
    fn schedule_runs_at_its_time_and_cancel_stops_it() {
        let c = manual();
        let woken = Arc::new(AtomicUsize::new(0));
        let w = woken.clone();
        let timers = Timers::new(move || {
            w.fetch_add(1, Ordering::SeqCst);
        });
        let mut heap = DeadlineHeap::default();
        let ran = Arc::new(AtomicUsize::new(0));
        let (r1, r2) = (ran.clone(), ran.clone());
        let keep = timers.schedule(Some(c.now() + secs(1)), move || {
            r1.fetch_add(1, Ordering::SeqCst);
        });
        let stop = timers.schedule(Some(c.now() + secs(1)), move || {
            r2.fetch_add(10, Ordering::SeqCst);
        });
        assert_eq!(woken.load(Ordering::SeqCst), 2);
        lap(&mut heap, &timers, c.now());
        assert_eq!(heap.len(), 2);
        stop.cancel();
        c.advance(secs(1));
        lap(&mut heap, &timers, c.now());
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        assert_eq!(heap.len(), 0);
        // A cancel after the callback ran, or twice, leaves nothing behind.
        keep.cancel();
        stop.cancel();
        assert!(timers.take().dropped.is_empty());
        // Cancelled before the owner took it in: the table forgets it.
        let never = timers.schedule(Some(c.now()), || panic!("cancelled"));
        never.cancel();
        let changes = timers.take();
        assert!(changes.added.is_empty() && changes.dropped.is_empty());
        // No deadline: never added, never run.
        let none = timers.schedule(None, || panic!("no deadline"));
        assert!(timers.take().added.is_empty());
        none.cancel();
    }

    #[test]
    fn a_cancel_during_the_lap_that_fires_it_stops_it() {
        // asyncio skips a handle cancelled after it became ready.
        let c = manual();
        let timers = Timers::new(|| {});
        let mut heap = DeadlineHeap::default();
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        let second = Arc::new(Mutex::new(None::<TimerHandle>));
        let s = second.clone();
        timers.schedule(Some(c.now()), move || {
            if let Some(h) = s.lock().unwrap().as_ref() {
                h.cancel();
            }
        });
        let h = timers.schedule(Some(c.now()), move || {
            r.fetch_add(1, Ordering::SeqCst);
        });
        *second.lock().unwrap() = Some(h);
        lap(&mut heap, &timers, c.now());
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_self_rescheduling_callback_fires_once_per_lap() {
        let c = manual();
        let timers = Timers::new(|| {});
        let mut heap = DeadlineHeap::default();
        let fired = Arc::new(AtomicUsize::new(0));
        fn again(timers: Timers, at: Instant, fired: Arc<AtomicUsize>) {
            let t = timers.clone();
            timers.schedule(Some(at), move || {
                fired.fetch_add(1, Ordering::SeqCst);
                again(t, at, fired);
            });
        }
        again(timers.clone(), c.now(), fired.clone());
        for n in 1..=3 {
            lap(&mut heap, &timers, c.now());
            assert_eq!(fired.load(Ordering::SeqCst), n);
        }
        // The same holds for an entry added straight into the heap.
        let mut h = DeadlineHeap::default();
        h.insert(c.now(), ());
        for _ in 0..3 {
            let due = h.take_due(c.now());
            assert_eq!(due.len(), 1);
            h.insert(c.now(), ());
        }
    }

    #[test]
    fn a_panicking_callback_is_isolated() {
        let c = manual();
        let timers = Timers::new(|| {});
        let mut heap = DeadlineHeap::default();
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        timers.schedule(Some(c.now()), || panic!("callback"));
        timers.schedule(Some(c.now()), move || {
            r.fetch_add(1, Ordering::SeqCst);
        });
        lap(&mut heap, &timers, c.now());
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn async_sleeps() {
        let c = manual();
        let timers = Timers::new(|| {});
        let mut heap = DeadlineHeap::default();
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);

        let mut sleep = timers.sleep(Some(c.now() + secs(1)));
        // Nothing joins the table before the first poll.
        assert!(timers.take().added.is_empty());
        assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
        assert!(Pin::new(&mut sleep).poll(&mut cx).is_pending());
        lap(&mut heap, &timers, c.now());
        assert_eq!(heap.len(), 1);
        c.advance(secs(1));
        lap(&mut heap, &timers, c.now());
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert!(Pin::new(&mut sleep).poll(&mut cx).is_ready());
        drop(sleep);
        assert!(timers.take().dropped.is_empty());

        // Polled and dropped before the owner takes it: the table is empty.
        for _ in 0..3 {
            let mut s = timers.sleep(Some(c.now() + secs(1)));
            assert!(Pin::new(&mut s).poll(&mut cx).is_pending());
        }
        let changes = timers.take();
        assert!(changes.added.is_empty() && changes.dropped.is_empty());

        // Dropped after the owner took it: it leaves the heap, unwoken.
        let mut s = timers.sleep(Some(c.now() + secs(1)));
        assert!(Pin::new(&mut s).poll(&mut cx).is_pending());
        lap(&mut heap, &timers, c.now());
        assert_eq!(heap.len(), 1);
        drop(s);
        c.advance(secs(1));
        lap(&mut heap, &timers, c.now());
        assert_eq!(heap.len(), 0);
        assert_eq!(count.0.load(Ordering::SeqCst), 1);

        // No deadline: pending for ever, never in the table.
        let mut s = timers.sleep(None);
        assert!(Pin::new(&mut s).poll(&mut cx).is_pending());
        assert!(timers.take().added.is_empty());
    }

    /// A waker that records whether its sleep's lock was held when it was
    /// woken or dropped.
    struct CellProbe {
        cell: Arc<OnceLock<Arc<Cell>>>,
        under_lock: Arc<AtomicBool>,
    }

    impl CellProbe {
        fn check(&self) {
            if let Some(c) = self.cell.get()
                && c.0.try_lock().is_err()
            {
                self.under_lock.store(true, Ordering::SeqCst);
            }
        }
    }

    impl Wake for CellProbe {
        fn wake(self: Arc<Self>) {
            self.check();
        }
    }

    impl Drop for CellProbe {
        fn drop(&mut self) {
            self.check();
        }
    }

    #[test]
    fn a_sleeps_wakers_are_woken_and_dropped_after_the_lock() {
        let c = manual();
        let timers = Timers::new(|| {});
        let mut heap = DeadlineHeap::default();
        let cell = Arc::new(OnceLock::new());
        let under_lock = Arc::new(AtomicBool::new(false));
        let probe = || {
            Waker::from(Arc::new(CellProbe {
                cell: cell.clone(),
                under_lock: under_lock.clone(),
            }))
        };
        let (first, second) = (probe(), probe());
        let mut sleep = timers.sleep(Some(c.now() + secs(1)));
        assert!(
            Pin::new(&mut sleep)
                .poll(&mut Context::from_waker(&first))
                .is_pending()
        );
        let _ = cell.set(sleep.reg.as_ref().unwrap().1.clone());
        drop(first);
        // The first is displaced, and dropped with its last reference.
        assert!(
            Pin::new(&mut sleep)
                .poll(&mut Context::from_waker(&second))
                .is_pending()
        );
        drop(second);
        // Firing wakes the second, its last reference, and drops it.
        lap(&mut heap, &timers, c.now());
        c.advance(secs(1));
        lap(&mut heap, &timers, c.now());
        assert!(!under_lock.load(Ordering::SeqCst));
        assert!(
            Pin::new(&mut sleep)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    }
}
