//! Shared values updated in place, as ib_async's objects are.
//!
//! ib_async hands out one object and changes it where it lies. Here that
//! object is a [`Live`] handle. While an IB holds it, every write is the
//! owner's; before that, and once no IB holds it, it is the program's.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex, Weak, mpsc};

use crate::error::{Error, Result};
use crate::event::{lock, on_owner};

/// A write to run on the owner: a control in an IB's queue.
pub(crate) type Control = Box<dyn FnOnce() + Send>;

/// An IB, as the holder of bound objects and the home of its events.
pub(crate) trait Holder: Send + Sync {
    /// Whether the IB's queue is still open. It is called under a cell's
    /// lock, so it takes only leaf locks.
    fn is_active(&self) -> bool;
    /// Admits `control` to the IB's queue, waiting while its controls are at
    /// their bound. Gives it back once the queue is closed.
    fn push_control(&self, control: Control) -> Result<(), Control>;
}

/// Where an object's writes go: its cell, which knows its holders.
pub(crate) trait Route: Send + Sync {
    /// Admits `f` through the first active holder, dropping each holder
    /// whose queue turns out closed. Gives `f` back when no holder is left,
    /// for the caller to run.
    fn post(&self, f: Control) -> Option<Control>;
    /// The IBs that hold the object.
    fn holders(&self) -> Vec<Weak<dyn Holder>>;
}

/// The storage behind [`Observed`], which seals it.
pub(crate) mod sealed {
    use std::sync::{Arc, Weak};

    use super::{Live, Observed, Route};
    use crate::event::Event;

    /// An object event's last value, rebuilt while its object lives.
    pub type Rebuild<T> = Arc<dyn Fn() -> Option<T> + Send + Sync>;

    /// The events a live type carries, made with each object.
    pub trait Storage {
        /// The type's events; `()` for a type with none.
        type Events: Send + Sync + 'static;
        /// Makes a new object's events.
        fn events(maker: &Maker) -> Self::Events;
    }

    /// Makes an object's events.
    pub struct Maker(pub(super) Weak<dyn Route>);

    impl Maker {
        /// An object event named `name`, as ib_async names it: run on the
        /// owner while the object is bound, and keeping its last value with
        /// the object held weakly.
        pub fn event<P: Weaken>(&self, name: &'static str) -> Event<P> {
            Event::object(name, self.0.clone())
        }
    }

    /// An object event's payload, which begins with its own object.
    pub trait Weaken: Clone + Send + Sync + 'static {
        /// The payload with its object held weakly.
        fn weaken(&self) -> Rebuild<Self>;
    }

    impl<X: Observed> Weaken for Live<X> {
        fn weaken(&self) -> Rebuild<Self> {
            let w = self.downgrade();
            Arc::new(move || w.upgrade())
        }
    }

    impl<X: Observed, A: Clone + Send + Sync + 'static> Weaken for (Live<X>, A) {
        fn weaken(&self) -> Rebuild<Self> {
            let (w, a) = (self.0.downgrade(), self.1.clone());
            Arc::new(move || Some((w.upgrade()?, a.clone())))
        }
    }

    impl<X, A, B> Weaken for (Live<X>, A, B)
    where
        X: Observed,
        A: Clone + Send + Sync + 'static,
        B: Clone + Send + Sync + 'static,
    {
        fn weaken(&self) -> Rebuild<Self> {
            let (w, a, b) = (self.0.downgrade(), self.1.clone(), self.2.clone());
            Arc::new(move || Some((w.upgrade()?, a.clone(), b.clone())))
        }
    }
}

/// A type ib_async shares and changes in place, held as [`Live`]: `Ticker`,
/// `Trade`, `PnL`, `PnLSingle`, `BarDataList`, `RealTimeBarList`,
/// `ScanDataList`, `Order`, `CommissionReport` and `BarList`. Sealed: only
/// this crate's types implement it.
pub trait Observed: sealed::Storage + Clone + Send + Sync + 'static {}

struct CellValue<T> {
    value: Arc<T>,
    /// Bumped by every write.
    revision: u64,
    /// Each IB that holds the object.
    holders: Vec<Weak<dyn Holder>>,
}

struct Cell<T: Observed> {
    value: Mutex<CellValue<T>>,
    events: T::Events,
}

/// The first holder whose queue is open. The handles taken of closed ones go
/// to `spent`, which the caller drops after the cell's lock.
fn first_active(
    holders: &[Weak<dyn Holder>],
    spent: &mut Vec<Arc<dyn Holder>>,
) -> Option<Arc<dyn Holder>> {
    for w in holders {
        if let Some(h) = w.upgrade() {
            if h.is_active() {
                return Some(h);
            }
            spent.push(h);
        }
    }
    None
}

impl<T: Observed> Route for Cell<T> {
    fn post(&self, mut f: Control) -> Option<Control> {
        loop {
            let mut spent = Vec::new();
            let holder = {
                let mut cv = lock(&self.value);
                // An IB that is gone leaves; its allocation goes with it.
                cv.holders.retain(|w| w.strong_count() > 0);
                first_active(&cv.holders, &mut spent)
            };
            drop(spent);
            let Some(h) = holder else {
                return Some(f);
            };
            match h.push_control(f) {
                Ok(()) => return None,
                Err(back) => {
                    f = back;
                    let gone = Arc::as_ptr(&h);
                    lock(&self.value)
                        .holders
                        .retain(|w| !std::ptr::addr_eq(w.as_ptr(), gone));
                }
            }
        }
    }

    fn holders(&self) -> Vec<Weak<dyn Holder>> {
        lock(&self.value).holders.clone()
    }
}

/// A shared handle to a value ib_async updates in place. Cloning gives
/// another handle to the same object.
///
/// Reads are snapshots: [`read`](Live::read) gives the value as last
/// written, never a guard. A write while a reader still holds a snapshot
/// copies the value once.
pub struct Live<T: Observed>(Arc<Cell<T>>);

impl<T: Observed> Clone for Live<T> {
    fn clone(&self) -> Self {
        Live(self.0.clone())
    }
}

impl<T: Observed> Live<T> {
    /// A new object with its events, the program's until an IB holds it: the
    /// ib_async constructor, such as `Trade()` or `BarDataList()`.
    #[must_use]
    pub fn new(value: T) -> Self {
        Live(Arc::new_cyclic(|cell: &Weak<Cell<T>>| {
            let route: Weak<dyn Route> = cell.clone();
            Cell {
                value: Mutex::new(CellValue {
                    value: Arc::new(value),
                    revision: 0,
                    holders: Vec::new(),
                }),
                events: T::events(&sealed::Maker(route)),
            }
        }))
    }

    /// The value as last written: an immutable snapshot.
    #[must_use]
    pub fn read(&self) -> Arc<T> {
        lock(&self.0.value).value.clone()
    }

    /// Changes the value with `f`, which runs once on a copy with no lock
    /// held; the copy is written only if nothing else wrote the object
    /// meanwhile, else this is `Err(Value)`.
    ///
    /// While an IB holds the object, the write is the owner's: inline on the
    /// owner thread, otherwise a control in a holding IB's queue that this
    /// waits for. Otherwise it runs on the caller, and is written only if no
    /// IB took the object meanwhile. A panic in `f` reaches the caller.
    pub fn edit(&self, f: impl FnOnce(&mut T) + Send + 'static) -> Result<()> {
        if on_owner() {
            return self.edit_here(f, false);
        }
        let slot = Arc::new(Mutex::new(Some(f)));
        let (tx, rx) = mpsc::sync_channel(1);
        let (live, pending) = (self.clone(), slot.clone());
        let control: Control = Box::new(move || {
            let taken = lock(&pending).take();
            if let Some(f) = taken {
                let _ = tx.send(catch_unwind(AssertUnwindSafe(|| live.edit_here(f, false))));
            }
        });
        match self.0.post(control) {
            None => match rx.recv() {
                Ok(Ok(done)) => done,
                Ok(Err(panic)) => resume_unwind(panic),
                Err(_) => Err(Error::NotConnected),
            },
            Some(back) => {
                drop(back);
                let taken = lock(&slot).take();
                match taken {
                    // No holder took it: the object was the program's, and
                    // the copy is written only if it still is.
                    Some(f) => self.edit_here(f, true),
                    None => Err(Error::NotConnected),
                }
            }
        }
    }

    /// A handle that does not keep the object alive.
    #[must_use]
    pub fn downgrade(&self) -> WeakLive<T> {
        WeakLive(Arc::downgrade(&self.0))
    }

    /// Whether `a` and `b` are the same object: ib_async's `is`.
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.0, &b.0)
    }

    /// Runs `f` on a copy with no lock held and writes the copy if the
    /// revision is unchanged and, when the object was the program's, still
    /// no IB holds it. `unbound` is the caller's finding that no IB took the
    /// edit, so the copy is the program's however the object stands now;
    /// otherwise this runs on the owner, and the object was the program's if
    /// it had no active holder here.
    fn edit_here(&self, f: impl FnOnce(&mut T), unbound: bool) -> Result<()> {
        let mut spent = Vec::new();
        let (start, revision, unbound) = {
            let cv = lock(&self.0.value);
            let active = first_active(&cv.holders, &mut spent);
            let unbound = unbound || active.is_none();
            spent.extend(active);
            (cv.value.clone(), cv.revision, unbound)
        };
        drop(spent);
        let mut copy = T::clone(&start);
        drop(start);
        f(&mut copy);
        let mut spent = Vec::new();
        let mut displaced = None;
        let written = {
            let mut cv = lock(&self.0.value);
            let active = first_active(&cv.holders, &mut spent);
            let ok = cv.revision == revision && !(unbound && active.is_some());
            spent.extend(active);
            if ok {
                displaced = Some(std::mem::replace(&mut cv.value, Arc::new(copy)));
                cv.revision += 1;
            }
            ok
        };
        drop(displaced);
        drop(spent);
        if written {
            Ok(())
        } else {
            Err(Error::Value("the object changed while the edit ran".into()))
        }
    }
}

impl<T: Observed> Live<T> {
    /// One read-modify-write under the cell's lock, for the owner's writes.
    /// `f` is this crate's code, never the program's.
    pub(crate) fn update<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut cv = lock(&self.0.value);
        cv.revision += 1;
        f(Arc::make_mut(&mut cv.value))
    }

    /// Adds `holder` to the IBs that hold the object, under the cell's lock.
    pub(crate) fn bind(&self, holder: Weak<dyn Holder>) {
        let mut cv = lock(&self.0.value);
        cv.holders.retain(|w| w.strong_count() > 0);
        if !cv.holders.iter().any(|w| w.ptr_eq(&holder)) {
            cv.holders.push(holder);
        }
    }

    /// The object's events.
    pub(crate) fn events(&self) -> &T::Events {
        &self.0.events
    }

    /// Admits `f` to the owner through the object's first active holder, or
    /// gives it back when no IB holds the object, for the caller to run.
    pub(crate) fn post(&self, f: Control) -> Option<Control> {
        self.0.post(f)
    }

    /// The object's address, for hashing by identity as ib_async's `id`.
    pub(crate) fn addr(&self) -> usize {
        Arc::as_ptr(&self.0).addr()
    }
}

impl<T: Observed + fmt::Debug> fmt::Debug for Live<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.read(), f)
    }
}

/// A handle to a [`Live`] object that does not keep it alive: what a
/// handler captures to refer to its own object.
pub struct WeakLive<T: Observed>(Weak<Cell<T>>);

impl<T: Observed> Clone for WeakLive<T> {
    fn clone(&self) -> Self {
        WeakLive(self.0.clone())
    }
}

impl<T: Observed> WeakLive<T> {
    /// The object, while any [`Live`] handle to it remains.
    #[must_use]
    pub fn upgrade(&self) -> Option<Live<T>> {
        self.0.upgrade().map(Live)
    }
}

impl<T: Observed> fmt::Debug for WeakLive<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("(WeakLive)")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
    use std::thread;

    use super::*;
    use crate::event::{Event, set_on_owner};

    /// An IB's queue, run by hand as the owner.
    #[derive(Default)]
    pub(crate) struct FakeIb {
        queue: Mutex<Vec<Control>>,
        /// The queue is closed: not active, and pushes come back.
        pub(crate) closed: AtomicBool,
        /// Pushes come back while it still reads as active, as when the
        /// queue closes between the check and the push.
        pub(crate) closing: AtomicBool,
    }

    impl Holder for FakeIb {
        fn is_active(&self) -> bool {
            !self.closed.load(SeqCst)
        }

        fn push_control(&self, control: Control) -> Result<(), Control> {
            if self.closed.load(SeqCst) || self.closing.load(SeqCst) {
                return Err(control);
            }
            lock(&self.queue).push(control);
            Ok(())
        }
    }

    impl FakeIb {
        pub(crate) fn new() -> Arc<Self> {
            Arc::default()
        }

        pub(crate) fn holder(self: &Arc<Self>) -> Weak<dyn Holder> {
            let w: Weak<FakeIb> = Arc::downgrade(self);
            w
        }

        pub(crate) fn queued(&self) -> usize {
            lock(&self.queue).len()
        }

        /// Runs the queued controls on this thread as the owner.
        pub(crate) fn run(&self) -> usize {
            let controls = std::mem::take(&mut *lock(&self.queue));
            let n = controls.len();
            set_on_owner(true);
            for c in controls {
                c();
            }
            set_on_owner(false);
            n
        }

        /// Runs the owner until `job` ends, and gives its result.
        pub(crate) fn serve<R>(&self, job: thread::JoinHandle<R>) -> thread::Result<R> {
            while !job.is_finished() {
                self.run();
                thread::yield_now();
            }
            job.join()
        }
    }

    /// A live type with object events, for these tests.
    #[derive(Clone, Debug, Default, PartialEq)]
    pub(crate) struct Thing {
        pub(crate) n: i32,
    }

    pub(crate) struct ThingEvents {
        pub(crate) status_event: Event<Live<Thing>>,
        pub(crate) update_event: Event<(Live<Thing>, bool)>,
        pub(crate) fill_event: Event<(Live<Thing>, i32, String)>,
    }

    impl sealed::Storage for Thing {
        type Events = ThingEvents;
        fn events(m: &sealed::Maker) -> ThingEvents {
            ThingEvents {
                status_event: m.event("statusEvent"),
                update_event: m.event("updateEvent"),
                fill_event: m.event("fillEvent"),
            }
        }
    }

    impl Observed for Thing {}

    fn revision(t: &Live<Thing>) -> u64 {
        lock(&t.0.value).revision
    }

    fn holders(t: &Live<Thing>) -> usize {
        lock(&t.0.value).holders.len()
    }

    #[test]
    fn an_unbound_edit_runs_on_the_caller_and_leaves_snapshots_alone() {
        let t = Live::new(Thing { n: 1 });
        let before = t.read();
        let here = thread::current().id();
        let ran_on = Arc::new(Mutex::new(None));
        let r = ran_on.clone();
        t.edit(move |v| {
            *lock(&r) = Some(thread::current().id());
            v.n = 2;
        })
        .unwrap();
        assert_eq!(*lock(&ran_on), Some(here));
        assert_eq!(t.read().n, 2);
        assert_eq!(before.n, 1);
        assert_eq!(revision(&t), 1);
    }

    #[test]
    fn an_unbound_edit_that_loses_to_a_bind_or_a_write_writes_nothing() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        let (t2, h) = (t.clone(), ib.holder());
        let r = t.edit(move |v| {
            v.n = 5;
            t2.bind(h);
        });
        assert!(matches!(r, Err(Error::Value(_))));
        assert_eq!(t.read().n, 0);

        let u = Live::new(Thing::default());
        let runs = Arc::new(AtomicUsize::new(0));
        let (u2, runs2) = (u.clone(), runs.clone());
        let r = u.edit(move |v| {
            runs2.fetch_add(1, SeqCst);
            v.n = 5;
            u2.update(|w| w.n = 7);
        });
        assert!(matches!(r, Err(Error::Value(m)) if m == "the object changed while the edit ran"));
        assert_eq!(u.read().n, 7);
        assert_eq!(runs.load(SeqCst), 1);
    }

    #[test]
    fn a_bound_edit_runs_once_on_the_owner() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        let runs = Arc::new(AtomicUsize::new(0));
        let on = Arc::new(AtomicBool::new(false));
        let (t2, runs2, on2) = (t.clone(), runs.clone(), on.clone());
        let job = thread::spawn(move || {
            t2.edit(move |v| {
                runs2.fetch_add(1, SeqCst);
                on2.store(on_owner(), SeqCst);
                v.n = 3;
            })
        });
        assert!(ib.serve(job).unwrap().is_ok());
        assert_eq!(runs.load(SeqCst), 1);
        assert!(on.load(SeqCst));
        assert_eq!(t.read().n, 3);
    }

    #[test]
    fn a_bound_edit_whose_closure_writes_the_object_keeps_that_write() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        let runs = Arc::new(AtomicUsize::new(0));
        let (t2, runs2) = (t.clone(), runs.clone());
        set_on_owner(true);
        let r = t.edit(move |v| {
            runs2.fetch_add(1, SeqCst);
            v.n = 1;
            t2.update(|w| w.n = 9);
        });
        set_on_owner(false);
        assert!(matches!(r, Err(Error::Value(_))));
        assert_eq!(t.read().n, 9);
        assert_eq!(runs.load(SeqCst), 1);
        assert_eq!(ib.queued(), 0);
    }

    #[test]
    fn an_edit_runs_on_the_caller_once_no_holder_is_active() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        ib.closed.store(true, SeqCst);
        t.edit(|v| v.n = 4).unwrap();
        assert_eq!(t.read().n, 4);
        assert_eq!(ib.queued(), 0);

        let gone = FakeIb::new();
        t.bind(gone.holder());
        drop(gone);
        t.edit(|v| v.n = 5).unwrap();
        assert_eq!(t.read().n, 5);
    }

    #[test]
    fn an_edit_skips_a_closed_holder_for_an_open_one() {
        let t = Live::new(Thing::default());
        let (a, b) = (FakeIb::new(), FakeIb::new());
        t.bind(a.holder());
        t.bind(b.holder());
        // `a` is kept alive, as by an `IBHandle`, but its queue is closed.
        a.closed.store(true, SeqCst);
        let t2 = t.clone();
        let job = thread::spawn(move || t2.edit(|v| v.n = 6));
        assert!(b.serve(job).unwrap().is_ok());
        assert_eq!(t.read().n, 6);
        assert_eq!(a.queued(), 0);
    }

    #[test]
    fn a_push_that_comes_back_drops_that_holder_and_tries_the_next() {
        let t = Live::new(Thing::default());
        let (a, b) = (FakeIb::new(), FakeIb::new());
        t.bind(a.holder());
        t.bind(b.holder());
        a.closing.store(true, SeqCst);
        let t2 = t.clone();
        let job = thread::spawn(move || t2.edit(|v| v.n = 8));
        assert!(b.serve(job).unwrap().is_ok());
        assert_eq!(t.read().n, 8);
        assert_eq!(holders(&t), 1);
    }

    #[test]
    fn an_edit_dropped_unrun_is_not_connected() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        let t2 = t.clone();
        let job = thread::spawn(move || t2.edit(|v| v.n = 1));
        while ib.queued() == 0 {
            thread::yield_now();
        }
        drop(std::mem::take(&mut *lock(&ib.queue)));
        assert!(matches!(job.join().unwrap(), Err(Error::NotConnected)));
        assert_eq!(t.read().n, 0);
    }

    #[test]
    fn a_panic_in_a_bound_edit_reaches_the_caller_not_the_owner() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        let t2 = t.clone();
        let job = thread::spawn(move || t2.edit(|_| panic!("edit failed")));
        assert!(ib.serve(job).is_err());
        assert_eq!(revision(&t), 0);
    }

    #[test]
    fn post_goes_through_a_holder_or_comes_back() {
        let t = Live::new(Thing::default());
        assert!(t.post(Box::new(|| {})).is_some());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        assert!(t.post(Box::new(|| {})).is_none());
        assert_eq!(ib.queued(), 1);
    }

    #[test]
    fn addr_is_the_objects_identity() {
        let (a, b) = (Live::new(Thing::default()), Live::new(Thing::default()));
        assert_eq!(a.addr(), a.clone().addr());
        assert_ne!(a.addr(), b.addr());
    }

    #[test]
    fn handles_cross_threads() {
        fn send_sync<T: Send + Sync>() {}
        fn send<T: Send>() {}
        send_sync::<Live<Thing>>();
        send_sync::<WeakLive<Thing>>();
        send_sync::<Event<Live<Thing>>>();
        send::<crate::event::Subscription<Live<Thing>>>();
    }

    #[test]
    fn binding_twice_holds_once() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        t.bind(ib.holder());
        assert_eq!(holders(&t), 1);
    }

    #[test]
    fn update_bumps_the_revision_and_copies_only_for_a_reader() {
        let t = Live::new(Thing::default());
        let first = Arc::as_ptr(&t.read());
        t.update(|v| v.n = 1);
        assert_eq!(Arc::as_ptr(&t.read()), first);
        let held = t.read();
        t.update(|v| v.n = 2);
        assert_eq!(held.n, 1);
        assert_eq!(t.read().n, 2);
        assert_eq!(revision(&t), 2);
        assert_eq!(format!("{t:?}"), "Thing { n: 2 }");
    }

    #[test]
    fn new_makes_the_objects_events_by_their_names() {
        let t = Live::new(Thing::default());
        let e = t.events();
        assert_eq!(e.status_event.name(), "statusEvent");
        assert_eq!(e.update_event.name(), "updateEvent");
        assert_eq!(e.fill_event.name(), "fillEvent");
        assert!(e.update_event.error_event().is_some());
        assert!(e.update_event.value().is_none());
    }

    #[test]
    fn an_object_events_value_is_gone_with_its_object() {
        let t = Live::new(Thing::default());
        let e = t.events();
        let (status, update, fill) = (
            e.status_event.clone(),
            e.update_event.clone(),
            e.fill_event.clone(),
        );
        let seen = Arc::new(AtomicUsize::new(0));
        let (seen2, weak) = (seen.clone(), t.downgrade());
        update.connect(move |(obj, _)| {
            if weak.upgrade().is_some_and(|w| Live::ptr_eq(obj, &w)) {
                seen2.fetch_add(1, SeqCst);
            }
        });
        status.emit(&t);
        update.emit(&(t.clone(), true));
        fill.emit(&(t.clone(), 3, "x".to_owned()));
        assert_eq!(seen.load(SeqCst), 1);
        assert!(Live::ptr_eq(&status.value().unwrap(), &t));
        let (obj, flag) = update.value().unwrap();
        assert!(Live::ptr_eq(&obj, &t) && flag);
        drop(obj);
        let (obj, n, s) = fill.value().unwrap();
        assert!(Live::ptr_eq(&obj, &t) && n == 3 && s == "x");
        drop(obj);

        let weak = t.downgrade();
        drop(t);
        assert!(weak.upgrade().is_none());
        assert!(status.value().is_none());
        assert!(update.value().is_none());
        assert!(fill.value().is_none());
    }

    #[test]
    fn a_bound_objects_events_run_on_the_owner() {
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        t.bind(ib.holder());
        let ev = t.events().update_event.clone();
        let on = Arc::new(Mutex::new(Vec::new()));
        let on2 = on.clone();
        ev.connect(move |_| lock(&on2).push(on_owner()));
        let done = Arc::new(AtomicBool::new(false));
        let done2 = done.clone();
        ev.done_event()
            .unwrap()
            .connect(move |_| done2.store(on_owner(), SeqCst));

        ev.emit(&(t.clone(), false));
        ev.set_done();
        assert!(lock(&on).is_empty());
        assert!(!ev.done());
        assert_eq!(ib.run(), 2);
        assert_eq!(*lock(&on), vec![true]);
        assert!(done.load(SeqCst));

        ib.closed.store(true, SeqCst);
        ev.emit(&(t.clone(), false));
        assert_eq!(*lock(&on), vec![true, false]);
    }

    /// A holder that is never active, whose last strong handle is the one
    /// an edit or a post takes of it: `on_release` runs when that is dropped.
    struct OnRelease {
        me: Mutex<Option<Arc<OnRelease>>>,
        on_release: Box<dyn Fn() + Send + Sync>,
    }

    impl OnRelease {
        fn bind_to(t: &Live<Thing>, on_release: impl Fn() + Send + Sync + 'static) {
            let h = Arc::new(OnRelease {
                me: Mutex::new(None),
                on_release: Box::new(on_release),
            });
            *lock(&h.me) = Some(h.clone());
            let w: Weak<OnRelease> = Arc::downgrade(&h);
            t.bind(w);
        }
    }

    impl Holder for OnRelease {
        fn is_active(&self) -> bool {
            let me = lock(&self.me).take();
            drop(me);
            false
        }

        fn push_control(&self, control: Control) -> Result<(), Control> {
            Err(control)
        }
    }

    impl Drop for OnRelease {
        fn drop(&mut self) {
            (self.on_release)();
        }
    }

    #[test]
    fn an_edit_left_to_the_caller_is_not_written_once_an_ib_takes_the_object() {
        // The bind lands after the post found no active holder and before
        // the edit's first lock.
        let t = Live::new(Thing::default());
        let ib = FakeIb::new();
        let (t2, to) = (t.clone(), ib.holder());
        OnRelease::bind_to(&t, move || t2.bind(to.clone()));
        let r = t.edit(|v| v.n = 1);
        assert!(matches!(r, Err(Error::Value(_))));
        assert_eq!(t.read().n, 0);
        assert_eq!(ib.queued(), 0);
    }

    #[test]
    fn an_edit_drops_the_holders_it_took_after_the_lock() {
        let t = Live::new(Thing::default());
        let under_lock = Arc::new(AtomicBool::new(false));
        let (t2, u) = (t.clone(), under_lock.clone());
        OnRelease::bind_to(&t, move || {
            u.store(t2.0.value.try_lock().is_err(), SeqCst);
        });
        set_on_owner(true);
        let r = t.edit(|v| v.n = 1);
        set_on_owner(false);
        assert!(r.is_ok());
        assert!(!under_lock.load(SeqCst));
        assert_eq!(t.read().n, 1);
    }
}
