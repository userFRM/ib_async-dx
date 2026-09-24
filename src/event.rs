//! ib_async's events: eventkit's `Event`, its handlers and subscriptions.
//!
//! An event keeps one list of slots in connection order. A handler and a
//! subscription are both slots, as eventkit's `aiter` is a connected
//! listener. `emit` calls a snapshot of that list, so a slot connected or
//! disconnected during an emission takes effect from the next one.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "the owner and the IB's events use these")
)]

use std::any::Any;
use std::borrow::Cow;
use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use crate::live::sealed::{Rebuild, Weaken};
use crate::live::{Control, Holder, Route};

/// Takes a crate lock, recovering it from poison.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

thread_local! {
    static ON_OWNER: Cell<bool> = const { Cell::new(false) };
}

/// Whether this thread is the owner, the one thread that runs every IB.
pub(crate) fn on_owner() -> bool {
    ON_OWNER.with(Cell::get)
}

/// Marks this thread as the owner, or not.
pub(crate) fn set_on_owner(on: bool) {
    ON_OWNER.with(|c| c.set(on));
}

/// Names a connected handler or subscription, for [`Event::disconnect`] and
/// [`Event::contains`]: what eventkit's `disconnect` and `in` find by the
/// callable itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HandlerId(u64);

fn next_id() -> HandlerId {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    HandlerId(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// A handler's panic, as an event's `error_event` carries it: eventkit's
/// `(source, exception)`. It displays as the panic's message, as Python's
/// `str()` of the exception.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandlerError {
    /// The name of the event whose handler panicked.
    pub event: String,
    /// The panic's message.
    pub message: String,
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HandlerError {}

/// Why a [`Subscription`] gave no value.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RecvError {
    /// Nothing is queued yet (`try_recv`).
    Empty,
    /// The time passed with nothing queued (`recv_timeout`).
    Timeout,
    /// The event is done and every value queued before was received: the
    /// end of eventkit's `aiter`.
    Done,
    /// A blocking receive on the owner thread, which would wait for itself.
    OwnerThread,
    /// A handler of the event panicked. eventkit's `aiter` raises the
    /// `error_event` value in its consumer, then stops; the subscription
    /// ends after this.
    Handler(HandlerError),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Empty => f.write_str("no value is queued"),
            RecvError::Timeout => f.write_str("timed out"),
            RecvError::Done => f.write_str("the event is done"),
            RecvError::OwnerThread => f.write_str("This event loop is already running"),
            RecvError::Handler(e) => fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for RecvError {}

type Handler<T> = Arc<dyn Fn(&T) + Send + Sync>;

/// Where an event's emissions run.
#[derive(Clone)]
enum Home {
    /// The program's event: on the thread that emits.
    Program,
    /// An IB's event: on the owner, reached from other threads through the
    /// IB's queue.
    Ib(Weak<dyn Holder>),
    /// An object's event: on the owner while the object is bound, reached
    /// through its holders; on the caller once it is the program's again.
    Object(Weak<dyn Route>),
}

enum Last<T> {
    None,
    /// Shared, so `value()` clones it after the lock.
    Value(Arc<T>),
    /// An object event's last value, its own object held weakly.
    Rebuild(Rebuild<T>),
}

struct State<T> {
    slots: Vec<(HandlerId, Handler<T>)>,
    last: Last<T>,
    done: bool,
}

struct EventInner<T> {
    name: Cow<'static, str>,
    state: Mutex<State<T>>,
    error_event: Option<Event<HandlerError>>,
    done_event: Option<Event<()>>,
    home: Home,
    weaken: Option<fn(&T) -> Rebuild<T>>,
}

/// An event that handlers connect to and subscriptions receive from:
/// eventkit's `Event`.
///
/// Cloning gives another handle to the same event. On an IB's event, or on
/// an object event of an object an IB holds, `emit` and `set_done` run on
/// the owner: inline there, and from any other thread as a control that
/// returns once admitted, as eventkit's `emit_threadsafe` schedules the emit
/// on the loop. Any other event runs its slots on the thread that emits.
#[derive(Clone)]
pub struct Event<T>(Arc<EventInner<T>>);

impl<T: Clone + Send + Sync + 'static> Event<T> {
    /// An event the program owns: eventkit's `Event(name)`. An empty name
    /// is `"Event"`, as eventkit names an unnamed event after its class.
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        let name = name.into();
        let name = if name.is_empty() {
            "Event".into()
        } else {
            name
        };
        Self::build(name, true, Home::Program, None)
    }

    /// A helper stage's output event, run where `source`'s emissions run:
    /// on the owner while an IB holds the ticker a chain hangs on.
    pub(crate) fn follows<S>(name: &'static str, source: &Event<S>) -> Self {
        Self::build(name.into(), true, source.0.home.clone(), None)
    }

    /// An IB's event, run on the owner and reached from other threads
    /// through `holder`'s queue.
    pub(crate) fn with_holder(
        name: impl Into<Cow<'static, str>>,
        holder: Weak<dyn Holder>,
    ) -> Self {
        Self::build(name.into(), true, Home::Ib(holder), None)
    }

    /// An object event: routed by its object's holders, and keeping its last
    /// value with its own object held weakly.
    pub(crate) fn object(name: &'static str, route: Weak<dyn Route>) -> Self
    where
        T: Weaken,
    {
        Self::build(name.into(), true, Home::Object(route), Some(T::weaken))
    }

    fn build(
        name: Cow<'static, str>,
        subs: bool,
        home: Home,
        weaken: Option<fn(&T) -> Rebuild<T>>,
    ) -> Self {
        Event(Arc::new(EventInner {
            name,
            state: Mutex::new(State {
                slots: Vec::new(),
                last: Last::None,
                done: false,
            }),
            // Sub-events run where their event runs.
            error_event: subs.then(|| Event::build("error".into(), false, home.clone(), None)),
            done_event: subs.then(|| Event::build("done".into(), false, home.clone(), None)),
            home,
            weaken,
        }))
    }

    /// The event's name: eventkit's `name()`.
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// Connects `handler`, called with each value emitted: eventkit's
    /// `connect` and `+=`. A handler that must not keep its object alive
    /// captures a `Weak` or a [`WeakLive`](crate::WeakLive), since handlers
    /// are held strongly.
    pub fn connect(&self, handler: impl Fn(&T) + Send + Sync + 'static) -> HandlerId {
        let id = next_id();
        lock(&self.0.state).slots.push((id, Arc::new(handler)));
        id
    }

    /// Connects an async handler: eventkit's `connect` of a coroutine
    /// function. `handler` is called inline like any handler, and the future
    /// it returns becomes a task the owner runs, as eventkit hands it to
    /// `asyncio.ensure_future`. A panic while polling the task is logged at
    /// ERROR and drops the task.
    pub fn connect_async<F>(&self, handler: impl Fn(&T) -> F + Send + Sync + 'static) -> HandlerId
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.connect(move |v| spawn(handler(v)))
    }

    /// A receiver of every value emitted from now on: eventkit's `aiter`,
    /// `__aiter__` and `__await__`. On a done event it is already ended.
    pub fn subscribe(&self) -> Subscription<T> {
        let queue = Arc::new(Queue::new());
        let on_error = self.0.error_event.as_ref().map(|e| {
            let q = queue.clone();
            e.connect(move |err: &HandlerError| q.push(Err(End::Error(err.clone()))))
        });
        let on_done = self.0.done_event.as_ref().map(|e| {
            let q = queue.clone();
            e.connect(move |_: &()| q.push(Err(End::Done)))
        });
        let q = queue.clone();
        let on_value = {
            let mut st = lock(&self.0.state);
            (!st.done).then(|| {
                let id = next_id();
                st.slots
                    .push((id, Arc::new(move |v: &T| q.push(Ok(v.clone())))));
                id
            })
        };
        let mut sub = Subscription {
            event: self.clone(),
            queue,
            ids: [on_value, on_error, on_done],
            ended: false,
        };
        if on_value.is_none() {
            sub.queue.push(Err(End::Done));
            sub.finish();
        }
        sub
    }

    /// Emits `value` to every slot in connection order: eventkit's `emit`,
    /// `__call__` and `emit_threadsafe`. A slot that panics is isolated, its
    /// error emitted on `error_event`, and the next slot runs.
    pub fn emit(&self, value: &T) {
        if self.runs_here() {
            self.emit_here(value);
        } else {
            let (ev, v) = (self.clone(), value.clone());
            self.post(Box::new(move || ev.emit_here(&v)));
        }
    }

    /// Marks the event done and emits `done_event`: eventkit's `set_done`.
    /// Handlers stay connected, and a later `emit` still reaches them;
    /// subscriptions end after their queued values. Routed like `emit`.
    pub fn set_done(&self) {
        if self.runs_here() {
            self.set_done_here();
        } else {
            let ev = self.clone();
            self.post(Box::new(move || ev.set_done_here()));
        }
    }

    /// Whether `set_done` has run: eventkit's `done()`.
    pub fn done(&self) -> bool {
        lock(&self.0.state).done
    }

    /// The last value emitted: eventkit's `value()`. An object event gives
    /// `None` once its object is gone.
    pub fn value(&self) -> Option<T> {
        let last = match &lock(&self.0.state).last {
            Last::None => return None,
            Last::Value(v) => Ok(v.clone()),
            Last::Rebuild(r) => Err(r.clone()),
        };
        match last {
            Ok(v) => Some(T::clone(&v)),
            Err(rebuild) => rebuild(),
        }
    }

    /// The number of connected handlers and subscriptions: eventkit's
    /// `len(event)`.
    pub fn len(&self) -> usize {
        lock(&self.0.state).slots.len()
    }

    /// Whether nothing is connected. eventkit's `bool(event)` is always true.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `id` is connected: eventkit's `in`.
    pub fn contains(&self, id: HandlerId) -> bool {
        lock(&self.0.state).slots.iter().any(|s| s.0 == id)
    }

    /// Disconnects every handler and subscription: eventkit's `clear`. It
    /// runs no slot and takes effect from the next emission.
    pub fn clear(&self) {
        let removed = std::mem::take(&mut lock(&self.0.state).slots);
        drop(removed);
    }

    /// The event a handler's panic is emitted on: eventkit's `error_event`.
    /// `None` on an `error_event` or `done_event` itself, as in eventkit.
    pub fn error_event(&self) -> Option<&Event<HandlerError>> {
        self.0.error_event.as_ref()
    }

    /// The event `set_done` emits: eventkit's `done_event`. `None` on an
    /// `error_event` or `done_event` itself, as in eventkit.
    pub fn done_event(&self) -> Option<&Event<()>> {
        self.0.done_event.as_ref()
    }

    fn runs_here(&self) -> bool {
        matches!(self.0.home, Home::Program) || on_owner()
    }

    /// Admits `f` to the owner through the event's holder, or runs it here
    /// when no holder takes it.
    fn post(&self, f: Control) {
        let back = match &self.0.home {
            Home::Program => Some(f),
            Home::Ib(h) => match h.upgrade() {
                Some(h) => h.push_control(f).err(),
                None => Some(f),
            },
            Home::Object(r) => match r.upgrade() {
                Some(r) => r.post(f),
                None => Some(f),
            },
        };
        if let Some(f) = back {
            f();
        }
    }

    fn emit_here(&self, value: &T) {
        let last = match self.0.weaken {
            Some(weaken) => Last::Rebuild(weaken(value)),
            None => Last::Value(Arc::new(value.clone())),
        };
        let (slots, displaced) = {
            let mut st = lock(&self.0.state);
            (st.slots.clone(), std::mem::replace(&mut st.last, last))
        };
        drop(displaced);
        for (_, call) in &slots {
            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| call(value))) {
                self.handler_panicked(&*panic);
            }
        }
    }

    fn set_done_here(&self) {
        let was = std::mem::replace(&mut lock(&self.0.state).done, true);
        if !was && let Some(d) = &self.0.done_event {
            d.emit_here(&());
            d.set_done_here();
        }
    }

    fn handler_panicked(&self, panic: &(dyn Any + Send)) {
        let message = panic_message(panic);
        match &self.0.error_event {
            Some(e) => e.emit_here(&HandlerError {
                event: self.name().to_owned(),
                message,
            }),
            None => log::error!(
                target: "eventkit.event",
                "Value caused exception for event {}: {message}",
                self.name()
            ),
        }
    }
}

impl<T> Event<T> {
    /// A handle that does not keep the event alive: what a helper stage
    /// keeps of its source.
    pub(crate) fn downgrade(&self) -> WeakEvent<T> {
        WeakEvent(Arc::downgrade(&self.0))
    }

    /// The IBs that hold what this event belongs to: its IB, or the holders
    /// of its object; none for the program's own event.
    pub(crate) fn holders(&self) -> Vec<Weak<dyn Holder>> {
        match &self.0.home {
            Home::Program => Vec::new(),
            Home::Ib(h) => vec![h.clone()],
            Home::Object(r) => r.upgrade().map(|r| r.holders()).unwrap_or_default(),
        }
    }

    /// Disconnects the handler or subscription `id`: eventkit's `disconnect`
    /// and `-=`. Gives whether it was connected.
    pub fn disconnect(&self, id: HandlerId) -> bool {
        let removed = {
            let mut st = lock(&self.0.state);
            st.slots
                .iter()
                .position(|s| s.0 == id)
                .map(|i| st.slots.remove(i))
        };
        removed.is_some()
    }
}

impl<T> fmt::Debug for Event<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let slots = lock(&self.0.state).slots.len();
        f.debug_struct("Event")
            .field("name", &self.0.name)
            .field("slots", &slots)
            .finish()
    }
}

/// An event held weakly.
pub(crate) struct WeakEvent<T>(Weak<EventInner<T>>);

impl<T> WeakEvent<T> {
    /// The event, while a handle to it remains.
    pub(crate) fn upgrade(&self) -> Option<Event<T>> {
        self.0.upgrade().map(Event)
    }
}

/// A panic's message: its `&str` or `String`, else the std hook's text.
pub(crate) fn panic_message(panic: &(dyn Any + Send)) -> String {
    match panic.downcast_ref::<&str>() {
        Some(s) => (*s).to_owned(),
        None => match panic.downcast_ref::<String>() {
            Some(s) => s.clone(),
            None => "Box<dyn Any>".to_owned(),
        },
    }
}

enum End {
    Error(HandlerError),
    Done,
}

struct QueueState<T> {
    items: VecDeque<T>,
    end: Option<End>,
    waker: Option<Waker>,
}

impl<T> QueueState<T> {
    /// The next value, then the end: a handler's error once, then `Done`.
    fn next(&mut self) -> Option<Result<T, RecvError>> {
        if let Some(v) = self.items.pop_front() {
            return Some(Ok(v));
        }
        let end = self.end.take()?;
        self.end = Some(End::Done);
        Some(Err(match end {
            End::Error(e) => RecvError::Handler(e),
            End::Done => RecvError::Done,
        }))
    }
}

struct Queue<T> {
    state: Mutex<QueueState<T>>,
    ready: Condvar,
}

impl<T> Queue<T> {
    fn new() -> Self {
        Queue {
            state: Mutex::new(QueueState {
                items: VecDeque::new(),
                end: None,
                waker: None,
            }),
            ready: Condvar::new(),
        }
    }

    /// Queues a value or the end; nothing after the end. The waiter is
    /// notified and woken after the lock is released.
    fn push(&self, item: Result<T, End>) {
        let waker = {
            let mut st = lock(&self.state);
            if st.end.is_some() {
                return;
            }
            match item {
                Ok(v) => st.items.push_back(v),
                Err(end) => st.end = Some(end),
            }
            st.waker.take()
        };
        self.ready.notify_one();
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// A receiver of an event's emissions, in order, with no bound: what
/// awaiting or iterating an eventkit `Event` gives.
///
/// It owns its event and holds a slot on it; dropping it disconnects the
/// slot. When a handler of the event panics, it gives
/// [`RecvError::Handler`] once and then ends. When the event is done, it
/// ends after the values queued before. As an [`Iterator`] and a
/// [`Stream`](futures_core::Stream) it yields `Result` items: `Done` ends
/// it, and any other error is yielded once before it ends.
#[must_use]
pub struct Subscription<T> {
    event: Event<T>,
    queue: Arc<Queue<T>>,
    /// Its slots on the event, its `error_event` and its `done_event`.
    ids: [Option<HandlerId>; 3],
    /// The iterator or stream has yielded its end.
    ended: bool,
}

impl<T: Clone + Send + Sync + 'static> Subscription<T> {
    /// Waits for the next value. On the owner thread it fails at once with
    /// `OwnerThread`.
    pub fn recv(&mut self) -> Result<T, RecvError> {
        if on_owner() {
            return Err(RecvError::OwnerThread);
        }
        self.wait(None)
    }

    /// Waits at most `timeout` for the next value, failing with `Timeout`.
    /// On the owner thread it fails at once with `OwnerThread`.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvError> {
        if on_owner() {
            return Err(RecvError::OwnerThread);
        }
        self.wait(Instant::now().checked_add(timeout))
    }

    /// Takes the next value if one is queued, failing with `Empty` if not.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        let got = lock(&self.queue.state).next();
        self.settle(got.unwrap_or(Err(RecvError::Empty)))
    }

    /// Waits for the next value without blocking a thread, on any executor.
    pub async fn recv_async(&mut self) -> Result<T, RecvError> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn wait(&mut self, deadline: Option<Instant>) -> Result<T, RecvError> {
        let got = {
            let mut st = lock(&self.queue.state);
            loop {
                if let Some(got) = st.next() {
                    break got;
                }
                st = match deadline {
                    None => self
                        .queue
                        .ready
                        .wait(st)
                        .unwrap_or_else(PoisonError::into_inner),
                    Some(d) => {
                        let left = d.saturating_duration_since(Instant::now());
                        if left.is_zero() {
                            break Err(RecvError::Timeout);
                        }
                        self.queue
                            .ready
                            .wait_timeout(st, left)
                            .unwrap_or_else(PoisonError::into_inner)
                            .0
                    }
                };
            }
        };
        self.settle(got)
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        let waker = cx.waker().clone();
        let (got, spare) = {
            let mut st = lock(&self.queue.state);
            match st.next() {
                Some(got) => (Some(got), Some(waker)),
                None => (None, st.waker.replace(waker)),
            }
        };
        drop(spare);
        match got {
            Some(got) => Poll::Ready(self.settle(got)),
            None => Poll::Pending,
        }
    }

    /// Leaves the event once the end is received, as `aiter`'s `finally`
    /// disconnects.
    fn settle(&mut self, got: Result<T, RecvError>) -> Result<T, RecvError> {
        if matches!(got, Err(RecvError::Done | RecvError::Handler(_))) {
            self.finish();
        }
        got
    }

    fn item(&mut self, got: Result<T, RecvError>) -> Option<Result<T, RecvError>> {
        match got {
            Ok(v) => Some(Ok(v)),
            Err(RecvError::Done) => {
                self.ended = true;
                None
            }
            Err(e) => {
                self.ended = true;
                Some(Err(e))
            }
        }
    }

    /// The number of values queued and not yet received.
    pub fn len(&self) -> usize {
        lock(&self.queue.state).items.len()
    }

    /// Whether no value is queued.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Subscription<T> {
    fn finish(&mut self) {
        let [value, error, done] = std::mem::take(&mut self.ids);
        if let Some(id) = value {
            self.event.disconnect(id);
        }
        if let (Some(id), Some(e)) = (error, &self.event.0.error_event) {
            e.disconnect(id);
        }
        if let (Some(id), Some(e)) = (done, &self.event.0.done_event) {
            e.disconnect(id);
        }
    }
}

impl<T> fmt::Debug for Subscription<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("event", &self.event)
            .finish_non_exhaustive()
    }
}

impl<T> Drop for Subscription<T> {
    fn drop(&mut self) {
        self.finish();
    }
}

impl<T: Clone + Send + Sync + 'static> Iterator for Subscription<T> {
    type Item = Result<T, RecvError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        let got = self.recv();
        self.item(got)
    }
}

impl<T: Clone + Send + Sync + 'static> futures_core::Stream for Subscription<T> {
    type Item = Result<T, RecvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        this.poll_recv(cx).map(|got| this.item(got))
    }
}

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// An async handler's future, run by the owner: the task eventkit's
/// `ensure_future` makes.
pub(crate) struct Task {
    /// `None` once it has finished or panicked.
    future: Mutex<Option<BoxFuture>>,
    /// Its waker fired since its last poll.
    woken: AtomicBool,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
        wake_owner();
    }
}

impl Task {
    /// Polls the future once, with no lock held. A panic is caught and
    /// logged at ERROR, and the task is dropped, as asyncio only logs a
    /// task's unretrieved exception.
    pub(crate) fn poll(self: &Arc<Self>) {
        let Some(mut future) = lock(&self.future).take() else {
            return;
        };
        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut cx))) {
            Ok(Poll::Pending) => *lock(&self.future) = Some(future),
            Ok(Poll::Ready(())) => {}
            Err(panic) => log::error!(
                target: "asyncio",
                "Task exception was never retrieved: {}",
                panic_message(&*panic)
            ),
        }
    }
}

/// The owner's tasks, each polled at most once per lap, in a lap its waker
/// fired before.
#[derive(Default)]
pub(crate) struct Tasks(Vec<Arc<Task>>);

impl Tasks {
    /// Takes in the tasks started since the last lap.
    pub(crate) fn take_spawned(&mut self) {
        self.0.append(&mut std::mem::take(&mut *lock(&SPAWNED)));
    }

    /// The tasks woken before this call, each to be polled once; a wake
    /// after it is the next lap's. Finished tasks leave the list.
    pub(crate) fn take_woken(&mut self) -> Vec<Arc<Task>> {
        self.0.retain(|t| lock(&t.future).is_some());
        self.0
            .iter()
            .filter(|t| t.woken.swap(false, Ordering::SeqCst))
            .cloned()
            .collect()
    }

    /// How many tasks have not finished.
    pub(crate) fn len(&self) -> usize {
        self.0.iter().filter(|t| lock(&t.future).is_some()).count()
    }
}

/// Tasks started and not yet taken by the owner.
static SPAWNED: Mutex<Vec<Arc<Task>>> = Mutex::new(Vec::new());

/// What wakes the owner, set once it runs.
static OWNER_WAKE: OnceLock<fn()> = OnceLock::new();

/// Sets what a task's start or waker calls to wake the owner.
pub(crate) fn set_owner_wake(wake: fn()) {
    let _ = OWNER_WAKE.set(wake);
}

fn wake_owner() {
    if let Some(wake) = OWNER_WAKE.get() {
        wake();
    }
}

fn spawn(future: impl Future<Output = ()> + Send + 'static) {
    let task = Arc::new(Task {
        future: Mutex::new(Some(Box::pin(future))),
        woken: AtomicBool::new(true),
    });
    lock(&SPAWNED).push(task);
    wake_owner();
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::thread::{self, Thread};

    use futures_core::Stream;

    use super::*;
    use crate::live::tests::FakeIb;
    use crate::tests::{capture_logs, errors_here};

    /// Polls `f` once with a waker that does nothing.
    fn now<F: Future>(f: F) -> Option<F::Output> {
        match pin!(f).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(v) => Some(v),
            Poll::Pending => None,
        }
    }

    /// Runs `f` to its end on this thread.
    fn block_on<F: Future>(f: F) -> F::Output {
        struct Unpark(Thread);
        impl Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(Unpark(thread::current())));
        let mut f = pin!(f);
        loop {
            if let Poll::Ready(v) = f.as_mut().poll(&mut Context::from_waker(&waker)) {
                return v;
            }
            thread::park();
        }
    }

    fn boom(event: &str) -> RecvError {
        RecvError::Handler(HandlerError {
            event: event.to_owned(),
            message: "boom".to_owned(),
        })
    }

    #[test]
    fn a_subscription_after_a_nested_emitter_gets_the_inner_value_first() {
        let ev = Event::<i32>::new("e");
        let inner = ev.clone();
        ev.connect(move |v| {
            if *v == 1 {
                inner.emit(&2);
            }
        });
        let mut sub = ev.subscribe();
        ev.emit(&1);
        assert_eq!(sub.try_recv(), Ok(2));
        assert_eq!(sub.try_recv(), Ok(1));
        assert_eq!(sub.try_recv(), Err(RecvError::Empty));
        ev.clear();
    }

    #[test]
    fn slots_changed_during_an_emission_count_from_the_next() {
        let ev = Event::<i32>::new("e");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let third = Arc::new(OnceLock::new());
        let (ev2, seen2, third2) = (ev.clone(), seen.clone(), third.clone());
        ev.connect(move |v| {
            lock(&seen2).push(format!("first {v}"));
            if *v == 1 {
                let seen3 = seen2.clone();
                ev2.connect(move |v| lock(&seen3).push(format!("new {v}")));
                if let Some(id) = third2.get() {
                    assert!(ev2.disconnect(*id));
                }
            }
        });
        let seen3 = seen.clone();
        let id = ev.connect(move |v| lock(&seen3).push(format!("third {v}")));
        third.set(id).unwrap();
        ev.emit(&1);
        ev.emit(&2);
        assert_eq!(
            *lock(&seen),
            ["first 1", "third 1", "first 2", "new 2"].map(String::from)
        );
        assert!(!ev.contains(id));
        assert_eq!(ev.len(), 2);
        ev.clear();
    }

    #[test]
    fn a_panicking_handler_is_isolated_and_its_error_emitted() {
        capture_logs();
        let ev = Event::<i32>::new("ticks");
        let ran = Arc::new(AtomicUsize::new(0));
        let (a, b) = (ran.clone(), ran.clone());
        ev.connect(move |_| {
            a.fetch_add(1, SeqCst);
        });
        ev.connect(|_| panic!("boom"));
        ev.connect(move |_| {
            b.fetch_add(1, SeqCst);
        });
        let errors = Arc::new(Mutex::new(Vec::new()));
        let errors2 = errors.clone();
        ev.error_event()
            .unwrap()
            .connect(move |e: &HandlerError| lock(&errors2).push(e.clone()));
        ev.emit(&1);
        assert_eq!(ran.load(SeqCst), 2);
        assert_eq!(
            *lock(&errors),
            [HandlerError {
                event: "ticks".into(),
                message: "boom".into()
            }]
        );

        // No listener: the error is dropped, not logged.
        let quiet = Event::<i32>::new("quiet");
        quiet.connect(|_| panic!("boom"));
        quiet.emit(&1);
        assert_eq!(errors_here(), []);

        // A sub-event has no error_event: its handler's panic is logged.
        let ev = Event::<i32>::new("ticks");
        ev.done_event().unwrap().connect(|_| panic!("boom"));
        assert!(ev.done_event().unwrap().error_event().is_none());
        ev.set_done();
        assert!(ev.done());
        assert_eq!(
            errors_here(),
            [(
                "eventkit.event".to_owned(),
                "Value caused exception for event done: boom".to_owned()
            )]
        );
    }

    #[test]
    fn an_ibs_event_runs_on_the_owner() {
        let ib = FakeIb::new();
        let ev = Event::<i32>::with_holder("updateEvent", ib.holder());
        let on = Arc::new(Mutex::new(Vec::new()));
        let on2 = on.clone();
        ev.connect(move |v| lock(&on2).push((*v, on_owner())));
        let done_on = Arc::new(Mutex::new(None));
        let done2 = done_on.clone();
        ev.done_event()
            .unwrap()
            .connect(move |_| *lock(&done2) = Some(on_owner()));

        ev.emit(&1);
        ev.set_done();
        assert!(lock(&on).is_empty());
        assert!(!ev.done());
        assert_eq!(ib.queued(), 2);
        assert_eq!(ib.run(), 2);
        assert_eq!(*lock(&on), [(1, true)]);
        assert_eq!(*lock(&done_on), Some(true));
        assert!(ev.done());

        // On the owner thread it runs inline.
        set_on_owner(true);
        ev.emit(&2);
        set_on_owner(false);
        assert_eq!(*lock(&on), [(1, true), (2, true)]);

        // Once the IB's queue is closed, on the caller.
        ib.closed.store(true, SeqCst);
        ev.emit(&3);
        assert_eq!(*lock(&on), [(1, true), (2, true), (3, false)]);
    }

    #[test]
    fn a_programs_event_emitted_on_another_thread_runs_there() {
        let ev = Event::<i32>::new("e");
        let on = Arc::new(Mutex::new(None));
        let on2 = on.clone();
        ev.connect(move |_| *lock(&on2) = Some(thread::current().id()));
        let ev2 = ev.clone();
        let job = thread::spawn(move || {
            ev2.emit(&1);
            thread::current().id()
        });
        let there = job.join().unwrap();
        assert_eq!(*lock(&on), Some(there));
    }

    #[test]
    fn done_keeps_handlers_and_emits_done_event_once() {
        let ev = Event::<i32>::new("e");
        let got = Arc::new(Mutex::new(Vec::new()));
        let got2 = got.clone();
        ev.connect(move |v| lock(&got2).push(*v));
        let dones = Arc::new(AtomicUsize::new(0));
        let dones2 = dones.clone();
        ev.done_event().unwrap().connect(move |_| {
            dones2.fetch_add(1, SeqCst);
        });
        ev.set_done();
        ev.set_done();
        assert!(ev.done());
        assert!(ev.done_event().unwrap().done());
        assert_eq!(dones.load(SeqCst), 1);
        ev.emit(&1);
        let got3 = got.clone();
        ev.connect(move |v| lock(&got3).push(*v * 10));
        ev.emit(&2);
        assert_eq!(*lock(&got), [1, 2, 20]);
        assert_eq!(ev.value(), Some(2));
    }

    #[test]
    fn a_panicking_slot_ends_every_subscription_with_its_error() {
        let ev = Event::<i32>::new("e");
        let mut a = ev.subscribe();
        let mut b = ev.subscribe();
        let mut c = ev.subscribe();
        ev.connect(|_| panic!("boom"));
        ev.emit(&1);

        assert_eq!(a.by_ref().collect::<Vec<_>>(), [Ok(1), Err(boom("e"))]);
        assert_eq!(a.next(), None);

        let mut next = || now(std::future::poll_fn(|cx| Pin::new(&mut b).poll_next(cx)));
        assert_eq!(next(), Some(Some(Ok(1))));
        assert_eq!(next(), Some(Some(Err(boom("e")))));
        assert_eq!(next(), Some(None));

        assert_eq!(now(c.recv_async()), Some(Ok(1)));
        assert_eq!(now(c.recv_async()), Some(Err(boom("e"))));
        assert_eq!(now(c.recv_async()), Some(Err(RecvError::Done)));

        // Each left the event once it reached its end.
        assert_eq!(ev.len(), 1);
        assert!(ev.error_event().unwrap().is_empty());
        assert!(ev.done_event().unwrap().is_empty());
    }

    #[test]
    fn a_dropped_subscription_leaves_the_event() {
        let ev = Event::<i32>::new("e");
        assert!(ev.is_empty());
        let sub = ev.subscribe();
        assert_eq!(ev.len(), 1);
        assert_eq!(ev.error_event().unwrap().len(), 1);
        assert_eq!(ev.done_event().unwrap().len(), 1);
        drop(sub);
        assert!(ev.is_empty());
        assert!(ev.error_event().unwrap().is_empty());
        assert!(ev.done_event().unwrap().is_empty());
    }

    #[test]
    fn blocking_receives_fail_on_the_owner_thread() {
        let ev = Event::<i32>::new("e");
        let mut sub = ev.subscribe();
        set_on_owner(true);
        assert_eq!(sub.recv(), Err(RecvError::OwnerThread));
        assert_eq!(
            sub.recv_timeout(Duration::from_secs(1)),
            Err(RecvError::OwnerThread)
        );
        assert_eq!(sub.try_recv(), Err(RecvError::Empty));
        ev.emit(&1);
        assert_eq!(now(sub.recv_async()), Some(Ok(1)));
        assert_eq!(sub.next(), Some(Err(RecvError::OwnerThread)));
        assert_eq!(sub.next(), None);
        set_on_owner(false);
    }

    #[test]
    fn a_subscription_receives_in_order_and_ends_after_its_queue() {
        let ev = Event::<i32>::new("e");
        let mut sub = ev.subscribe();
        assert_eq!(sub.try_recv(), Err(RecvError::Empty));
        assert_eq!(
            sub.recv_timeout(Duration::from_millis(10)),
            Err(RecvError::Timeout)
        );
        // A deadline past what an Instant holds is no deadline.
        ev.emit(&7);
        assert_eq!(sub.recv_timeout(Duration::MAX), Ok(7));
        ev.emit(&1);
        ev.emit(&2);
        assert_eq!(sub.len(), 2);
        ev.set_done();
        ev.emit(&3);
        assert_eq!(sub.recv(), Ok(1));
        assert_eq!(sub.recv(), Ok(2));
        assert_eq!(sub.recv(), Err(RecvError::Done));
        assert_eq!(sub.try_recv(), Err(RecvError::Done));
        assert!(sub.is_empty());

        // On a done event, an ended subscription that holds no slot.
        let mut late = ev.subscribe();
        assert_eq!(late.recv(), Err(RecvError::Done));
        assert_eq!(ev.len(), 0);
    }

    #[test]
    fn receives_wake_across_threads() {
        let ev = Event::<i32>::new("e");
        let mut sub = ev.subscribe();
        let ev2 = ev.clone();
        let job = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            ev2.emit(&1);
            thread::sleep(Duration::from_millis(20));
            ev2.emit(&2);
        });
        assert_eq!(sub.recv(), Ok(1));
        assert_eq!(block_on(sub.recv_async()), Ok(2));
        job.join().unwrap();
    }

    #[test]
    fn value_is_the_last_emitted() {
        let ev = Event::<String>::new("e");
        assert_eq!(ev.value(), None);
        ev.emit(&"a".to_owned());
        ev.emit(&"b".to_owned());
        assert_eq!(ev.value().as_deref(), Some("b"));
        assert_eq!(format!("{ev:?}"), r#"Event { name: "e", slots: 0 }"#);
    }

    /// A value whose drop records whether its event's lock was held.
    #[derive(Clone)]
    struct Probe {
        ev: Arc<OnceLock<Event<Probe>>>,
        under_lock: Arc<AtomicBool>,
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            if let Some(ev) = self.ev.get()
                && ev.0.state.try_lock().is_err()
            {
                self.under_lock.store(true, SeqCst);
            }
        }
    }

    #[test]
    fn removed_slots_and_values_are_dropped_after_the_lock() {
        let cell = Arc::new(OnceLock::new());
        let under_lock = Arc::new(AtomicBool::new(false));
        let probe = Probe {
            ev: cell.clone(),
            under_lock: under_lock.clone(),
        };
        let ev = Event::<Probe>::new("e");
        cell.set(ev.clone()).unwrap();

        let p = probe.clone();
        let id = ev.connect(move |_| drop(p.clone()));
        assert!(ev.disconnect(id));
        let p = probe.clone();
        ev.connect(move |_| drop(p.clone()));
        ev.clear();

        let mut sub = ev.subscribe();
        ev.emit(&probe);
        ev.emit(&probe);
        drop(sub.try_recv());
        drop(sub);

        // A handler that disconnects itself lives to its emission's end.
        let own = Arc::new(OnceLock::new());
        let (p, own2, ev2) = (probe.clone(), own.clone(), ev.clone());
        let id = ev.connect(move |_| {
            let _keep = &p;
            ev2.disconnect(*own2.get().unwrap());
        });
        own.set(id).unwrap();
        ev.emit(&probe);
        drop(probe);
        assert!(!under_lock.load(SeqCst));
        assert!(ev.is_empty());
    }

    /// Serializes the tests that share the process's task inbox.
    static TASK_TESTS: Mutex<()> = Mutex::new(());

    fn tasks() -> Tasks {
        drop(std::mem::take(&mut *lock(&SPAWNED)));
        Tasks::default()
    }

    /// Runs one lap's task phase, as the owner.
    fn lap(tasks: &mut Tasks) {
        tasks.take_spawned();
        set_on_owner(true);
        for t in tasks.take_woken() {
            t.poll();
        }
        set_on_owner(false);
    }

    #[test]
    fn an_async_handler_is_called_inline_and_its_task_polled_after() {
        let _serial = lock(&TASK_TESTS);
        let mut tasks = tasks();
        let ev = Event::<i32>::new("e");
        let (called, polled) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (called2, polled2) = (called.clone(), polled.clone());
        ev.connect_async(move |v| {
            called2.fetch_add(*v as usize, SeqCst);
            let polled = polled2.clone();
            async move {
                polled.fetch_add(1, SeqCst);
            }
        });
        ev.emit(&3);
        assert_eq!(called.load(SeqCst), 3);
        assert_eq!(polled.load(SeqCst), 0);
        lap(&mut tasks);
        assert_eq!(polled.load(SeqCst), 1);
        lap(&mut tasks);
        assert_eq!(tasks.len(), 0);
    }

    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, SeqCst);
        }
    }

    #[test]
    fn a_panic_while_polling_a_task_is_logged_and_drops_it() {
        capture_logs();
        let _serial = lock(&TASK_TESTS);
        let mut tasks = tasks();
        let ev = Event::<i32>::new("e");
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped2 = dropped.clone();
        ev.connect_async(move |_| {
            let guard = Dropped(dropped2.clone());
            async move {
                let _guard = guard;
                panic!("boom");
            }
        });
        ev.emit(&1);
        lap(&mut tasks);
        assert!(dropped.load(SeqCst));
        assert_eq!(tasks.len(), 0);
        assert_eq!(
            errors_here(),
            [(
                "asyncio".to_owned(),
                "Task exception was never retrieved: boom".to_owned()
            )]
        );
    }

    /// Pending until woken once, then ready.
    struct Twice(bool);

    impl Future for Twice {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            WAKER.with(|w| *lock(w) = Some(cx.waker().clone()));
            Poll::Pending
        }
    }

    thread_local! {
        static WAKER: Mutex<Option<Waker>> = const { Mutex::new(None) };
    }

    #[test]
    fn a_task_outlives_its_event_and_runs_when_woken() {
        let _serial = lock(&TASK_TESTS);
        let mut tasks = tasks();
        let ev = Event::<i32>::new("e");
        let finished = Arc::new(AtomicBool::new(false));
        let finished2 = finished.clone();
        ev.connect_async(move |_| {
            let finished = finished2.clone();
            async move {
                Twice(false).await;
                finished.store(true, SeqCst);
            }
        });
        ev.emit(&1);
        drop(ev);
        lap(&mut tasks);
        lap(&mut tasks);
        assert!(!finished.load(SeqCst));
        assert_eq!(tasks.len(), 1);
        WAKER.with(|w| lock(w).take()).unwrap().wake();
        lap(&mut tasks);
        assert!(finished.load(SeqCst));
        lap(&mut tasks);
        assert_eq!(tasks.len(), 0);
    }

    #[test]
    fn a_task_started_on_a_user_thread_runs_on_the_owner() {
        let _serial = lock(&TASK_TESTS);
        let mut tasks = tasks();
        let ev = Event::<i32>::new("e");
        let on = Arc::new(Mutex::new(None));
        let on2 = on.clone();
        ev.connect_async(move |_| {
            let on = on2.clone();
            async move {
                *lock(&on) = Some(on_owner());
            }
        });
        let ev2 = ev.clone();
        thread::spawn(move || ev2.emit(&1)).join().unwrap();
        assert_eq!(*lock(&on), None);
        lap(&mut tasks);
        assert_eq!(*lock(&on), Some(true));
    }

    static OWNER_WAKES: AtomicUsize = AtomicUsize::new(0);

    fn count_wake() {
        OWNER_WAKES.fetch_add(1, SeqCst);
    }

    #[test]
    fn a_tasks_start_and_its_waker_wake_the_owner() {
        let _serial = lock(&TASK_TESTS);
        let mut tasks = tasks();
        set_owner_wake(count_wake);
        let ev = Event::<i32>::new("e");
        ev.connect_async(|_| Twice(false));
        let before = OWNER_WAKES.load(SeqCst);
        ev.emit(&1);
        assert_eq!(OWNER_WAKES.load(SeqCst), before + 1);
        lap(&mut tasks);
        WAKER.with(|w| lock(w).take()).unwrap().wake();
        assert_eq!(OWNER_WAKES.load(SeqCst), before + 2);
        lap(&mut tasks);
        assert_eq!(tasks.len(), 0);
    }

    #[test]
    fn a_self_waking_task_is_polled_once_per_lap() {
        let _serial = lock(&TASK_TESTS);
        let mut tasks = tasks();
        let ev = Event::<i32>::new("e");
        let polls = Arc::new(AtomicUsize::new(0));
        let polls2 = polls.clone();
        ev.connect_async(move |_| {
            let polls = polls2.clone();
            std::future::poll_fn(move |cx| {
                polls.fetch_add(1, SeqCst);
                cx.waker().wake_by_ref();
                Poll::<()>::Pending
            })
        });
        ev.emit(&1);
        for n in 1..=3 {
            lap(&mut tasks);
            assert_eq!(polls.load(SeqCst), n);
        }
    }

    #[test]
    fn an_empty_name_is_events() {
        assert_eq!(Event::<i32>::new("").name(), "Event");
        assert_eq!(Event::<i32>::new("e").name(), "e");
    }

    #[test]
    fn an_ibs_sub_events_run_on_the_owner() {
        let ib = FakeIb::new();
        let ev = Event::<i32>::with_holder("updateEvent", ib.holder());
        let on = Arc::new(Mutex::new(Vec::new()));
        let (o, o2) = (on.clone(), on.clone());
        ev.done_event()
            .unwrap()
            .connect(move |_| lock(&o).push(on_owner()));
        ev.error_event()
            .unwrap()
            .connect(move |_| lock(&o2).push(on_owner()));
        ev.done_event().unwrap().emit(&());
        ev.error_event().unwrap().emit(&HandlerError {
            event: "e".into(),
            message: "m".into(),
        });
        assert!(lock(&on).is_empty());
        assert_eq!(ib.run(), 2);
        assert_eq!(*lock(&on), [true, true]);
    }

    #[test]
    fn try_recv_alone_leaves_the_event_at_its_end() {
        let ev = Event::<i32>::new("e");
        let mut sub = ev.subscribe();
        ev.set_done();
        assert_eq!(sub.try_recv(), Err(RecvError::Done));
        assert!(ev.is_empty());
        assert!(ev.error_event().unwrap().is_empty());
        assert!(ev.done_event().unwrap().is_empty());
    }

    /// A waker that records whether its queue's lock was held when it was
    /// woken or dropped.
    struct QueueProbe {
        queue: Arc<Queue<i32>>,
        under_lock: Arc<AtomicBool>,
    }

    impl QueueProbe {
        fn check(&self) {
            if self.queue.state.try_lock().is_err() {
                self.under_lock.store(true, SeqCst);
            }
        }
    }

    impl Wake for QueueProbe {
        fn wake(self: Arc<Self>) {
            self.check();
        }
    }

    impl Drop for QueueProbe {
        fn drop(&mut self) {
            self.check();
        }
    }

    #[test]
    fn a_subscriptions_waker_is_woken_and_dropped_after_the_lock() {
        let ev = Event::<i32>::new("e");
        let mut sub = ev.subscribe();
        let under_lock = Arc::new(AtomicBool::new(false));
        let probe = || {
            Waker::from(Arc::new(QueueProbe {
                queue: sub.queue.clone(),
                under_lock: under_lock.clone(),
            }))
        };
        let (first, second) = (probe(), probe());
        assert!(sub.poll_recv(&mut Context::from_waker(&first)).is_pending());
        drop(first);
        // The first is displaced, and dropped with its last reference.
        assert!(
            sub.poll_recv(&mut Context::from_waker(&second))
                .is_pending()
        );
        drop(second);
        // The push wakes the second, the last reference, and drops it.
        ev.emit(&1);
        assert!(!under_lock.load(SeqCst));
        assert_eq!(sub.try_recv(), Ok(1));
    }

    /// A value whose clone records whether its event's lock was held.
    struct CloneProbe {
        ev: Arc<OnceLock<Event<CloneProbe>>>,
        under_lock: Arc<AtomicBool>,
    }

    impl Clone for CloneProbe {
        fn clone(&self) -> Self {
            if let Some(ev) = self.ev.get()
                && ev.0.state.try_lock().is_err()
            {
                self.under_lock.store(true, SeqCst);
            }
            CloneProbe {
                ev: self.ev.clone(),
                under_lock: self.under_lock.clone(),
            }
        }
    }

    #[test]
    fn value_clones_after_the_lock() {
        let cell = Arc::new(OnceLock::new());
        let under_lock = Arc::new(AtomicBool::new(false));
        let ev = Event::<CloneProbe>::new("e");
        cell.set(ev.clone()).unwrap();
        ev.emit(&CloneProbe {
            ev: cell.clone(),
            under_lock: under_lock.clone(),
        });
        assert!(ev.value().is_some());
        assert!(!under_lock.load(SeqCst));
        ev.clear();
    }

    #[test]
    fn errors_display_as_their_values() {
        let e = HandlerError {
            event: "e".into(),
            message: "boom".into(),
        };
        assert_eq!(e.to_string(), "boom");
        assert_eq!(RecvError::Handler(e).to_string(), "boom");
        assert_eq!(RecvError::Timeout.to_string(), "timed out");
        let boxed: Box<dyn std::error::Error> = Box::new(RecvError::Done);
        assert_eq!(boxed.to_string(), "the event is done");
    }
}
