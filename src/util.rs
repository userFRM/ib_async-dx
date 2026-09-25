//! ib_async's `util`: its constants, `formatSI`, the IB datetime helpers,
//! the forms its date and time parameters take, and the global error event.

use std::future::{Future, IntoFuture};
use std::pin::{Pin, pin};
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use jiff::tz::{TimeZone, TimeZoneDatabase};
use jiff::{Timestamp, Zoned, civil};

use crate::error::{Error, Result};
use crate::event::{Event, lock, on_owner};
use crate::objects::DynamicValue;
use crate::pending::{Pending, Unpark};

/// The TWS API's "not set" for an integer: ib_async's `UNSET_INTEGER`
/// (`2**31 - 1`).
pub const UNSET_INTEGER: i32 = i32::MAX;

/// The TWS API's "not set" for a float: ib_async's `UNSET_DOUBLE`
/// (`sys.float_info.max`).
pub const UNSET_DOUBLE: f64 = f64::MAX;

/// 1970-01-01 00:00:00 UTC: ib_async's `EPOCH`.
pub static EPOCH: LazyLock<Zoned> = LazyLock::new(|| Timestamp::UNIX_EPOCH.to_zoned(TimeZone::UTC));

/// The error of a peer or internal close of any IB, emitted on the owner:
/// ib_async's `globalErrorEvent`. Every blocking wait fails with its value.
pub fn global_error_event() -> &'static Event<Error> {
    // eventkit names an unnamed event after its class.
    static EVENT: LazyLock<Event<Error>> = LazyLock::new(|| Event::new("Event"));
    &EVENT
}

/// Runs `f` to its end on this thread, parked between polls: ib_async's
/// `util.run` of a coroutine, with `timeout` as its `wait_for`. It gives the
/// future's output, or `Err(Timeout)` at the deadline, the future then
/// dropped as asyncio cancels its task. While it waits it listens to
/// [`global_error_event`], and a peer or internal close of any IB fails it
/// with that close's error once the owner's unit that emitted it ends, as
/// `util.run` cancels its task and raises the value. On the owner thread it
/// is `Err(Value)` at once, as asyncio refuses to run a loop that is already
/// running.
pub(crate) fn block_on<F: IntoFuture>(f: F, timeout: Option<Duration>) -> Result<F::Output> {
    if on_owner() {
        return Err(Error::Value(
            "This event loop is already running".to_owned(),
        ));
    }
    let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
    // The close's error decides this gate, published when the owner's unit
    // ends, as a waiter's slot is.
    let (mut gate, reply) = Pending::<()>::new(None);
    let reply = Mutex::new(Some(reply));
    let event = global_error_event();
    let id = event.connect(move |e| {
        let reply = lock(&reply).take();
        if let Some(reply) = reply {
            reply.send(Err(e.clone()));
        }
    });
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut f = pin!(f.into_future());
    let r = loop {
        if let Poll::Ready(Err(e)) = Pin::new(&mut gate).poll(&mut cx) {
            break Err(e);
        }
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            break Ok(v);
        }
        match deadline.map(|d| d.checked_duration_since(Instant::now())) {
            None => thread::park(),
            Some(Some(left)) if !left.is_zero() => thread::park_timeout(left),
            Some(_) if gate.expire() => break Err(Error::Timeout),
            // Decided: its publication wakes this thread.
            Some(_) => thread::park(),
        }
    };
    drop(gate);
    event.disconnect(id);
    r
}

/// `n` to three significant digits and an SI prefix, as `"1.23 k"`:
/// ib_async's `formatSI`, whose `n` is a float. A magnitude below `1e-22` is
/// `"0.00 "`. NaN, infinity and a magnitude at or past `9.99e26` are
/// `Err(Value)`, where ib_async's assertion fails.
pub fn format_si(n: f64) -> Result<String> {
    // n * 10 ** (3 * k) for k = 0..=8, as Python multiplies by the exact
    // integer 10 ** (3 * k), correctly rounded to a float.
    const UP: [f64; 9] = [1.0, 1e3, 1e6, 1e9, 1e12, 1e15, 1e18, 1e21, 1e24];
    let (sign, n) = if n < 0.0 { ("-", -n) } else { ("", n) };
    if n < 1e-22 {
        // ib_async assigns here, dropping the sign it wrote.
        return Ok("0.00 ".to_owned());
    }
    if n.is_nan() || n >= 9.99e26 {
        return Err(Error::Value(format!("formatSI: {n} is not below 9.99e26")));
    }
    let log = n.log10().floor() as i32;
    let (mut i, mut j) = (log.div_euclid(3), log.rem_euclid(3));
    let mut val = String::new();
    for _ in 0..2 {
        let scaled = if i <= 0 {
            n * UP
                .get(i.unsigned_abs() as usize)
                .copied()
                .unwrap_or(f64::NAN)
        } else {
            // Python's `10 ** -k` is the float power.
            n * 10f64.powf(f64::from(-3 * i))
        };
        val = format!("{scaled:.*}", (2 - j) as usize);
        if val != "1000" {
            break;
        }
        i += 1;
        j = 0;
    }
    let mut s = format!("{sign}{val} ");
    if i != 0
        && let Some(prefix) = "yzafpnum kMGTPEZY".chars().nth((i + 8) as usize)
    {
        s.push(prefix);
    }
    Ok(s)
}

/// A date, an aware datetime or a naive one: what ib_async's
/// `parseIBDatetime` returns, and a bar's `date`.
///
/// Two values of the same form compare; values of different forms are
/// unequal and unordered, as Python refuses to order a date against a
/// datetime or a naive datetime against an aware one. `At` compares by
/// instant. Python compares two aware values that share a zone by their wall
/// fields instead, so in a repeated hour it calls 01:30 EDT and 01:30 EST
/// equal; here they are an hour apart.
#[derive(Clone, Debug)]
pub enum BarDate {
    /// A date: ib_async's `date`.
    Day(civil::Date),
    /// An aware datetime: ib_async's `datetime` with a `tzinfo`.
    At(Zoned),
    /// A naive datetime: ib_async's `datetime` without a `tzinfo`.
    Naive(civil::DateTime),
}

impl PartialEq for BarDate {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for BarDate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (BarDate::Day(a), BarDate::Day(b)) => Some(a.cmp(b)),
            (BarDate::At(a), BarDate::At(b)) => Some(a.timestamp().cmp(&b.timestamp())),
            (BarDate::Naive(a), BarDate::Naive(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

/// A point in time as ib_async's `Time_t` (`time | datetime`) gives it:
/// what `schedule`, `wait_until` and `time_range` take.
#[derive(Clone, Debug)]
pub enum TimeT {
    /// An instant.
    At(Timestamp),
    /// An aware datetime.
    Zoned(Zoned),
    /// A time of day: today at that time in the system zone, as ib_async
    /// fills a `time` with today's date. A time in a DST gap lands after the
    /// gap, as Python's naive datetime does.
    Today(civil::Time),
}

impl From<Timestamp> for TimeT {
    fn from(t: Timestamp) -> Self {
        TimeT::At(t)
    }
}

impl From<Zoned> for TimeT {
    fn from(t: Zoned) -> Self {
        TimeT::Zoned(t)
    }
}

impl From<civil::Time> for TimeT {
    fn from(t: civil::Time) -> Self {
        TimeT::Today(t)
    }
}

impl TimeT {
    /// The instant this names, as a datetime: `Today` is `now`'s date in
    /// `local` at the time given, and `At` is in UTC.
    pub(crate) fn to_zoned(&self, now: Timestamp, local: &TimeZone) -> Result<Zoned> {
        match self {
            TimeT::At(t) => Ok(t.to_zoned(TimeZone::UTC)),
            TimeT::Zoned(z) => Ok(z.clone()),
            TimeT::Today(t) => now
                .to_zoned(local.clone())
                .date()
                .to_datetime(*t)
                .to_zoned(local.clone())
                .map_err(value),
        }
    }
}

/// A date or time argument as ib_async's `datetime | date | str | None`
/// gives it, kept as given: a request stores it and sends
/// [`format_ib_datetime`]'s text for it.
///
/// ib_async's `None` is spelled `DateTimeArg::None`; a bare `None` names no
/// type and does not compile:
///
/// ```compile_fail
/// let _ = ib_async_dx::util::format_ib_datetime(None);
/// ```
///
/// ```
/// use ib_async_dx::util::{DateTimeArg, format_ib_datetime};
/// assert_eq!(format_ib_datetime(DateTimeArg::None).ok().as_deref(), Some(""));
/// ```
#[derive(Clone, Debug)]
pub enum DateTimeArg {
    /// No value: ib_async's `None`, sent as `""`.
    None,
    /// Text, sent as given.
    Text(String),
    /// A date: the end of that day in the system zone.
    Day(civil::Date),
    /// An aware datetime.
    At(Zoned),
    /// A naive datetime, read in the system zone.
    Naive(civil::DateTime),
}

impl From<&str> for DateTimeArg {
    fn from(s: &str) -> Self {
        DateTimeArg::Text(s.to_owned())
    }
}

impl From<String> for DateTimeArg {
    fn from(s: String) -> Self {
        DateTimeArg::Text(s)
    }
}

impl From<BarDate> for DateTimeArg {
    fn from(d: BarDate) -> Self {
        match d {
            BarDate::Day(d) => DateTimeArg::Day(d),
            BarDate::At(z) => DateTimeArg::At(z),
            BarDate::Naive(t) => DateTimeArg::Naive(t),
        }
    }
}

impl From<Zoned> for DateTimeArg {
    fn from(z: Zoned) -> Self {
        DateTimeArg::At(z)
    }
}

impl From<civil::Date> for DateTimeArg {
    fn from(d: civil::Date) -> Self {
        DateTimeArg::Day(d)
    }
}

impl From<civil::DateTime> for DateTimeArg {
    fn from(t: civil::DateTime) -> Self {
        DateTimeArg::Naive(t)
    }
}

/// The text IB takes for a date or time: ib_async's `formatIBDatetime`.
/// `None` and `""` are `""`, other text is kept, and a datetime is converted
/// to UTC and written `%Y%m%d %H:%M:%S UTC`. A naive datetime is read in the
/// system zone, and a date is 23:59:59 on that day there. A time the zone
/// cannot represent, or a UTC year outside 1..=9999, is `Err(Value)`.
pub fn format_ib_datetime(t: impl Into<DateTimeArg>) -> Result<String> {
    format_ib_datetime_in(t.into(), &TimeZone::system())
}

fn format_ib_datetime_in(t: DateTimeArg, local: &TimeZone) -> Result<String> {
    let at = match t {
        DateTimeArg::None => return Ok(String::new()),
        DateTimeArg::Text(s) => return Ok(s),
        DateTimeArg::At(z) => z.timestamp(),
        DateTimeArg::Naive(t) => t.to_zoned(local.clone()).map_err(value)?.timestamp(),
        DateTimeArg::Day(d) => d
            .at(23, 59, 59, 0)
            .to_zoned(local.clone())
            .map_err(value)?
            .timestamp(),
    };
    let utc = at.to_zoned(TimeZone::UTC);
    if !(1..=9999).contains(&utc.year()) {
        return Err(Error::Value("date value out of range".to_owned()));
    }
    Ok(utc.strftime("%Y%m%d %H:%M:%S UTC").to_string())
}

/// A date or datetime in IB's text: ib_async's `parseIBDatetime`.
///
/// Eight characters are a date, `YYYYmmdd`. Digits alone are epoch seconds,
/// an instant in UTC. `date time zone`, separated by single spaces, is that
/// wall time in that IANA zone. Anything else is read as a naive datetime,
/// `%Y%m%d%H:%M:%S`, from its first 16 characters once spaces and dashes are
/// removed. What does not parse, and an unknown zone, is `Err(Value)`.
pub fn parse_ib_datetime(s: &str) -> Result<BarDate> {
    if s.chars().count() == 8 {
        let part = |skip, take| -> Result<i64> {
            let p: String = s.chars().skip(skip).take(take).collect();
            py_int(&p).ok_or_else(|| {
                Error::Value(format!("invalid literal for int() with base 10: '{p}'"))
            })
        };
        let (y, m, d) = (part(0, 4)?, part(4, 2)?, part(6, 2)?);
        return day(y, m, d).map(BarDate::Day);
    }
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return s
            .parse::<i64>()
            .ok()
            .and_then(|n| Timestamp::from_second(n).ok())
            .map(|t| BarDate::At(t.to_zoned(TimeZone::UTC)))
            .ok_or_else(|| Error::Value(format!("timestamp out of range: {s}")));
    }
    if s.matches(' ').count() >= 2 && !s.contains("  ") {
        let mut parts = s.splitn(3, ' ');
        let (s0, s1, zone) = (
            parts.next().unwrap_or(""),
            parts.next().unwrap_or(""),
            parts.next().unwrap_or(""),
        );
        let wall = strptime(&format!("{s0}{s1}"), FORMAT)?;
        let tz = self::zone(zone)?;
        return wall.to_zoned(tz).map(BarDate::At).map_err(value);
    }
    let digits: String = s
        .chars()
        .filter(|c| *c != ' ' && *c != '-')
        .take(16)
        .collect();
    strptime(&digits, FORMAT).map(BarDate::Naive)
}

/// `parseIBDatetime`'s format for a datetime.
const FORMAT: &str = "%Y%m%d%H:%M:%S";

/// The zone named `name`, as ib_async's `ZoneInfo(name)` finds it: the
/// system's database, then the bundled copy, as `zoneinfo` searches its
/// `TZPATH` and then the `tzdata` package ib_async requires. Names such as
/// `US/Eastern`, which IB sends, are often missing from a system's database.
/// An unknown name is `Err(Value)`.
pub(crate) fn zone(name: &str) -> Result<TimeZone> {
    TimeZone::get(name).or_else(|e| TimeZoneDatabase::bundled().get(name).map_err(|_| value(e)))
}

fn value(e: impl std::fmt::Display) -> Error {
    Error::Value(e.to_string())
}

/// Python's `date(y, m, d)`, with its checks and texts.
fn day(y: i64, m: i64, d: i64) -> Result<civil::Date> {
    if !(1..=9999).contains(&y) {
        return Err(Error::Value(format!("year must be in 1..9999, not {y}")));
    }
    if !(1..=12).contains(&m) {
        return Err(Error::Value(format!("month must be in 1..12, not {m}")));
    }
    let (year, month) = (y as i16, m as i8);
    let days = civil::Date::new(year, month, 1)
        .map_err(value)?
        .days_in_month();
    if !(1..=i64::from(days)).contains(&d) {
        return Err(Error::Value(format!(
            "day {d} must be in range 1..{days} for month {m} in year {y}"
        )));
    }
    civil::Date::new(year, month, d as i8).map_err(value)
}

/// `datetime.strptime(s, format)` for a format of `%Y`, `%m`, `%d`, `%H`,
/// `%M`, `%S` and literal characters. Each directive tries CPython's
/// patterns for it in their order, and the first match must use all of `s`;
/// a field the format lacks is Python's default. The texts of the errors are
/// Python's. Only ASCII digits are read.
pub(crate) fn strptime(s: &str, format: &str) -> Result<civil::DateTime> {
    let parts: Vec<Part> = parts(format.as_bytes()).collect();
    // Y, m, d, H, M, S, with Python's defaults.
    let mut f = [1900, 1, 1, 0, 0, 0];
    let Some(end) = first_match(s.as_bytes(), &parts, 0, &mut f) else {
        return Err(Error::Value(format!(
            "time data '{s}' does not match format '{format}'"
        )));
    };
    if let Some(rest) = s.get(end..).filter(|r| !r.is_empty()) {
        return Err(Error::Value(format!("unconverted data remains: {rest}")));
    }
    let [y, mo, d, h, mi, sec] = f;
    let date = day(y, mo, d)?;
    if sec > 59 {
        return Err(Error::Value(format!("second must be in 0..59, not {sec}")));
    }
    let (Ok(h), Ok(mi), Ok(sec)) = (i8::try_from(h), i8::try_from(mi), i8::try_from(sec)) else {
        return Err(Error::Value(format!("time data '{s}' is out of range")));
    };
    civil::Time::new(h, mi, sec, 0)
        .map(|t| date.to_datetime(t))
        .map_err(value)
}

/// One alternative of a directive: a byte range per character.
type Alt = &'static [(u8, u8)];

/// A piece of a format: a directive, as the index of its field and CPython's
/// alternatives for it in `_strptime`'s order, or a literal byte.
enum Part {
    Field(usize, &'static [Alt]),
    Literal(u8),
}

/// The pieces of `format`.
fn parts(format: &[u8]) -> impl Iterator<Item = Part> + '_ {
    const D: (u8, u8) = (b'0', b'9');
    const SP: (u8, u8) = (b' ', b' ');
    const ONE: (u8, u8) = (b'1', b'9');
    let mut i = 0;
    std::iter::from_fn(move || {
        let c = *format.get(i)?;
        i += 1;
        if c != b'%' {
            return Some(Part::Literal(c));
        }
        let d = *format.get(i)?;
        i += 1;
        Some(match d {
            b'Y' => Part::Field(0, &[&[D, D, D, D]]),
            b'm' => Part::Field(
                1,
                &[&[(b'1', b'1'), (b'0', b'2')], &[(b'0', b'0'), ONE], &[ONE]],
            ),
            b'd' => Part::Field(
                2,
                &[
                    &[(b'3', b'3'), (b'0', b'1')],
                    &[(b'1', b'2'), D],
                    &[(b'0', b'0'), ONE],
                    &[ONE],
                    &[SP, ONE],
                ],
            ),
            b'H' => Part::Field(
                3,
                &[
                    &[(b'2', b'2'), (b'0', b'3')],
                    &[(b'0', b'1'), D],
                    &[D],
                    &[SP, D],
                ],
            ),
            b'M' => Part::Field(4, &[&[(b'0', b'5'), D], &[D]]),
            b'S' => Part::Field(
                5,
                &[&[(b'6', b'6'), (b'0', b'1')], &[(b'0', b'5'), D], &[D]],
            ),
            other => Part::Literal(other),
        })
    })
}

/// The first match of `parts` at `pos`, as CPython's regular expression
/// finds it: the end it reaches, with each field's digits in `out`.
fn first_match(s: &[u8], parts: &[Part], pos: usize, out: &mut [i64; 6]) -> Option<usize> {
    let Some((part, rest)) = parts.split_first() else {
        return Some(pos);
    };
    let (i, alternatives) = match part {
        Part::Literal(c) => {
            return (s.get(pos) == Some(c))
                .then(|| first_match(s, rest, pos + 1, out))
                .flatten();
        }
        Part::Field(i, alternatives) => (*i, *alternatives),
    };
    for alt in alternatives {
        let Some(text) = s.get(pos..pos + alt.len()) else {
            continue;
        };
        if text
            .iter()
            .zip(alt.iter())
            .all(|(c, (lo, hi))| (lo..=hi).contains(&c))
        {
            if let Some(slot) = out.get_mut(i) {
                *slot = text
                    .iter()
                    .filter(|c| c.is_ascii_digit())
                    .fold(0, |v, c| v * 10 + i64::from(c - b'0'));
            }
            if let Some(end) = first_match(s, rest, pos + alt.len(), out) {
                return Some(end);
            }
        }
    }
    None
}

/// Python's `float(s)`: surrounding whitespace, a sign, decimal digits with
/// single underscores between them, a fraction and an exponent, or
/// `nan`/`inf`/`infinity` in any case. Only ASCII digits are read.
pub(crate) fn py_float(s: &str) -> Option<f64> {
    without_underscores(s.trim())?.parse().ok()
}

/// Python's `int(s)` in base 10, when the value fits an `i64`: surrounding
/// whitespace, a sign, and ASCII digits with single underscores between
/// them.
pub(crate) fn py_int(s: &str) -> Option<i64> {
    let t = s.trim();
    let digits = t.strip_prefix(['+', '-']).unwrap_or(t);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit() || b == b'_') {
        return None;
    }
    without_underscores(t)?.parse().ok()
}

/// Text as ib_async's tick 47 and `FlexReport.extract` read it: `float()`,
/// then `int()`, keeping the text where `float()` fails.
pub(crate) fn py_number(s: &str) -> DynamicValue {
    match py_float(s) {
        None => DynamicValue::Str(s.to_owned()),
        Some(f) => py_int(s).map_or(DynamicValue::Float(f), DynamicValue::Int),
    }
}

/// `s` with its underscores removed, or `None` when one is not between two
/// digits, Python's rule for numeric text.
fn without_underscores(s: &str) -> Option<String> {
    let b = s.as_bytes();
    for (i, _) in s.match_indices('_') {
        let before = i.checked_sub(1).and_then(|j| b.get(j));
        if !before.is_some_and(u8::is_ascii_digit) || !b.get(i + 1).is_some_and(u8::is_ascii_digit)
        {
            return None;
        }
    }
    Some(s.replace('_', ""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::set_on_owner;
    use crate::pending::unit;
    use crate::tests::GLOBAL_ERRORS;
    use std::cmp::Ordering;

    fn ny() -> TimeZone {
        TimeZone::get("America/New_York").unwrap()
    }

    #[test]
    fn format_si_matches_ib_async() {
        // Each pinned against ib_async 2.1.0's formatSI with a float.
        for (n, want) in [
            (1234.5, "1.23 k"),
            (-1234.5, "-1.23 k"),
            (0.0, "0.00 "),
            (-1e-30, "0.00 "),
            (5.0, "5.00 "),
            (999.0, "999 "),
            (999.49, "999 "),
            (999.5, "1.00 k"),
            (999.999, "1.00 k"),
            (1000.0, "1.00 k"),
            (0.001234, "1.23 m"),
            (0.5, "500 m"),
            (1e-22, "100 y"),
            (123456789.0, "123 M"),
            (1e26, "100 Y"),
        ] {
            assert_eq!(format_si(n).unwrap(), want, "{n}");
        }
        for n in [9.99e26, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(format_si(n), Err(Error::Value(_))), "{n}");
        }
    }

    #[test]
    fn global_error_event_is_one_event() {
        let e = global_error_event();
        assert!(std::ptr::eq(e, global_error_event()));
        assert_eq!(e.name(), "Event");
    }

    #[test]
    fn consts() {
        assert_eq!(UNSET_INTEGER, 2_147_483_647);
        assert_eq!(UNSET_DOUBLE, f64::MAX);
        assert_eq!(EPOCH.timestamp(), Timestamp::UNIX_EPOCH);
        assert_eq!(EPOCH.time_zone(), &TimeZone::UTC);
    }

    #[test]
    fn parse_each_form() {
        let date = parse_ib_datetime("20240105").unwrap();
        assert!(matches!(date, BarDate::Day(d) if d == civil::date(2024, 1, 5)));

        let BarDate::At(z) = parse_ib_datetime("1700000000").unwrap() else {
            panic!("epoch seconds are an instant")
        };
        assert_eq!(z.timestamp().as_second(), 1_700_000_000);
        assert_eq!(z.time_zone(), &TimeZone::UTC);

        let BarDate::At(z) = parse_ib_datetime("20221125 10:00:00 Europe/Amsterdam").unwrap()
        else {
            panic!("a zone makes an aware datetime")
        };
        assert_eq!(z.datetime(), civil::date(2022, 11, 25).at(10, 0, 0, 0));
        assert_eq!(z.time_zone().iana_name(), Some("Europe/Amsterdam"));
        assert_eq!(z.offset().seconds(), 3600);

        for (s, want) in [
            (
                "20240105  09:30:00",
                civil::date(2024, 1, 5).at(9, 30, 0, 0),
            ),
            (
                "2024-01-05 09:30:00.0",
                civil::date(2024, 1, 5).at(9, 30, 0, 0),
            ),
            // One-digit fields, as strptime takes them.
            ("2024-1-5 9:30:00", civil::date(2024, 1, 5).at(9, 30, 0, 0)),
            ("2024010509:30:0", civil::date(2024, 1, 5).at(9, 30, 0, 0)),
            // Past 16 characters is ignored.
            ("2024010509:30:00x", civil::date(2024, 1, 5).at(9, 30, 0, 0)),
        ] {
            let got = parse_ib_datetime(s).unwrap();
            assert!(matches!(got, BarDate::Naive(t) if t == want), "{s}");
        }
    }

    #[test]
    fn parse_errors() {
        for s in [
            "20240230",
            "00000101",
            "+2024010",
            "2024_101",
            "",
            "abc",
            "20240105 09:30:60",
            "2024-01-05",
            "202401050930:00:00",
            "20221125 10:00:00 Mars/Base",
            "99999999999999999999",
        ] {
            assert!(
                matches!(parse_ib_datetime(s), Err(Error::Value(_))),
                "{s:?}"
            );
        }
        let Err(Error::Value(m)) = parse_ib_datetime("abc") else {
            panic!()
        };
        assert_eq!(m, "time data 'abc' does not match format '%Y%m%d%H:%M:%S'");
        let Err(Error::Value(m)) = parse_ib_datetime("2024_101") else {
            panic!()
        };
        assert_eq!(m, "invalid literal for int() with base 10: '_1'");
    }

    #[test]
    fn parse_in_a_zone_resolves_as_python() {
        // A repeated hour takes its first occurrence and a gap the offset
        // before it, as Python's fold=0 does.
        let BarDate::At(z) = parse_ib_datetime("20241103 01:30:00 America/New_York").unwrap()
        else {
            panic!()
        };
        assert_eq!(z.offset().seconds(), -4 * 3600);
        let BarDate::At(z) = parse_ib_datetime("20240310 02:30:00 America/New_York").unwrap()
        else {
            panic!()
        };
        assert_eq!(z.timestamp().to_string(), "2024-03-10T07:30:00Z");
    }

    #[test]
    fn bar_date_order_within_a_form_and_none_across() {
        let d1 = BarDate::Day(civil::date(2024, 1, 5));
        let d2 = BarDate::Day(civil::date(2024, 1, 6));
        assert!(d1 < d2);
        assert_eq!(d1, d1.clone());
        let n1 = BarDate::Naive(civil::date(2024, 1, 5).at(9, 30, 0, 0));
        let n2 = BarDate::Naive(civil::date(2024, 1, 5).at(10, 0, 0, 0));
        assert!(n1 < n2);
        let z = civil::date(2024, 1, 5)
            .at(0, 0, 0, 0)
            .to_zoned(ny())
            .unwrap();
        let a = BarDate::At(z.clone());
        // Python: date == datetime is False and ordering raises; naive
        // against aware likewise.
        for (x, y) in [(&d1, &n1), (&d1, &a), (&n1, &a)] {
            assert_ne!(x, y);
            assert_eq!(x.partial_cmp(y), None);
            assert_eq!(y.partial_cmp(x), None);
        }
        // Across zones Python compares instants, as here.
        assert_eq!(a, BarDate::At(z.with_time_zone(TimeZone::UTC)));
    }

    #[test]
    fn bar_date_by_instant_in_a_repeated_hour() {
        let edt = civil::date(2024, 11, 3)
            .at(1, 30, 0, 0)
            .to_zoned(ny())
            .unwrap();
        let est = edt
            .checked_add(jiff::SignedDuration::from_hours(1))
            .unwrap();
        assert_eq!(edt.datetime(), est.datetime());
        let (edt, est) = (BarDate::At(edt), BarDate::At(est));
        // Python calls these equal: one tzinfo, so it compares the wall
        // fields and ignores fold. Here they are an hour apart.
        assert_ne!(edt, est);
        assert_eq!(edt.partial_cmp(&est), Some(Ordering::Less));
    }

    #[test]
    fn format_each_form() {
        let ny = ny();
        let f = |t: DateTimeArg| format_ib_datetime_in(t, &ny).unwrap();
        assert_eq!(f(DateTimeArg::None), "");
        assert_eq!(f("".into()), "");
        assert_eq!(
            f("20240105 09:30:00 US/Eastern".into()),
            "20240105 09:30:00 US/Eastern"
        );
        assert_eq!(f(String::from("x").into()), "x");
        // Pinned against ib_async with TZ=America/New_York.
        assert_eq!(f(civil::date(2024, 1, 5).into()), "20240106 04:59:59 UTC");
        assert_eq!(
            f(civil::date(2024, 1, 5).at(9, 30, 0, 0).into()),
            "20240105 14:30:00 UTC"
        );
        assert_eq!(
            f(civil::date(2024, 3, 10).at(2, 30, 0, 0).into()),
            "20240310 07:30:00 UTC"
        );
        assert_eq!(
            f(civil::date(2024, 11, 3).at(1, 30, 0, 0).into()),
            "20241103 05:30:00 UTC"
        );
        let ams = TimeZone::get("Europe/Amsterdam").unwrap();
        let z = civil::date(2024, 1, 5)
            .at(9, 30, 0, 0)
            .to_zoned(ams)
            .unwrap();
        assert_eq!(f(z.clone().into()), "20240105 08:30:00 UTC");
        assert_eq!(f(BarDate::At(z).into()), "20240105 08:30:00 UTC");
        assert_eq!(f(civil::date(999, 1, 5).into()), "09990106 04:56:01 UTC");
        // Python: OverflowError / ValueError past year 9999 in UTC.
        let late = format_ib_datetime_in(civil::date(9999, 12, 31).into(), &ny);
        assert!(matches!(late, Err(Error::Value(_))));
    }

    #[test]
    fn date_time_arg_none_is_not_empty_text() {
        let none = DateTimeArg::None;
        let empty = DateTimeArg::from("");
        assert!(matches!(none, DateTimeArg::None));
        assert!(matches!(&empty, DateTimeArg::Text(s) if s.is_empty()));
        assert_eq!(format_ib_datetime(none).unwrap(), "");
        assert_eq!(format_ib_datetime(empty).unwrap(), "");
    }

    #[test]
    fn time_t_today_in_a_dst_gap_lands_after_it() {
        let ny = ny();
        let now = civil::date(2024, 3, 10)
            .at(0, 30, 0, 0)
            .to_zoned(ny.clone())
            .unwrap();
        let t = TimeT::from(civil::time(2, 30, 0, 0));
        let z = t.to_zoned(now.timestamp(), &ny).unwrap();
        assert_eq!(z.datetime(), civil::date(2024, 3, 10).at(3, 30, 0, 0));
        assert_eq!(z.timestamp().to_string(), "2024-03-10T07:30:00Z");
    }

    #[test]
    fn time_t_across_a_fall_back_names_the_instant() {
        let ny = ny();
        let edt = civil::date(2024, 11, 3)
            .at(1, 30, 0, 0)
            .to_zoned(ny.clone())
            .unwrap();
        let est = edt
            .checked_add(jiff::SignedDuration::from_hours(1))
            .unwrap();
        let now = edt.timestamp();
        let a = TimeT::from(edt.clone()).to_zoned(now, &ny).unwrap();
        let b = TimeT::from(est).to_zoned(now, &ny).unwrap();
        // Same wall time, an hour apart: a wait for `b` lasts an hour.
        assert_eq!(a.datetime(), b.datetime());
        assert_eq!(b.timestamp().duration_since(a.timestamp()).as_secs(), 3600);
        let at = TimeT::from(now).to_zoned(now, &ny).unwrap();
        assert_eq!(at.timestamp(), now);
        assert_eq!(at.time_zone(), &TimeZone::UTC);
    }

    #[test]
    fn python_numbers() {
        for (s, f, i) in [
            ("1_0", Some(10.0), Some(10)),
            (" +1_000 ", Some(1000.0), Some(1000)),
            ("\u{3000}1\u{85}", Some(1.0), Some(1)),
            ("0_0", Some(0.0), Some(0)),
            ("00", Some(0.0), Some(0)),
            ("-0", Some(-0.0), Some(0)),
            ("1e1_0", Some(1e10), None),
            ("1e5", Some(1e5), None),
            ("1.0", Some(1.0), None),
            ("1.", Some(1.0), None),
            (".5", Some(0.5), None),
            ("Infinity", Some(f64::INFINITY), None),
            ("-inf", Some(f64::NEG_INFINITY), None),
            ("1e400", Some(f64::INFINITY), None),
            ("9223372036854775808", Some(9.223372036854776e18), None),
            (
                "-9223372036854775808",
                Some(-9.223372036854776e18),
                Some(i64::MIN),
            ),
            ("1__0", None, None),
            ("_1", None, None),
            ("1_", None, None),
            ("1_.5", None, None),
            ("1._5", None, None),
            ("1_e5", None, None),
            ("in_f", None, None),
            ("\u{1c}1", None, None),
            ("-", None, None),
            ("", None, None),
            (".", None, None),
            ("1.5e", None, None),
            ("0x10", None, None),
            ("1 2", None, None),
        ] {
            assert_eq!(py_float(s), f, "float({s:?})");
            assert_eq!(py_int(s), i, "int({s:?})");
        }
        assert!(py_float("-nan").unwrap().is_nan());
        assert!(py_float("NaN").unwrap().is_nan());
    }

    #[test]
    fn dynamic_values_parse_as_ib_async_does() {
        use DynamicValue::{Float, Int, Str};
        // Each expectation is ib_async's `float()` then `int()` on the text.
        for (text, want) in [
            ("1", Int(1)),
            ("+12", Int(12)),
            (" 7 ", Int(7)),
            ("-0", Int(0)),
            ("1_000", Int(1000)),
            ("1.5", Float(1.5)),
            ("1e3", Float(1000.0)),
            ("-2.5E-3", Float(-0.0025)),
            ("inf", Float(f64::INFINITY)),
            ("9223372036854775808", Float(9.223372036854776e18)),
            ("abc", Str("abc".into())),
            ("", Str(String::new())),
            ("0x10", Str("0x10".into())),
        ] {
            assert_eq!(py_number(text), want, "{text:?}");
        }
        assert!(matches!(py_number("nan"), Float(f) if f.is_nan()));
        assert_eq!(
            py_number("9223372036854775807"),
            Int(9_223_372_036_854_775_807)
        );
    }

    #[test]
    fn zones_ib_sends_are_found_in_the_bundled_database() {
        // `US/Eastern` is a link that a system's database may lack.
        let BarDate::At(z) = parse_ib_datetime("20240105 09:30:00 US/Eastern").unwrap() else {
            panic!("a zone makes an aware datetime")
        };
        assert_eq!(z.timestamp().to_string(), "2024-01-05T14:30:00Z");
        assert!(zone("Nowhere/Else").is_err());
    }

    #[test]
    fn strptime_takes_cpythons_patterns_and_texts() {
        let t = strptime("20240105:0930", "%Y%m%d:%H%M").unwrap();
        assert_eq!(t, civil::date(2024, 1, 5).at(9, 30, 0, 0));
        // `%d` and `%H` take a space before one digit, as CPython's do.
        let t = strptime("2024015: 930", "%Y%m%d:%H%M").unwrap();
        assert_eq!(t, civil::date(2024, 1, 5).at(9, 30, 0, 0));
        // Each text is Python 3.14's.
        for (s, format, want) in [
            (
                "00000101:0930",
                "%Y%m%d:%H%M",
                "year must be in 1..9999, not 0",
            ),
            (
                "20240230:0930",
                "%Y%m%d:%H%M",
                "day 30 must be in range 1..29 for month 2 in year 2024",
            ),
            (
                "2024013109:30:60",
                "%Y%m%d%H:%M:%S",
                "second must be in 0..59, not 60",
            ),
            (
                "2024013124:30:00",
                "%Y%m%d%H:%M:%S",
                "time data '2024013124:30:00' does not match format '%Y%m%d%H:%M:%S'",
            ),
            (
                "20240105:09301",
                "%Y%m%d:%H%M",
                "unconverted data remains: 1",
            ),
        ] {
            let Err(Error::Value(m)) = strptime(s, format) else {
                panic!("{s} parsed")
            };
            assert_eq!(m, want, "{s}");
        }
        let Err(Error::Value(m)) = parse_ib_datetime("20241305") else {
            panic!()
        };
        assert_eq!(m, "month must be in 1..12, not 13");
    }

    #[test]
    fn block_on_runs_a_future_to_its_end_or_its_deadline() {
        let _g = lock(&GLOBAL_ERRORS);
        let before = global_error_event().len();
        assert_eq!(block_on(async { 7 }, None).unwrap(), 7);
        // Woken from another thread.
        let (p, reply) = Pending::<i32>::new(None);
        let t = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            reply.send(Ok(3))
        });
        assert_eq!(block_on(p, None).unwrap().unwrap(), 3);
        assert!(t.join().unwrap());
        // At the deadline the future is dropped, and its waiter leaves.
        let (p, reply) = Pending::<i32>::new(None);
        let r = block_on(p, Some(Duration::from_millis(10)));
        assert!(matches!(r, Err(Error::Timeout)));
        assert!(!reply.send(Ok(1)));
        assert_eq!(global_error_event().len(), before);
    }

    #[test]
    fn block_on_fails_with_a_close_once_its_unit_ends() {
        let _g = lock(&GLOBAL_ERRORS);
        let before = global_error_event().len();
        let t = thread::spawn(|| block_on(std::future::pending::<()>(), None));
        while global_error_event().len() == before {
            thread::yield_now();
        }
        let t = unit(
            || {
                global_error_event().emit(&Error::Connection("Socket disconnect".to_owned()));
                thread::sleep(Duration::from_millis(30));
                assert!(!t.is_finished(), "the close is seen when its unit ends");
                t
            },
            |_| Error::NotConnected,
        )
        .unwrap();
        let r = t.join().unwrap();
        assert!(matches!(r, Err(Error::Connection(m)) if m == "Socket disconnect"));
        assert_eq!(global_error_event().len(), before);
    }

    #[test]
    fn block_on_the_owner_thread_fails_at_once() {
        thread::spawn(|| {
            set_on_owner(true);
            let r = block_on(async {}, None);
            assert!(matches!(r, Err(Error::Value(m)) if m == "This event loop is already running"));
        })
        .join()
        .unwrap();
    }
}
