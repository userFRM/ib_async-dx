#!/usr/bin/env python3
"""Every public name of ib_async 2.1.0, in Rust spelling: what ib_async-dx must carry.

    python scripts/parity.py --rust     # writes tests/parity_names.rs and tests/twins_send.rs

ib_async and eventkit are read from their source, by AST; neither is imported. Every name
goes through ``snake()``, the mapping the oracle's expectations are written in. A name
ib_async exports is either named in the generated files, so that a name the crate lacks is a
compile error, or listed below as excluded, with its reason. A name that is neither stops
the script.

The generated files, compiled with the crate's tests:

``tests/parity_names.rs``
    The 133 callables of ``IB``, each called on an ``&IB`` as ib_async code calls
    ``ib.x(..)``; its events and the per-object events on values reached through it; every
    field of every mapped type, through a borrowed value; the constructors, the members of
    every exported class, ``Client``, ``util``, eventkit's ``Event``, the ticker helpers and
    the extras beyond ib_async; typed signatures of a representative set; and a test of every
    default with an ib_async counterpart.
``tests/twins_send.rs``
    Every twin's future, the extras' twins and the ``time_range_async`` stream are ``Send``.

A call's arguments are placeholders of the parameter types the crate's own source declares:
they make the call compile, and the signatures below are what pins types.
"""

import argparse
import ast
import importlib.metadata
import importlib.util
import json
import math
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IB_ASYNC_VERSION = "2.1.0"

RUST_KEYWORDS = frozenset(
    "as async await break const continue crate dyn else enum extern false fn for gen if impl in "
    "let loop match mod move mut pub ref return self static struct super trait true type unsafe "
    "use where while abstract become box do final macro override priv try typeof unsized virtual "
    "yield".split()
)


def snake(name):
    """The parity mapping from an ib_async name to its Rust name."""
    s = name.replace("PnL", "Pnl").replace("PNL", "Pnl")
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    s = re.sub(r"(?<=[A-Za-z])(?=[0-9])", "_", s)
    s = re.sub(r"(?<=[0-9])(?=[A-Za-z])", "_", s).lower()
    return s + "_" if s in RUST_KEYWORDS else s


# ---------------------------------------------------------------- what is not mapped, and why

EXCLUDED = {
    # Exported names.
    "IBC": "launches and supervises a TWS or gateway process; the engine has none",
    "Watchdog": "supervises a TWS or gateway process; session recovery is the engine's",
    "Wrapper": "built inside IB with no way to replace it; its callbacks are the engine's, "
    "implemented in full (scripts/check_record.py), and its fields map below",
    "__version_info__": "a tuple of VERSION's parts; parse VERSION",
    # IB members.
    "IB.wrapper": "excluded with Wrapper",
    # Client (ib.client).
    "Client.send": "writes a gateway's API socket, which the engine does not speak",
    "Client.sendMsg": "writes a gateway's API socket",
    "Client.setConnectOptions": "part of a gateway's API\\0 handshake",
    "Client.startApi": "part of a gateway's API\\0 handshake",
    "Client.verifyRequest": "part of a gateway's API handshake",
    "Client.verifyMessage": "part of a gateway's API handshake",
    "Client.verifyAndAuthRequest": "part of a gateway's API handshake",
    "Client.verifyAndAuthMessage": "part of a gateway's API handshake",
    "Client.MinClientVersion": "a gateway protocol's version range",
    "Client.MaxClientVersion": "a gateway protocol's version range",
    "Client.optCapab": "sent in a gateway's handshake",
    "Client.connectOptions": "sent in a gateway's handshake",
    "Client.host": "the venue names the server; EClientConfig.host is only where a logon knocks",
    "Client.port": "the venue names the server",
    "Client.wrapper": "excluded with Wrapper",
    "Client.decoder": "decodes a gateway socket",
    "Client.conn": "a gateway socket",
    "Client.MaxRequests": "throttles a gateway socket; the engine paces its own wire",
    "Client.RequestsInterval": "throttles a gateway socket",
    "Client.throttleStart": "throttles a gateway socket",
    "Client.throttleEnd": "throttles a gateway socket",
    # Members of exported classes.
    "Contract.create": "picks a subclass by secType; Contract is one type whose kind is sec_type",
    "Contract.recreate": "picks a subclass by secType",
    "Ticker.created": "__post_init__'s copy guard, not data",
    "SoftDollarTier.__bool__": "Rust has no truthiness; compare with SoftDollarTier::default()",
    "FlexReport.df": "pandas",
    "Tickfilter.on_source": "eventkit's operator plumbing",
    "Midpoints.on_source": "eventkit's operator plumbing",
    "TimeBars.on_source": "eventkit's operator plumbing",
    "TickBars.on_source": "eventkit's operator plumbing",
    "VolumeBars.on_source": "eventkit's operator plumbing",
    "*.dict": "reflection over a dataclass; fields are public",
    "*.tuple": "reflection over a dataclass",
    "*.update": "reflection over a dataclass; struct update syntax updates",
    # Wrapper's fields that are its own bookkeeping.
    "Wrapper.ib": "the Wrapper's back reference",
    "Wrapper.reqId2Ticker": "request bookkeeping; ticker(contract) finds a ticker",
    "Wrapper.ticker2ReqId": "request bookkeeping",
    "Wrapper.permId2Trade": "bookkeeping; trades filter by perm_id",
    "Wrapper.pnlKey2ReqId": "request bookkeeping",
    "Wrapper.pnlSingleKey2ReqId": "request bookkeeping",
    "Wrapper.wshMetaReqId": "request bookkeeping",
    "Wrapper.wshEventReqId": "request bookkeeping",
    "Wrapper.lastTime": "each updated ticker's time is its pass's arrival; set_timeout detects idleness",
    "Wrapper.time": "each updated ticker's time is its pass's arrival",
    "Wrapper.defaults": "what was passed to IB::with",
    # util.
    "util.isNan": "f64::is_nan",
    "util.logToFile": "a Rust program chooses its log backend; the crate logs to ib_async.*",
    "util.logToConsole": "a Rust program chooses its log backend",
    "util.df": "pandas",
    "util.barplot": "matplotlib",
    "util.dataclassAsDict": "reflection over a dataclass; fields are public",
    "util.dataclassAsTuple": "reflection over a dataclass",
    "util.dataclassNonDefaults": "reflection over a dataclass",
    "util.dataclassUpdate": "reflection over a dataclass; struct update syntax updates",
    "util.dataclassRepr": "reflection over a dataclass; Debug prints",
    "util.isnamedtupleinstance": "reflection over a NamedTuple",
    "util.tree": "reflection over a dataclass",
    "util.allowCtrlC": "asyncio loop plumbing; delivery runs on the owner thread",
    "util.patchAsyncio": "asyncio loop plumbing; a blocking call from a handler fails fast",
    "util.getLoop": "asyncio loop plumbing",
    "util.startLoop": "asyncio loop plumbing",
    "util.useQt": "Qt loop plumbing",
    "util.timeit": "std::time::Instant",
    # eventkit's Event.
    "Event.__bool__": "always true",
    "Event.run": "loop plumbing, with no asyncio loop to run",
    "Event.disconnect_obj": "handlers are closures, disconnected by HandlerId",
    "Event.__getitem__": "an operator; Subscription is an Iterator and a Stream",
    "Event.__reduce__": "pickling",
    "Event.__or__": "pipe",
    "Event.pipe": "operator plumbing; a derived event is Event::new fed by a handler",
    "Event.fork": "operator plumbing",
    "Event.set_source": "operator plumbing",
    "Event.init": "eventkit's own constructor plumbing",
    "Event.create": "eventkit's own constructor plumbing",
    "Event.wait": "a constructor for an asyncio loop",
    "Event.aiterate": "a constructor for an asyncio loop",
    "Event.sequence": "a constructor for an asyncio loop",
    "Event.repeat": "a constructor for an asyncio loop",
    "Event.range": "a constructor for an asyncio loop",
    "Event.timerange": "a constructor for an asyncio loop",
    "Event.timer": "a constructor; timebars takes any Event<Zoned> a program emits",
    "Event.marble": "a constructor for an asyncio loop",
    "Event.NO_VALUE": "value() gives None",
}

# eventkit's operator library, mapped to std's iterator adapters or a Stream extension
# crate's over a Subscription, not ported.
EVENT_OPERATORS = frozenset(
    "filter skip take takewhile dropwhile takeuntil constant iterate count enumerate timestamp "
    "partial partial_right star pack pluck map emap mergemap concatmap chainmap switchmap reduce "
    "min max sum product mean any all ema previous pairwise changes unique last list deque array "
    "chunk chunkwith chain merge concat switch zip ziplatest delay timeout throttle debounce copy "
    "deepcopy sample errors end_on_error".split()
)

# ---------------------------------------------------------------- where a name maps, when not snake()

# Python protocol methods: Rust traits, or the constructors and Drop.
PROTOCOL = {
    "__eq__": "PartialEq",
    "__hash__": "PartialEq",  # with __eq__; X9: Contract is PartialEq, not Hash
    "__repr__": "Debug",
    "__str__": "Debug",
    "__init__": None,  # the constructors below, and Ticker::new
    "__post_init__": None,
    "__enter__": None,  # Drop for IB
    "__exit__": None,
    "__del__": None,
    "__add__": "Add",
    "__sub__": "Sub",
    "__mul__": "Mul",
}

IB_ASSOCIATED = {"schedule", "sleep", "timeRange", "timeRangeAsync", "waitUntil", "oneCancelsAll"}

# util's names: its loop functions are IB's (IB aliases them as static methods).
UTIL = {
    "formatSI": "util::format_si",
    "formatIBDatetime": "util::format_ib_datetime",
    "parseIBDatetime": "util::parse_ib_datetime",
    "UNSET_INTEGER": "util::UNSET_INTEGER",
    "UNSET_DOUBLE": "util::UNSET_DOUBLE",
    "EPOCH": "util::EPOCH",
    "Time_t": "util::TimeT",
    "globalErrorEvent": "util::global_error_event",
    "run": "IBHandle::run_until",
    "schedule": "IB::schedule",
    "sleep": "IB::sleep",
    "timeRange": "IB::time_range",
    "timeRangeAsync": "IB::time_range_async",
    "waitUntil": "IB::wait_until",
    "waitUntilAsync": "IB::wait_until_async",
}

# The Wrapper's fields that a program reads, and the accessor that gives each.
WRAPPER_FIELDS = {
    "accountValues": "account_values",
    "acctSummary": "account_summary",
    "portfolio": "portfolio",
    "positions": "positions",
    "trades": "trades",
    "fills": "fills",
    "newsTicks": "news_ticks",
    "tickers": "tickers",
    "pendingTickers": "pending_tickers",
    "msgId2NewsBulletin": "news_bulletins",
    "reqId2Subscriber": "realtime_bars",
    "reqId2PnL": "pnl",
    "reqId2PnlSingle": "pnl_single",
    "accounts": "managed_accounts",
    "clientId": "client().client_id",
}

# Exported names that are not a type of the same name.
EXPORTS = {
    "Event": "Event::<()>",
    "util": "util",
    "IB": "IB",
    "Client": "Client",
    "StartupFetch": "StartupFetch",
    "StartupFetchALL": "StartupFetch::ALL",
    "StartupFetchNONE": "StartupFetch::NONE",
    "RequestError": "Error::Request",
    "FlexError": "Error::Flex",
    "FlexReport": "flex::FlexReport",
    "__version__": "VERSION",
}
FLEX_ONLY = {"FlexError", "FlexReport"}

# Subclasses that are a constructor of their base in Rust.
CONTRACT_KINDS = "Stock Option Future ContFuture Forex Index CFD Commodity Bond FuturesOption MutualFund Warrant Bag Crypto".split()
ORDER_KINDS = {"LimitOrder": "limit", "MarketOrder": "market", "StopOrder": "stop", "StopLimitOrder": "stop_limit"}

# Methods of subclasses that are the base's in Rust.
MEMBER_HOME = {"Forex.pair": "Contract"}

# ib_async's ticker helpers, which ticker.py defines and does not export.
HELPERS = ["TickerUpdateEvent", "Tickfilter", "Midpoints", "Bar", "BarList", "TimeBars", "TickBars", "VolumeBars"]
HELPER_TYPES = {
    "TickerUpdateEvent": "Event<Live<Ticker>>",
    "Tickfilter": "TickFilter",
    "Midpoints": "TickFilter",
}

# The list types, whose items are a field in Rust.
LIST_ITEMS = {"BarDataList": "bars", "RealTimeBarList": "bars", "ScanDataList": "data", "BarList": "bars"}

# Types whose ib_async equality is identity: equality is the Live handle's.
IDENTITY_LIVE = {"Order", "Ticker", "BarDataList", "RealTimeBarList", "ScanDataList", "BarList"}

# The Python names of the methods beyond ib_async's API: 19 and 3 twins.
EXTRAS = (
    "reqMktDataEx reqCurrentTimeInMillis reqCurrentTimeInMillisAsync reqCorporateActions "
    "reqCorporateActionsAsync reqSpreadScan reqSpreadScanAsync tickerExtras optionModel "
    "closingOptionModel companyData enabledFeatures orderPermissions permittedOrderTypes "
    "algorithms algorithmsFor orderPresets positionsElsewhere accountValuesElsewhere "
    "competingSession reqPing lastRtt"
).split()

# Placeholders for the parameters of the generic functions the crate declares.
GENERIC_ARGS = {
    ("IBHandle", "loop_until"): ["|| None::<()>", "any()"],
    ("IBHandle", "run_until"): ["std::future::ready(())", "any()"],
    ("IB", "schedule"): ["Timestamp::UNIX_EPOCH", "|| {}"],
    ("OrderState", "transform"): ["|_| ()"],
}
IMPL_INTO = {"DateTimeArg": '""', "TimeT": "Timestamp::UNIX_EPOCH", "String": '""', "f64": "0.0"}

# Typed signatures: every twin kind, every kind of request and every method
# whose timeout has a default. A changed parameter or result fails to compile.
SIGNATURES = [
    # Connection and loop.
    "fn(&IBHandle, ConnectOptions) -> Result<()> = IBHandle::connect",
    "fn(&IBHandle) -> Option<String> = IBHandle::disconnect",
    "fn(&IBHandle) -> bool = IBHandle::is_connected",
    "fn(&IBHandle, Option<Duration>) -> Result<bool> = IBHandle::wait_on_update",
    "fn(&IBHandle, Option<Duration>) = IBHandle::set_timeout",
    "fn(&IBHandle) -> Result<()> = IBHandle::run",
    "fn(Duration) -> Result<bool> = IB::sleep",
    "fn(Timestamp) -> Result<bool> = IB::wait_until",
    "fn(Timestamp, fn()) -> Result<TimerHandle> = IB::schedule",
    "fn(Timestamp, Timestamp, Duration) -> Result<impl Stream<Item = Zoned> + Send> = IB::time_range_async",
    "fn(&IBHandle, ConnectOptions) -> impl Future<Output = Result<()>> + Send = IBHandle::connect_async",
    # State reads.
    "fn(&IBHandle, &str) -> Vec<AccountValue> = IBHandle::account_values",
    "fn(&IBHandle) -> Vec<Live<Trade>> = IBHandle::trades",
    "fn(&IBHandle) -> Vec<Live<Order>> = IBHandle::open_orders",
    "fn(&IBHandle, &Contract) -> Result<Option<Live<Ticker>>> = IBHandle::ticker",
    "fn(&IBHandle) -> Vec<Bars> = IBHandle::realtime_bars",
    # Orders.
    "fn(&IBHandle, &Contract, &Live<Order>) -> Result<Live<Trade>> = IBHandle::place_order",
    "fn(&IBHandle, &Contract, &Order) -> Result<OrderState> = IBHandle::what_if_order",
    "fn(&IBHandle, &Contract, &Order) -> Pending<OrderState> = IBHandle::what_if_order_async",
    "fn(&IBHandle) -> Result<Vec<Live<Trade>>> = IBHandle::req_open_orders",
    "fn(&IBHandle) -> Pending<Vec<Live<Trade>>> = IBHandle::req_open_orders_async",
    "fn(&IBHandle, Option<&ExecutionFilter>) -> Result<Vec<Fill>> = IBHandle::req_executions",
    "fn(&IBHandle) -> Result<()> = IBHandle::req_global_cancel",
    # Account.
    "fn(&IBHandle, &str) -> Result<()> = IBHandle::req_account_updates",
    "fn(&IBHandle, &str) -> Pending<()> = IBHandle::req_account_updates_async",
    "fn(&IBHandle) -> Result<Vec<Position>> = IBHandle::req_positions",
    "fn(&IBHandle, &str, &str) -> Result<Live<PnL>> = IBHandle::req_pnl",
    "fn(&IBHandle, &str) -> Result<Vec<AccountValue>> = IBHandle::account_summary",
    "fn(&IBHandle, &str) -> impl Future<Output = Result<Vec<AccountValue>>> + Send = IBHandle::account_summary_async",
    # Market data.
    "fn(&IBHandle, &Contract, &str, bool, bool, &[TagValue]) -> Result<Live<Ticker>> = IBHandle::req_mkt_data",
    "fn(&IBHandle, &Contract) -> Result<bool> = IBHandle::cancel_mkt_data",
    "fn(&IBHandle, i32) -> Result<Option<Vec<PriceIncrement>>> = IBHandle::req_market_rule",
    "fn(&IBHandle, i32) -> impl Future<Output = Result<Option<Vec<PriceIncrement>>>> + Send = IBHandle::req_market_rule_async",
    "fn(&IBHandle, &Contract, f64, f64, &[TagValue]) -> Result<Option<OptionComputation>> = IBHandle::calculate_implied_volatility",
    "fn(&IBHandle, &Contract, i32, &str, bool, &[TagValue]) -> Result<Live<RealTimeBarList>> = IBHandle::req_real_time_bars",
    # Reference data.
    "fn(&IBHandle, &mut [Contract]) -> Result<Vec<Qualified>> = IBHandle::qualify_contracts",
    "fn(&IBHandle, &mut [Contract], bool) -> impl Future<Output = Result<Vec<Qualified>>> + Send = IBHandle::qualify_contracts_async",
    "fn(&IBHandle, &Contract) -> Result<Vec<ContractDetails>> = IBHandle::req_contract_details",
    "fn(&IBHandle, &Contract) -> Pending<Vec<ContractDetails>> = IBHandle::req_contract_details_async",
    "fn(&IBHandle, &str) -> Result<Option<Vec<ContractDescription>>> = IBHandle::req_matching_symbols",
    "fn(&IBHandle, &Contract, String, &str, &str, &str, bool, i32, bool, &[TagValue], Option<Duration>) -> Result<Live<BarDataList>> = IBHandle::req_historical_data",
    "fn(&IBHandle, &Contract, &str, bool, i32) -> Result<BarDate> = IBHandle::req_head_time_stamp",
    "fn(&IBHandle, &Live<ScanDataList>) -> Result<()> = IBHandle::cancel_scanner_subscription",
    "fn(&IBHandle) -> Result<Zoned> = IBHandle::req_current_time",
    "fn(&IBHandle) -> Pending<Zoned> = IBHandle::req_current_time_async",
    "fn(&IBHandle, i32) -> Result<Option<String>> = IBHandle::request_fa",
    # Extras.
    "fn(&IBHandle, &Contract, &str, &str, Option<Duration>) -> Result<Vec<CorporateAction>> = IBHandle::req_corporate_actions",
    "fn(&IBHandle, &Contract, &SpreadScan, Option<Duration>) -> Result<Vec<ScannedStrategy>> = IBHandle::req_spread_scan",
    "fn(&IBHandle) -> Pending<i64> = IBHandle::req_current_time_in_millis_async",
    # Client.
    "fn(&Client, EClientConfig, i64, Option<Duration>) -> Result<()> = Client::connect",
    "fn(&Client) -> Result<i64> = Client::get_req_id",
    "fn(&Client) -> ConnState = Client::conn_state",
]

# ---------------------------------------------------------------- reading ib_async and eventkit


def package_dir(name):
    spec = importlib.util.find_spec(name)
    if spec is None or not spec.submodule_search_locations:
        sys.exit(f"parity: {name} is not installed")
    return Path(next(iter(spec.submodule_search_locations)))


def parse(path):
    return ast.parse(path.read_text(), filename=str(path))


def classes(tree):
    return {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}


def is_classvar(node):
    return "ClassVar" in ast.unparse(node.annotation)


def methods(cls):
    """Public and protocol methods, and the methods a class attribute aliases."""
    out = []
    for b in cls.body:
        if isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out.append(b.name)
        elif isinstance(b, ast.Assign) and isinstance(b.targets[0], ast.Name):
            if isinstance(b.value, ast.Name) or ast.unparse(b.value).startswith("staticmethod("):
                out.append(b.targets[0].id)
    return [m for m in out if not m.startswith("_") or m.startswith("__")]


def classvars(cls):
    out = []
    for b in cls.body:
        if isinstance(b, ast.AnnAssign) and is_classvar(b):
            out.append(b.target.id)
        elif isinstance(b, ast.Assign):
            for t in b.targets:
                for e in t.elts if isinstance(t, ast.Tuple) else [t]:
                    if isinstance(e, ast.Name) and not e.id.startswith("_"):
                        if not (isinstance(b.value, ast.Name) or ast.unparse(b.value).startswith("staticmethod(")):
                            out.append(e.id)
    return out


def self_attrs(fn):
    out = []
    for x in ast.walk(fn):
        targets = x.targets if isinstance(x, ast.Assign) else [x.target] if isinstance(x, ast.AnnAssign) else []
        for t in targets:
            for e in t.elts if isinstance(t, ast.Tuple) else [t]:
                if isinstance(e, ast.Attribute) and isinstance(e.value, ast.Name) and e.value.id == "self":
                    if not e.attr.startswith("_") and e.attr not in out:
                        out.append(e.attr)
    return out


class Model:
    """The ib_async sources, as the generator reads them."""

    def __init__(self):
        pkg = package_dir("ib_async")
        version = importlib.metadata.version("ib_async")
        if version != IB_ASYNC_VERSION:
            sys.exit(f"parity: ib_async {version} is installed; the crate follows {IB_ASYNC_VERSION}")
        self.trees = {p.stem: parse(p) for p in pkg.glob("*.py")}
        self.classes = {}
        self.module_of = {}
        for mod, tree in self.trees.items():
            for name, node in classes(tree).items():
                self.classes.setdefault(name, node)
                self.module_of.setdefault(name, mod)
        self.event = classes(parse(package_dir("eventkit") / "event.py"))["Event"]
        init = self.trees["__init__"]
        self.all = next(
            ast.literal_eval(n.value)
            for n in init.body
            if isinstance(n, ast.Assign) and ast.unparse(n.targets[0]) == "__all__"
        )

    def kind(self, name):
        """'dataclass', 'namedtuple', 'list', or None."""
        c = self.classes[name]
        if any(ast.unparse(d).startswith("dataclass") for d in c.decorator_list):
            return "dataclass"
        bases = [ast.unparse(b) for b in c.bases]
        if "NamedTuple" in bases:
            return "namedtuple"
        if any(b.startswith("list[") for b in bases):
            return "list"
        return None

    def fields(self, name):
        """(name, default node or None) in dataclass order, a base's first; overrides in place."""
        c = self.classes[name]
        out = {}
        for b in c.bases:
            base = ast.unparse(b)
            if base in self.classes and self.kind(base) == "dataclass":
                out.update(self.fields(base))
        for b in c.body:
            if isinstance(b, ast.AnnAssign) and isinstance(b.target, ast.Name) and not is_classvar(b):
                prev = out.get(b.target.id)
                out[b.target.id] = b.value if b.value is not None else prev
        return out

    def ib_class(self):
        return self.classes["IB"]


# ---------------------------------------------------------------- reading the crate's signatures


def split_top(s, sep=","):
    parts, depth, cur = [], 0, ""
    for ch in s.replace("->", "→"):
        depth += ch in "([<"
        depth -= ch in ")]>"
        if ch == sep and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return [p.strip().replace("→", "->") for p in parts if p.strip()]


def closing(s, i, open_="(", close=")"):
    depth = 0
    s2 = s.replace("->", "→ ")
    for j in range(i, len(s2)):
        depth += s2[j] == open_
        depth -= s2[j] == close
        if depth == 0:
            return j
    raise ValueError("unbalanced")


class Rust:
    """The crate's public functions, by the type whose impl block holds them, and its structs."""

    def __init__(self):
        self.fns = {}
        self.structs = {}
        for path in sorted((ROOT / "src").rglob("*.rs")):
            if "tests" in path.relative_to(ROOT / "src").parts:
                continue
            text = path.read_text()
            text = re.sub(r"(?m)^\s*//.*$", "", text)
            impls = [(m.start(), m.group(1)) for m in re.finditer(r"(?m)^impl(?:<[^{]*?>)? ([^{]+?) \{", text)]
            for m in re.finditer(r"(?m)^( *)pub (async )?fn (\w+)", text):
                owner = None
                if m.group(1):
                    before = [t for at, t in impls if at < m.start()]
                    if not before or " for " in before[-1]:
                        continue
                    owner = before[-1].strip()
                i = m.end()
                generics = ""
                if text[i] == "<":
                    j = closing(text, i, "<", ">")
                    generics, i = text[i + 1 : j], j + 1
                j = closing(text, i)
                params = split_top(" ".join(text[i + 1 : j].split()))
                owner = owner or path.stem
                self.fns.setdefault((owner, m.group(3)), (params, generics, bool(m.group(2))))
            for m in re.finditer(r"(?m)^pub struct (\w+)(?:<([^{]*?)>)? \{\n(.*?)^\}", text, re.S):
                # A parameter's default stands for it: `OrderStateNumeric<U = Option<f64>>`.
                given = dict(g.split(" = ", 1) for g in split_top(m.group(2) or "") if " = " in g)
                fields = {}
                for f in re.finditer(r"(?m)^    pub (\w+): (.+),$", m.group(3)):
                    fields[f.group(1)] = given.get(f.group(2), f.group(2))
                self.structs[m.group(1)] = fields

    def args(self, owner, name):
        """The call's receiver kind and placeholder arguments, or None when the crate has no such
        function: the call is then written with none, and fails to compile."""
        key = (owner, name)
        if key not in self.fns:
            return None, []
        params, generics, _ = self.fns[key]
        recv = None
        if params and "self" in params[0] and ":" not in params[0]:
            recv, params = params[0], params[1:]
        if key in GENERIC_ARGS:
            return recv, GENERIC_ARGS[key]
        out = []
        for p in params:
            ty = p.split(":", 1)[1].strip()
            m = re.match(r"impl Into<(\w+)>", ty)
            fnm = re.match(r"impl Fn(?:Once|Mut)?\((.*?)\)", ty)
            if m:
                out.append(IMPL_INTO[m.group(1)])
            elif fnm:
                n = len(split_top(fnm.group(1)))
                out.append("|" + ", ".join("_" * 1 for _ in range(n)) + "| any()")
            elif re.fullmatch(r"impl AsRef<Path>", ty):
                out.append('""')
            else:
                out.append("any()")
        return recv, out


def call(rust, owner, name, receiver):
    """A call of `owner::name`, on `receiver` when it takes self."""
    recv, args = rust.args(owner, name)
    a = ", ".join(args)
    if recv is None and (owner, name) in rust.fns:
        path = owner.split("<")[0]
        return f"{path}::{name}({a})"
    if recv in ("self", "mut self"):
        return f"any::<{owner}>().{name}({a})"
    return f"{receiver}.{name}({a})"


# ---------------------------------------------------------------- defaults


def default_of(node, model):
    """What an AST default states, as (kind, value)."""
    if node is None:
        return None
    if isinstance(node, ast.Constant):
        v = node.value
        if v is None:
            return ("none", None)
        if isinstance(v, bool):
            return ("bool", v)
        if isinstance(v, (int, float)):
            return ("num", v)
        if isinstance(v, str):
            return ("str", v)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        inner = default_of(node.operand, model)
        if inner and inner[0] == "num":
            return ("num", -inner[1])
    text = ast.unparse(node)
    if text in ("nan", "float('nan')", "math.nan"):
        return ("nan", None)
    if text in ("UNSET_DOUBLE", "UNSET_INTEGER", "util.UNSET_DOUBLE", "util.UNSET_INTEGER"):
        return ("unset", None)
    if text in ("EPOCH", "util.EPOCH"):
        return ("epoch", None)
    if text in ("timezone.utc", "dt.timezone.utc", "datetime.timezone.utc"):
        return ("utc", None)
    if isinstance(node, ast.Call) and ast.unparse(node.func) == "field":
        for k in node.keywords:
            if k.arg == "default":
                return default_of(k.value, model)
            if k.arg == "default_factory":
                f = ast.unparse(k.value)
                if f in ("list", "dict", "set", "deque"):
                    return ("empty", None)
                if f in model.classes:
                    return ("default", f)
    if isinstance(node, (ast.List, ast.Dict)) and not (node.elts if isinstance(node, ast.List) else node.keys):
        return ("empty", None)
    raise SystemExit(f"parity: no Rust form for the default {text}")


def rust_literal(v, ty):
    if ty in ("f64", "Option<f64>"):
        return repr(float(v)) if math.isfinite(v) else "f64::NAN"
    return str(int(v))


def assertion(expr, ty, d):
    """An assertion that `expr`, a field of Rust type `ty`, holds ib_async's default `d`."""
    kind, v = d
    opt = ty.startswith("Option<")
    say = json.dumps(expr)
    if kind in ("none", "unset"):
        return f"assert!({expr}.is_none(), {say});"
    if kind == "nan" and opt:
        return f"assert!({expr}.is_some_and(f64::is_nan), {say});"
    if kind == "nan":
        return f"assert!({expr}.is_nan(), {say});"
    if kind == "empty":
        return f"assert!({expr}.is_empty(), {say});"
    if kind == "epoch":
        if ty == "BarDate":
            return f"assert!(matches!(&{expr}, BarDate::At(t) if t.timestamp() == Timestamp::UNIX_EPOCH), {say});"
        return f"assert_eq!({expr}.timestamp(), Timestamp::UNIX_EPOCH, {say});"
    if kind == "utc":
        return f"assert_eq!({expr}, TimeZone::UTC, {say});"
    if kind == "default":
        inner = ty[len("Live<") : -1] if ty.startswith("Live<") else ty
        return f'assert_eq!(format!("{{:?}}", {expr}), format!("{{:?}}", {inner}::default()), {say});'
    if kind == "str":
        if opt:
            return f"assert_eq!({expr}.as_deref(), Some({json.dumps(v)}), {say});"
        return f"assert_eq!({expr}, {json.dumps(v)}, {say});"
    if kind == "bool":
        return f"assert!({'' if v else '!'}{expr}, {say});"
    if kind == "num":
        lit = rust_literal(v, ty)
        if opt:
            return f"assert_eq!({expr}, Some({lit}), {say});"
        return f"assert_eq!({expr}, {lit}, {say});"
    raise SystemExit(f"parity: no assertion for {kind}")


# ---------------------------------------------------------------- generation


class Out:
    def __init__(self):
        self.lines = []

    def __call__(self, s=""):
        self.lines.append(s)

    def text(self):
        return "\n".join(self.lines) + "\n"


class Unmapped(Exception):
    pass


def generate(model, rust):
    ib = model.ib_class()
    o = Out()
    missing = []

    def need(name):
        if name not in EXCLUDED:
            missing.append(name)

    o("//! ib_async 2.1.0's public names in Rust spelling, generated from ib_async's source by")
    o("//! `scripts/parity.py --rust`. Do not edit: regenerate.")
    o("//!")
    o("//! A name the crate lacks fails to compile. The functions are compiled and never run, but")
    o("//! `defaults`, which holds every default with an ib_async counterpart to ib_async's value.")
    o("")
    o("#![allow(dead_code, unused_imports, clippy::unit_arg, clippy::eq_op, clippy::type_complexity)]")
    o("")
    o("use std::future::Future;")
    o("use std::path::Path;")
    o("use std::time::Duration;")
    o("")
    o("use ib_async_dx::defaults::{")
    o("    CLIENT_CONNECT_TIMEOUT, CORPORATE_ACTIONS_TIMEOUT, HISTORICAL_TIMEOUT, SET_TIMEOUT,")
    o("    SPREAD_SCAN_TIMEOUT,")
    o("};")
    o("use ib_async_dx::util::{self, BarDate, DateTimeArg, TimeT};")
    o("use ib_async_dx::*;")
    o("use jiff::tz::TimeZone;")
    o("use jiff::{Timestamp, Zoned};")
    o("")
    o("/// A value of any type, for arguments: never called.")
    o("fn any<T>() -> T {")
    o("    unreachable!()")
    o("}")
    o("")
    o("fn used<T>(_: T) {}")
    o("fn eq<T: PartialEq>() {}")
    o("fn debug_clone<T: std::fmt::Debug + Clone>() {}")
    o("fn default<T: Default>() {}")
    o("")

    # -- The 133.
    names = []
    for m in methods(ib):
        if m.startswith("__"):
            continue
        names.append(m)
    assert len(names) == 133, f"IB has {len(names)} callables, not 133"
    o("/// The 133 callables of `IB`, called as ib_async code calls them: `ib.x(..)` through")
    o("/// `Deref`, or `IB::x(..)` for what ib_async aliases from util or declares static.")
    o("fn callables(ib: &IB) {")
    for m in names:
        if m in IB_ASSOCIATED:
            o(f"    used({call(rust, 'IB', snake(m), 'ib')}); // {m}")
        else:
            o(f"    used({call(rust, 'IBHandle', snake(m), 'ib')}); // {m}")
    o(f"    used({call(rust, 'IB', 'new', 'ib')}); // IB()")
    o(f"    used({call(rust, 'IB', 'with', 'ib')}); // IB(defaults=..)")
    o(f"    used({call(rust, 'IB', 'handle', 'ib')});")
    o("    used(ib.client()); // ib.client")
    o("    used(IB::EVENTS); // IB.events")
    o("}")
    o("")

    # IB's other members.
    o("/// `IB`'s class attributes are `IBConfig`'s fields.")
    o("fn ib_config(c: &IBConfig) {")
    attrs = [b.target.id for b in ib.body if isinstance(b, ast.AnnAssign)]
    o("    used((" + ", ".join(f"&c.{snake(a)}" for a in attrs) + "));")
    o("}")
    o("")
    for a in self_attrs(next(b for b in ib.body if isinstance(b, ast.FunctionDef) and b.name == "__init__")):
        if a != "client":
            need(f"IB.{a}")

    # -- Events.
    events = ast.literal_eval(next(b.value for b in ib.body if isinstance(b, ast.Assign) and ast.unparse(b.targets[0]) == "events"))
    assert len(events) == 25, f"IB has {len(events)} events"
    trade = model.classes["Trade"]
    trade_events = ast.literal_eval(next(b.value for b in trade.body if isinstance(b, ast.AnnAssign) and ast.unparse(b.target) == "events"))
    ticker = model.classes["Ticker"]
    ticker_events = ast.literal_eval(next(b.value for b in ticker.body if isinstance(b, ast.AnnAssign) and ast.unparse(b.target) == "events"))
    list_events = {}
    for cls in ("BarDataList", "RealTimeBarList", "ScanDataList", "BarList"):
        init = next(b for b in model.classes[cls].body if isinstance(b, ast.FunctionDef) and b.name == "__init__")
        list_events[cls] = [a for a in self_attrs(init) if a.endswith("Event")]
    per_object = len(trade_events) + len(ticker_events) + sum(map(len, list_events.values()))
    assert per_object == 12, f"{per_object} per-object events"
    o("/// The 25 events of `IB` and the added `tick_event`, and the 12 of the objects reached")
    o("/// through it.")
    o("fn events(ib: &IB) {")
    for e in events + ("tickEvent",):
        o(f"    used(ib.{snake(e)}());")
    o("    for trade in ib.trades() {")
    for e in trade_events:
        o(f"        used(trade.{snake(e)}());")
    o("    }")
    o("    used(Trade::EVENTS);")
    o("    for ticker in ib.tickers() {")
    for e in ticker_events:
        o(f"        used(ticker.{snake(e)}());")
    o("        let bars = ticker.update_event().trades().tickbars(1);")
    for e in list_events["BarList"]:
        o(f"        used(bars.bars.{snake(e)}());")
    o("    }")
    o("    used(Ticker::EVENTS);")
    o("    for bars in ib.realtime_bars() {")
    o("        match bars {")
    for cls, variant in (("BarDataList", "Historical"), ("RealTimeBarList", "RealTime"), ("ScanDataList", "Scan")):
        for e in list_events[cls]:
            o(f"            Bars::{variant}(l) => used(l.{snake(e)}()),")
    o("        }")
    o("    }")
    o("}")
    o("")

    # -- Exported names, and the fields and members of each class.
    record_types = []
    o("/// Every exported name that is not a type of its own name.")
    o("fn exports() {")
    for name in model.all:
        if name in EXCLUDED:
            continue
        if name in EXPORTS:
            if name in FLEX_ONLY:
                o('    #[cfg(feature = "flex")]')
            path = EXPORTS[name]
            if name == "RequestError":
                o("    let _ = |e: Error| matches!(e, Error::Request { .. });")
            elif name == "FlexError":
                o("    let _ = |e: Error| matches!(e, Error::Flex(..));")
            elif name == "Event":
                o('    used(Event::<()>::new("event"));')
            elif name == "util":
                continue  # its names are below
            elif name in ("IB", "Client", "StartupFetch", "FlexReport"):
                o(f"    used(any::<{path}>());")
            else:
                o(f"    used({path});")
            continue
        if name in CONTRACT_KINDS:
            o(f"    used({call(rust, 'Contract', snake(name), 'c')}); // {name}")
            continue
        if name in ORDER_KINDS:
            o(f"    used({call(rust, 'Order', ORDER_KINDS[name], 'o')}); // {name}")
            continue
        if name in model.classes:
            record_types.append(name)
            continue
        need(name)
    o(f"    used({call(rust, 'Contract', 'news', 'c')}); // the crate's news contract")
    o("    used(StartupFetch::POSITIONS | StartupFetch::ORDERS_OPEN | StartupFetch::ORDERS_COMPLETE);")
    o("    used(StartupFetch::ACCOUNT_UPDATES | StartupFetch::SUB_ACCOUNT_UPDATES | StartupFetch::EXECUTIONS);")
    o("}")
    o("")
    record_types.append("TradingSession")  # reached through ContractDetails, not exported
    ticker_fields = [f for f in model.fields("Ticker") if f != "created"]
    assert len(ticker_fields) == 110, f"Ticker has {len(ticker_fields)} fields"

    types_of = {}
    o("/// Every field of every mapped type, through a borrowed value, and each type's members.")
    for name in record_types + ["Bar", "BarList"]:
        kind = model.kind(name)
        rust_name = HELPER_TYPES.get(name, name)
        fields = [f for f in model.fields(name) if f"{name}.{f}" not in EXCLUDED] if kind else []
        if name in LIST_ITEMS:
            fields = [LIST_ITEMS[name]] + fields
        types_of[name] = rust_name
        if fields:
            o(f"fn fields_{snake(name)}(x: &{rust_name}) {{")
            chunks = [fields[i : i + 12] for i in range(0, len(fields), 12)]
            for chunk in chunks:
                o("    used((" + ", ".join(f"&x.{snake(f)}" for f in chunk) + ",));")
            o("}")
            o("")
    o("/// Each type's traits: ib_async's equality, where a Live handle's is identity, and the")
    o("/// Debug and Clone every value has.")
    o("fn traits() {")
    for name in record_types + ["Bar"]:
        rust_name = HELPER_TYPES.get(name, name)
        o(f"    debug_clone::<{rust_name}>();")
        if name in IDENTITY_LIVE:
            o(f"    eq::<Live<{rust_name}>>();")
        else:
            o(f"    eq::<{rust_name}>();")
    o("    eq::<Live<BarList>>();")
    o("}")
    o("")

    o("/// Where ib_async's type has no required field, a Rust Default.")
    o("fn defaults_exist() {")
    all_default = []
    for name in record_types + ["Bar"]:
        kind = model.kind(name)
        if kind not in ("dataclass",):
            continue
        fs = model.fields(name)
        # OrderCondition has no field of its own: in Rust it is the enum of the six.
        if fs and all(v is not None for v in fs.values()):
            all_default.append(name)
            o(f"    default::<{name}>();")
    o("}")
    o("")

    # Members.
    o("/// The methods and class constants of each exported class, and the ticker helpers.")
    o("fn members(ib: &IB) {")
    member_classes = [n for n in model.all if n in model.classes] + HELPERS
    for name in dict.fromkeys(member_classes):
        if name in EXCLUDED or name in ("IB", "Client", "Wrapper"):
            continue
        cls = model.classes[name]
        owner = HELPER_TYPES.get(name, name)
        if name in CONTRACT_KINDS:
            owner = "Contract"
        if name in ORDER_KINDS:
            owner = "Order"
        for m in methods(cls):
            key = f"{name}.{m}"
            if key in EXCLUDED:
                continue
            if m.startswith("__"):
                if m not in PROTOCOL:
                    need(key)
                continue
            home = MEMBER_HOME.get(key, owner)
            rname = snake(m)
            if name == "FlexReport":
                o('    #[cfg(feature = "flex")]')
                c = call(rust, "FlexReport", rname, "any::<&flex::FlexReport>()")
                o(f"    used({c.replace('FlexReport::', 'flex::FlexReport::')}); // {key}")
                continue
            if name == "TickerUpdateEvent":
                o(f"    used({call(rust, 'Event<Live<Ticker>>', rname, 'ib.tickers()[0].update_event()')}); // {key}")
                continue
            o(f"    used({call(rust, home, rname, f'any::<&{home}>()')}); // {key}")
        for cv in classvars(cls):
            key = f"{name}.{cv}"
            if key in EXCLUDED or cv == "events" or cv.startswith("_"):
                continue
            o(f"    used({owner}::{snake(cv).upper()}); // {key}")
    o("    let a: OptionComputation = any();")
    o("    used((a + a, a - a, a * 2.0)); // OptionComputation.__add__, __sub__, __mul__")
    o('    #[cfg(feature = "flex")]')
    o("    used((flex::FLEXREPORT_URL, any::<&flex::FlexReport>().root())); // FLEXREPORT_URL, root")
    o("}")
    o("")

    # -- The Wrapper's fields.
    o("/// The Wrapper's fields a program reads, as the accessors that give them.")
    o("fn wrapper_fields(ib: &IB) {")
    for f in model.fields("Wrapper"):
        if f.startswith("_"):
            continue
        key = f"Wrapper.{f}"
        if key in EXCLUDED:
            continue
        if f not in WRAPPER_FIELDS:
            need(key)
            continue
        acc = WRAPPER_FIELDS[f]
        recv, args = rust.args("IBHandle", acc) if "." not in acc else (None, [])
        o(f"    used(ib.{acc}({', '.join(args)})); // Wrapper.{f}")
    o("}")
    o("")

    # -- Client.
    client = model.classes["Client"]
    o("/// `ib.client`: ib_async's Client surface.")
    o("fn client(c: &Client) {")
    for m in methods(client):
        key = f"Client.{m}"
        if key in EXCLUDED or m.startswith("__"):
            continue
        o(f"    used({call(rust, 'Client', snake(m), 'c')}); // {key}")
    for cv in classvars(client):
        key = f"Client.{cv}"
        if key in EXCLUDED:
            continue
        if cv == "events":
            o("    used(Client::EVENTS);")
        elif cv in ("DISCONNECTED", "CONNECTING", "CONNECTED"):
            o(f"    used(ConnState::{cv.capitalize()});")
        else:
            need(key)
    for fn in (b for b in client.body if isinstance(b, ast.FunctionDef) and b.name in ("__init__", "reset")):
        for a in self_attrs(fn):
            key = f"Client.{a}"
            if key in EXCLUDED:
                continue
            o(f"    used(c.{snake(a)}()); // {key}")
    o("}")
    o("")

    # -- util.
    util_tree = model.trees["util"]
    o("/// util's names.")
    o("fn util_names() {")
    for n in util_tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names_ = [n.name]
        elif isinstance(n, ast.Assign):
            names_ = [t.id for t in n.targets if isinstance(t, ast.Name)]
        elif isinstance(n, ast.AnnAssign):
            names_ = [n.target.id]
        else:
            continue
        for u in names_:
            if u.startswith("_"):
                continue
            key = f"util.{u}"
            if key in EXCLUDED:
                continue
            if u not in UTIL:
                need(key)
                continue
            path = UTIL[u]
            owner, _, fname = path.rpartition("::")
            if (owner, fname) in rust.fns:
                o(f"    used({call(rust, owner, fname, 'any::<&IBHandle>()')}); // {key}")
            elif u == "Time_t":
                o(f"    used(any::<{path}>()); // {key}")
            else:
                o(f"    used(&{path}); // {key}")
    o("}")
    o("")

    # -- eventkit's Event.
    event_map = {
        "__post_init__": 'Event::<()>::new("name")',
        "__hash__": None,
        "__repr__": "format!(\"{e:?}\")",
        "name": "e.name()",
        "done": "e.done()",
        "set_done": "e.set_done()",
        "value": "e.value()",
        "connect": "e.connect(|_| {})",
        "__iadd__": "e.connect(|_| {})",
        "disconnect": "e.disconnect(any())",
        "__isub__": "e.disconnect(any())",
        "emit": "e.emit(&())",
        "__call__": "e.emit(&())",
        "emit_threadsafe": "e.emit(&())",
        "clear": "e.clear()",
        "error_event": "e.error_event()",
        "done_event": "e.done_event()",
        "aiter": "e.subscribe().recv_async()",
        "__aiter__": "e.subscribe().try_recv()",
        "__await__": "e.subscribe().recv()",
        "__len__": "e.len()",
        "__contains__": "e.contains(any())",
    }
    o("/// eventkit's `Event`, member by member.")
    o("fn event(e: &Event<()>) {")
    members_ = [b.target.id for b in model.event.body if isinstance(b, ast.AnnAssign) and not b.target.id.startswith("_")]
    members_ += methods(model.event)
    for m in dict.fromkeys(members_):
        key = f"Event.{m}"
        if key in EXCLUDED or m in EVENT_OPERATORS:
            continue
        if m not in event_map:
            need(key)
            continue
        if event_map[m]:
            o(f"    used({event_map[m]}); // {key}")
    o("    used(e.connect_async(|_| async {}));")
    o("    let mut s = e.subscribe();")
    o("    used((s.recv_timeout(any()), s.next()));")
    o("}")
    o("")

    # -- Extras.
    o("/// The methods beyond ib_async's API.")
    o("fn extras(ib: &IB) {")
    for x in EXTRAS:
        o(f"    used({call(rust, 'IBHandle', snake(x), 'ib')}); // {x}")
    o("}")
    o("")

    # -- Signatures.
    o("/// Typed signatures: a changed parameter or result fails to compile.")
    o("fn signatures() {")
    for s in SIGNATURES:
        ty, path = [x.strip() for x in s.rsplit("=", 1)]
        if "impl Future" in ty or "impl Stream" in ty:
            # An async fn's future and an async generator's stream have no name: pinned by the
            # call's argument and output types.
            if "impl Future" in ty:
                args, out = ty[3:].split(") -> impl Future<Output = ", 1)
                bound, given = f"Future<Output = {out.rsplit('>', 1)[0]}>", "F"
            else:
                args, item = ty[3:].split(") -> Result<impl Stream<Item = ", 1)
                bound, given = f"futures_core::Stream<Item = {item.rsplit('> + Send>', 1)[0]}>", "Result<F>"
            params = split_top(args)
            names_ = [f"p{i}" for i in range(len(params))]
            owner, fname = path.split("::")
            o("    {")
            for n, p in zip(names_, params):
                o(f"        let {n}: {p} = any();")
            o(f"        fn out<F: {bound} + Send>(_: {given}) {{}}")
            o(f"        out({owner}::{fname}({', '.join(names_)}));")
            o("    }")
        else:
            o(f"    let _: {ty} = {path};")
    o("}")
    o("")

    o("/// Every default with an ib_async counterpart holds ib_async's value.")
    o("#[test]")
    o("fn defaults() {")
    # IB's class attributes.
    ib_attrs = {b.target.id: b.value for b in ib.body if isinstance(b, ast.AnnAssign)}
    o("    let c = IBConfig::default();")
    for a, v in ib_attrs.items():
        d = default_of(v, model)
        if a == "TimezoneTWS":
            assert d == ("str", "")
            o("    assert!(c.timezone_tws.is_none(), \"TimezoneTWS '': no zone\");")
            continue
        if a == "RequestTimeout":
            assert d == ("num", 0)
            o("    assert!(c.request_timeout.is_none(), \"RequestTimeout 0: no limit\");")
            continue
        ty = rust.structs["IBConfig"][snake(a)]
        o("    " + assertion(f"c.{snake(a)}", ty, d))
    # connect's arguments.
    connect = next(b for b in ib.body if isinstance(b, ast.FunctionDef) and b.name == "connect")
    params = connect.args.args[-len(connect.args.defaults) :]
    o("    let o = ConnectOptions::default();")
    for p, v in zip(params, connect.args.defaults):
        if p.arg in ("host", "port"):
            continue
        d = default_of(v, model) if p.arg != "fetchFields" else None
        if p.arg == "timeout":
            o(f"    assert_eq!(o.timeout, Some(Duration::from_secs({int(d[1])})), \"timeout\");")
        elif p.arg == "readonly":
            o("    " + assertion("o.config.readonly", "bool", d))
        elif p.arg == "fetchFields":
            assert ast.unparse(v) == "StartupFetchALL"
            o("    assert!(o.fetch_fields == StartupFetch::ALL, \"fetchFields\");")
        elif p.arg == "clientId":
            o("    " + assertion("o.client_id", "i64", d))
        else:
            o("    " + assertion(f"o.{snake(p.arg)}", rust.structs["ConnectOptions"][snake(p.arg)], d))
    # The timeouts whose default the crate states as a constant.
    hist = next(b for b in ib.body if isinstance(b, ast.FunctionDef) and b.name == "reqHistoricalData")
    t = dict(zip([a.arg for a in hist.args.args[-len(hist.args.defaults) :]], hist.args.defaults))["timeout"]
    o(f"    assert_eq!(HISTORICAL_TIMEOUT, Duration::from_secs({ast.literal_eval(t)}));")
    for owner, method, name in ((ib, "setTimeout", "SET_TIMEOUT"), (model.classes["Client"], "connect", "CLIENT_CONNECT_TIMEOUT")):
        fn = next(b for b in owner.body if isinstance(b, ast.FunctionDef) and b.name == method)
        t = dict(zip([a.arg for a in fn.args.args[-len(fn.args.defaults) :]], fn.args.defaults))["timeout"]
        o(f"    assert_eq!({name}, Duration::from_secs_f64({float(ast.literal_eval(t))!r}));")
    o("    // No ib_async counterpart: the engine's own 15 s, and the spread scan's 10 s.")
    o("    assert_eq!(CORPORATE_ACTIONS_TIMEOUT, Duration::from_secs(15));")
    o("    assert_eq!(SPREAD_SCAN_TIMEOUT, Duration::from_secs(10));")
    # Every type whose every field defaults.
    for name in all_default:
        fs = model.fields(name)
        rfields = rust.structs.get(name)
        if rfields is None:
            missing.append(f"struct {name}")
            continue
        o(f"    let d = {name}::default();")
        for f, v in fs.items():
            if f"{name}.{f}" in EXCLUDED:
                continue
            d = default_of(v, model)
            rf = snake(f)
            if rf not in rfields:
                missing.append(f"field {name}.{rf}")
                continue
            o("    " + assertion(f"d.{rf}", rfields[rf], d))
    o("}")

    if missing:
        raise Unmapped("\n  ".join(["names mapped nowhere and not excluded:"] + missing))
    return o.text(), names


def twins(model, rust, names):
    o = Out()
    o("//! Every twin's future is `Send`: the 133's async forms, the extras', `Client`'s and util's.")
    o("//! Generated by `scripts/parity.py --rust`. Do not edit: regenerate.")
    o("")
    o("#![allow(dead_code, clippy::unit_arg)]")
    o("")
    o("use ib_async_dx::*;")
    o("use jiff::Timestamp;")
    o("")
    o("fn any<T>() -> T {")
    o("    unreachable!()")
    o("}")
    o("")
    o("fn assert_send<T: Send>(_: &T) {}")
    o("")
    o("fn twins(ib: &IB, client: &Client) {")
    for m in names + [x for x in EXTRAS if x.endswith("Async")]:
        if not m.endswith("Async"):
            continue
        owner = "IB" if m in IB_ASSOCIATED else "IBHandle"
        c = call(rust, owner, snake(m), "ib")
        if m == "timeRangeAsync":
            o(f"    if let Ok(stream) = {c} {{")
            o("        assert_send(&stream);")
            o("    }")
            continue
        o(f"    assert_send(&{c}); // {m}")
    o(f"    assert_send(&{call(rust, 'IB', 'wait_until_async', 'ib')}); // util.waitUntilAsync")
    o(f"    assert_send(&{call(rust, 'Client', 'connect_async', 'client')}); // Client.connectAsync")
    o("}")
    return o.text()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--rust", action="store_true", help="write tests/parity_names.rs and tests/twins_send.rs")
    args = ap.parse_args()
    model, rust = Model(), Rust()
    try:
        names_rs, names = generate(model, rust)
    except Unmapped as e:
        sys.exit(f"parity: {e}")
    send_rs = twins(model, rust, names)
    if args.rust:
        out = [ROOT / "tests" / "parity_names.rs", ROOT / "tests" / "twins_send.rs"]
        for path, text in zip(out, (names_rs, send_rs)):
            path.write_text(text)
        subprocess.run(["rustfmt", "--edition", "2024", *map(str, out)], check=True)
    print(f"parity: {len(names)} callables; every name is mapped or excluded")


if __name__ == "__main__":
    main()
