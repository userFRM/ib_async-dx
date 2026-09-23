# Evidence

What was measured, and on what. Status is assigned from a named artifact — a
test, a script, or a recorded server response — not from reading the code.

| Status | Definition |
| :---: | --- |
| ✅ Supported | Implemented; exercised against IBKR production servers; the answer is delivered to the caller |
| ✅ Documented | A property of the venue rather than a call: established against IBKR production servers, with nothing for this package to implement |
| ✅ Offline | Implemented, and held by the offline suite on every run; not yet exercised against IBKR production servers |
| — Not yet | Not in this repository yet |

## The suites

| Suite | Count | Needs a session |
| --- | ---: | --- |
| `tests/python` | 51 | 2 of them. The other 49 run offline |
| `tests/ib_async_upstream` | ib_async's own suite, 3 tests at 2.1.0; the 2 that take its shared fixture run on the engine | Yes |
| `scripts/` | 3 checks against a paper account | Yes |

CI builds the engine from source with its test hooks, installs this package —
and with it the real ib_async — and runs `tests/python` on every push to `main`
and every pull request. The workflow passes no credentials, so there the two
live tests are skipped; they run against the venue locally, with
`IB_USERNAME` and `IB_PASSWORD` set.

## Surfaces

| Surface | Status | Verification |
| --- | :---: | --- |
| `ib_async_dx` is ib_async's public API | ✅ Offline | `__all__` equal to ib_async's, and every name ib_async's own object except `IB` and `__version__`; ib_async's 13 submodules under both import forms: `tests/python/test_the_package_is_ib_async.py` |
| ib_async's `IB` on the engine, through `attach` | ✅ Supported | Its events, `*Async` calls and types, with no gateway. All 67 transport calls their `IB` makes are carried, read out of their source on every run by `test_every_call_their_library_makes_is_carried`. Live: `test_an_unmodified_program_runs_on_this_engine` and `test_an_order_lives_its_whole_life_through_their_api`, in `tests/python/test_ib_async_transport.py` |
| `ib_async_dx.IB` on the engine | ✅ Offline | The same client, installed by its `connect`, with the login, the positions fix and the competing-session warning: `tests/python/test_ib_runs_on_the_engine.py`. Written for the venue and not yet run there: their own suite through `tests/ib_async_upstream/conftest.py`; `scripts/sdk_sweep.py` (every read, printing what came back; places nothing), `scripts/sdk_lifecycle.py` and `scripts/order_round_trip.py` (an order placed, changed and withdrawn on paper); the seven [notebooks](./notebooks.md) |
| The two ib_async bug fixes | ✅ Offline | `tests/python/test_ib_runs_on_the_engine.py`, each beside ib_async's own answer |
| The engine's calls beyond the documented API, on `IB` | ✅ Offline | `tests/python/test_ib_runs_on_the_engine.py`; see [Beyond ib_async](./beyond.md) |
| `reqCorporateActions`, `reqSpreadScan`, `positionsElsewhere`, `accountValuesElsewhere`, `TickerExtras.statedRows` | — Not yet | Each waits on an addition to the engine; see [Beyond ib_async](./beyond.md#coming) |
| Rust client | — Not yet | Coming, on the engine's public API |

**ib_async's own suite.** Of its three tests at 2.1.0, `test_account_summary`
passes on the engine. `test_request_error_raised` asserts a `RequestError`
carrying code 321, which their wrapper cannot raise: it lists 321 among the
codes it treats as warnings, and a warning never ends the request it belongs
to, so that test fails the same way against any server.
`test_contract_format_data_pd` builds its own `IB` and connects it to
`127.0.0.1:4001` instead of taking the shared fixture, so it never reaches the
engine; with no gateway on that port it fails to connect.

## The package is ib_async

All offline, in `tests/python/test_the_package_is_ib_async.py`.

| What holds | Test |
| --- | --- |
| `__all__` is ib_async's 103 names, and only `IB` and `__version__` are other objects | `test_the_names_are_theirs_and_so_are_the_objects` |
| `__version__` is this package's, `__ib_async_version__` is ib_async's, `__version_info__` stays ib_async's | `test_the_versions_name_this_package_and_the_ib_async_it_runs` |
| Every submodule of ib_async's resolves here, as an attribute and as an import | `test_every_submodule_of_theirs_resolves_here`, `test_from_a_submodule_import_works` |
| `ib_async_dx.ib` is ib_async's `ib` module with `IB` replaced, and a star import of it binds the names ib_async's does | `test_the_ib_module_is_theirs_with_ib_replaced` |
| `IB` is a subclass of ib_async's, with ib_async's own `__init__`, and `attach` is outside `__all__` | `test_ib_is_their_ib` |
| `connect` and `connectAsync` are ib_async's parameters, defaults and kinds, then four keyword-only ones | `test_connect_is_theirs_plus_the_login` |

## `IB`, connected

All offline, on the engine's test session, in
`tests/python/test_ib_runs_on_the_engine.py`.

| What holds | Test |
| --- | --- |
| The login comes from the environment when `connect` names none, and the session file is named for the account | `test_the_login_comes_from_the_environment_when_connect_names_none` |
| A login named on `connect` is the one used, in ib_async's positional form; the client id reaches the login; `sessionFile=False` keeps nothing | `test_a_login_named_on_connect_is_the_one_used` |
| A session is paper unless the program says live | `test_a_session_is_paper_unless_the_program_says_live` |
| An `IB` connects again after it disconnected, and `placeOrder` is not wrapped again | `test_an_ib_connects_again_after_it_disconnected` |
| A second `connect` closes the session the first opened | `test_a_second_connect_closes_the_first_session` |
| `updateEvent` fires on what arrives, and `timeoutEvent` when nothing does | `test_updateEvent_and_timeoutEvent_follow_what_arrives` |
| `fetchFields` without `POSITIONS` asks for no positions at connect; ib_async 2.1 asks anyway | `test_fetchFields_without_positions_asks_for_no_positions` |
| `reqUserInfo` answers the White Branding ID; ib_async 2.1 answers `[]` | `test_reqUserInfo_answers_the_white_branding_id` |
| `reqMktDataEx` asks with the market data type named, and `None` keeps the session's | `test_reqMktDataEx_asks_with_the_market_data_type_named` |
| `reqCurrentTimeInMillis` answers an int of milliseconds, within 2 s of the local clock on a session with no venue | `test_reqCurrentTimeInMillis_is_the_venues_clock` |
| The option model is read by the ticker's request, and an unstated figure is `None` | `test_the_option_model_is_read_by_the_tickers_request` |
| What the engine states arrives in `TickerExtras`, `OrderPreset` and `CompetingSession` | `test_what_the_engine_states_is_carried_in_this_packages_types` |
| The account's grants are read from the engine | `test_the_accounts_grants_are_read_from_the_engine` |
| A ping reaches the engine | `test_a_ping_reaches_the_engine` |
| An extra called while not connected, before `connect` or after `disconnect`, raises `ConnectionError` | `test_an_extra_while_not_connected_raises` |

## What crosses to the engine

All offline, in `tests/python`.

| What holds | Test |
| --- | --- |
| `attach` replaces the transport and nothing else: their `IB`, their wrapper | `test_ib_async_transport.py::test_attach_replaces_only_the_transport` |
| A combination keeps its legs on every request path — details, quotes, bars | `test_ib_async_bridge_carries_everything.py::test_a_combination_keeps_its_legs_on_every_request_path` |
| A contract named by more than its symbol keeps the rest: an ISIN, `includeExpired` | `test_ib_async_bridge_carries_everything.py::test_a_contract_named_by_more_than_its_symbol_keeps_the_rest` |
| A contract named by its id states nothing else beside it | `test_ib_async_transport.py::test_a_contract_named_by_id_states_nothing_else` |
| An algo's parameters, a combination's routing and a soft-dollar tier reach the order | `test_ib_async_transport.py::test_what_tunes_an_algo_reaches_the_order` |
| A field set to something the engine cannot carry is refused, by name | `test_ib_async_transport.py::test_a_field_that_cannot_be_carried_is_refused` |
| A fill's cost reaches them as their own `CommissionReport` | `test_ib_async_bridge_carries_everything.py::test_a_fills_cost_arrives_as_the_record_their_wrapper_reads`, `test_ib_async_depth.py::test_a_commission_report_reaches_them_as_their_own_type` |
| A histogram reaches them as their `HistogramData` | `test_a_histogram_crosses_the_bridge_in_their_type.py` (2) |
| A historical tick reaches them as their own record | `test_ib_async_depth.py::test_a_historical_tick_is_handed_over_as_their_own_record` |
| A book level reaches their ticker, on both sides and below the top | `test_ib_async_depth.py::test_a_book_level_reaches_their_ticker`, `::test_the_other_side_and_a_deeper_level` |
| A price and its size reach them together, and a size with no price states none | `test_ib_async_bridge_carries_everything.py::test_a_size_with_no_price_behind_it_states_no_price` |
| A refusal reaches their `error` in the four-argument shape it declares | `test_a_refusal_reaches_their_wrapper.py` (3) |
| Every account the login holds crosses over | `test_ib_async_bridge_carries_everything.py::test_every_account_the_login_holds_crosses_over` |
| Order ids and request ids are counted apart, and a wide order id leaves requests numberable | `test_an_order_id_wider_than_a_request_leaves_requests_numberable.py` (2), `test_ib_async_depth.py::test_a_seeded_order_id_is_the_next_one_their_client_issues` |
| `readonly` reaches the session | `test_ib_async_transport.py::test_readonly_reaches_the_session_through_the_adapter` |
| An outage leaves the session connected; a session the engine ends fires `disconnectedEvent` once, stops delivery and refuses requests | `test_ib_async_transport.py::test_an_outage_leaves_the_session_open_and_an_end_closes_it` |
| Ending a session is not reported as a session that went away | `test_ib_async_transport.py::test_ending_a_session_is_not_a_session_that_went_away` |
| A request the engine does not carry says so, naming itself | `test_ib_async_transport.py::test_a_request_their_client_carries_and_this_one_does_not_says_so` |

## Measured on the engine

These were measured on the engine this package runs on, with the client this
package uses, and are recorded with the engine in
[ibkr-dx's evidence](https://github.com/userFRM/ibkr-dx/blob/6878c2e91795d9d52ccae7c8e18088da91b627c6/docs/evidence.md),
as it stood when they were taken.

| Capability | Status | What was seen |
| --- | :---: | --- |
| Market depth through ib_async | ✅ Supported | Inserts, updates and deletes delivered as the venue states them, ten levels a side, through ib_async's own ticker. Which venues answer is the account's entitlement |
| Real-time bars through ib_async | ✅ Supported | Five-second bars during regular hours, each with open, high, low, close and volume, alongside a book on the same session, through ib_async's own `reqRealTimeBars` |
| A session survives losing its connection | ✅ Supported | A dropped connection is rebuilt on the session already open, with no second factor: five forced drops recovered in 2–8 s, and an eight-hour session rode through its losses unattended |
| A session does not survive its process | ✅ Documented | Ended without logging out, the session was gone forty seconds later, and a later start was answered with a full login rather than a challenge. A restart is therefore a new login. What that costs an account with a second factor has not been measured; a paper session presents none |
| A long session | ✅ Supported | One session held for 175 minutes across a market open: 106,053 quotes, 180,433 trades, 95,985 book rows and 4,148 bars, with no unrequested disconnect |
