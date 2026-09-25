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
| `tests/python` | 300 | 2 of them. The other 298 run offline |
| `tests/ib_async_upstream` | ib_async's own suite, 3 tests at 2.1.0 (`ab629f34c1`), written to run on the engine; not run against the venue at this revision | Yes |
| `scripts/` | 3 checks against a paper account; not run against the venue at this revision | Yes |

CI builds the engine from source with its test hooks, at the engine commit this
package is tested against (`e3f4307a`), and runs `tests/python` on every push
to `main` and every pull request, on Python 3.11, 3.13 and free-threaded
3.14t, against this package installed from the wheel and from the source
distribution it builds, never from the checkout. It also installs the wheel
beside the engine built as a release builds it, without its test hooks, and
imports it. The workflow passes no credentials, so there the two live tests are
skipped; they run against the venue locally, with `IB_USERNAME` and
`IB_PASSWORD` set.

A release is the Python package and the Rust client together. Its workflow
refuses a tag until the Rust client is in the repository and nothing on this
page is still "Not yet", then runs the suite again against the tag's wheel and
source distribution before attaching them to the release.

## Surfaces

| Surface | Status | Verification |
| --- | :---: | --- |
| `ib_async_dx` is ib_async's public API | ✅ Offline | `__all__` equal to ib_async's, and every name ib_async's own object except `IB` and `IBC`, `__version__` among them; ib_async's 13 submodules under both import forms: `tests/python/test_the_package_is_ib_async.py` |
| ib_async's `IB` on the engine, through `attach` | ✅ Offline | Its events, `*Async` calls and types, with no gateway. All 67 transport calls their `IB` makes are carried, read out of their source on every run by `test_every_call_their_library_makes_is_carried`. Written for the venue and not run against it at this revision: `test_an_unmodified_program_runs_on_this_engine` and `test_an_order_lives_its_whole_life_through_their_api`, in `tests/python/test_ib_async_transport.py` |
| Their client's `send` and `sendMsg` | ✅ Offline | Each of the 80 requests their client writes as a message, written by their own code through `send`, reads back into the message it was and reaches the engine exactly as the same request made by name; a message that does not read is refused with 320, and one naming no request is logged and unanswered: `tests/python/test_a_raw_message_is_the_request_it_names.py` |
| `ib_async_dx.IB` on the engine | ✅ Offline | The same client, installed by its `connect`, with the login, the positions fix and the competing-session warning: `tests/python/test_ib_runs_on_the_engine.py`. Written for the venue and not yet run there: their own suite through `tests/ib_async_upstream/conftest.py`; `scripts/sdk_sweep.py` (every read, printing what came back; places nothing), `scripts/sdk_lifecycle.py` and `scripts/order_round_trip.py` (an order placed, changed and withdrawn on paper); the eight [notebooks](./notebooks.md) |
| The session's life: on the loop, a pass a batch; a login cancelled, overtaken or refused; a session ended as it opens or once open | ✅ Offline | `tests/python/test_a_session_opens_and_closes_on_the_loop.py` |
| The four ib_async bug fixes | ✅ Offline | `tests/python/test_ib_runs_on_the_engine.py` and `tests/python/test_a_session_opens_and_closes_on_the_loop.py`; see [Beyond ib_async](./beyond.md#ib_asyncs-bugs-fixed) |
| The engine's calls beyond the documented API, on `IB` | ✅ Offline | `tests/python/test_ib_runs_on_the_engine.py`; see [Beyond ib_async](./beyond.md) |
| `reqCorporateActions`, `reqSpreadScan`, `positionsElsewhere`, `accountValuesElsewhere` | ✅ Offline | `tests/python/test_ib_runs_on_the_engine.py`; see [Beyond ib_async](./beyond.md) |
| `TickerExtras.statedRows` | — Not yet | Waits on an addition to the engine; see [Beyond ib_async](./beyond.md#coming) |
| `IBC` with ib_async's own `Watchdog` | ✅ Offline | A reconnect loop, connecting with its IBC's login and again when the session ends, each `Watchdog` with its own: `tests/python/test_ib_runs_on_the_engine.py` |
| Rust client | — Not yet | Coming, on the engine's public API |

**ib_async's own suite.** Its three tests at 2.1.0, from their commit
`ab629f34c1`, have not been run against the venue at this revision, so none of
them is a result here. `test_request_error_raised` asserts a `RequestError`
carrying code 321 from a refused what-if, which their own `IB` never raises: it
counts 321 as a warning, and a warning never ends the request it belongs to.
`ib_async_dx.IB` ends it with the refusal, which
`test_a_what_if_refused_with_321_ends_with_the_refusal` holds offline.
`test_contract_format_data_pd` builds its own `ib_async.IB()`, which the
runner's conftest makes this package's before their tests are collected, so it
connects to the engine.

## The package is ib_async

All offline, in `tests/python/test_the_package_is_ib_async.py`.

| What holds | Test |
| --- | --- |
| `__all__` is ib_async's 103 names, and only `IB` and `IBC` are other objects | `test_the_names_are_theirs_and_so_are_the_objects` |
| `__version__` and `__version_info__` are ib_async's, and this package's version is `__ib_async_dx_version__`, outside `__all__` | `test_the_version_is_ib_asyncs_and_this_packages_is_apart` |
| Every submodule of ib_async's resolves here, as an attribute and as an import | `test_every_submodule_of_theirs_resolves_here`, `test_from_a_submodule_import_works` |
| `ib_async_dx.ib` is ib_async's `ib` module with `IB` replaced, and a star import of it binds the names ib_async's does | `test_the_ib_module_is_theirs_with_ib_replaced` |
| `ib_async_dx.ibcontroller` is ib_async's `ibcontroller` module with `IB` and `IBC` replaced, its `Watchdog` ib_async's own | `test_the_ibcontroller_module_is_theirs_with_IB_and_IBC_replaced` |
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
| An `IB` connects again after it disconnected, with ib_async's own `placeOrder` | `test_an_ib_connects_again_after_it_disconnected` |
| A second `connect` closes the session the first opened | `test_a_second_connect_closes_the_first_session` |
| An `IBC`'s login is the one a connect naming none logs in with, paper unless its trading mode is live, and terminating it ends that session | `test_an_ibc_carries_its_login_to_the_connect_and_terminates_its_session` |
| Terminating an `IBC` from another context ends every session its login opened, and no later connect logs in with it | `test_a_terminated_ibc_ends_every_session_of_its_login_and_lends_it_to_none` |
| ib_async's own `Watchdog` logs in with its IBC's login, again after the session ends, and each `Watchdog` with its own | `test_an_unmodified_watchdog_logs_in_with_its_ibcs_login` |
| An order's status names the client that placed it, so an order another client placed reaches its `Trade` | `test_an_order_status_names_the_client_that_placed_the_order` |
| A price reaches their ticker with the size that goes with it, and a size that changed alone as a size tick | `test_a_price_carries_its_size_and_a_size_alone_reaches_tickSize` |
| A refused new order reaches its `Trade`, and one refused with 321 is cancelled | `test_a_refused_new_order_reaches_its_trade` |
| An order placed once the engine has given the session up is refused after `placeOrder` has made its `Trade`, and the trade is cancelled before the close | `test_an_order_placed_as_the_engine_gives_the_session_up_reaches_its_trade` |
| A what-if refused with 321 ends with the refusal, as `RequestError` where `RaiseRequestErrors` is set | `test_a_what_if_refused_with_321_ends_with_the_refusal` |
| 321 on an order already working stays a warning, and the order is kept | `test_321_on_an_order_already_working_stays_a_warning` |
| The engine checks an option list as a gateway checks one: 10337 for another key, none taken on the two option computations, 10338 for another value, in the gateway's words, under the request's number, nothing sent | `test_an_option_list_is_checked_as_a_gateway_checks_one` |
| An order stating an attribute the venue no longer takes is carried to the engine, which gives the notice a gateway gives | `test_an_order_stating_a_retired_attribute_reaches_the_engine_with_it` |
| A handler asking again on every refusal is answered a refusal a pass, and does not hold the loop | `test_a_handler_that_asks_again_on_every_refusal_does_not_hold_the_loop` |
| A wrapper that raises is logged, and the session carries on | `test_a_wrapper_that_raises_is_logged_and_the_session_carries_on` |
| An order id the venue names after the connect is not handed out | `test_an_order_id_the_venue_names_after_the_connect_is_not_handed_out` |
| Prices stated as the session ends reach the ticker before the close | `test_prices_stated_as_the_session_ends_reach_the_ticker` |
| A refusal held as the program disconnects does not reach the next session | `test_a_refusal_held_as_the_program_disconnects_does_not_reach_the_next_session` |
| Orders and requests are numbered from one counter, so an order's refusal reaches the order and not a request still waiting | `test_orders_and_requests_are_numbered_from_one_counter` |
| `updateEvent` fires on what arrives, and `timeoutEvent` when nothing does | `test_updateEvent_and_timeoutEvent_follow_what_arrives` |
| `fetchFields` without `POSITIONS` asks for no positions at connect; ib_async 2.1 asks anyway | `test_fetchFields_without_positions_asks_for_no_positions` |
| `reqUserInfo` answers the White Branding ID; ib_async 2.1 answers `[]` | `test_reqUserInfo_answers_the_white_branding_id` |
| `reqMktDataEx` asks with the market data type named, with its option list for the engine to check, and `None` keeps the session's | `test_reqMktDataEx_asks_with_the_market_data_type_named` |
| `reqCurrentTimeInMillis` answers an int of milliseconds, within 2 s of the local clock on a session with no venue | `test_reqCurrentTimeInMillis_is_the_venues_clock` |
| The option model is read by the ticker's request, and an unstated figure is `None` | `test_the_option_model_is_read_by_the_tickers_request` |
| What the engine states arrives in `TickerExtras`, `OrderPreset` and `CompetingSession` | `test_what_the_engine_states_is_carried_in_this_packages_types` |
| The account's grants are read from the engine | `test_the_accounts_grants_are_read_from_the_engine` |
| A ping reaches the engine | `test_a_ping_reaches_the_engine` |
| An extra called while not connected, before `connect` or after `disconnect`, raises `ConnectionError` | `test_an_extra_while_not_connected_raises` |
| A record ib_async builds whole, a routing component, is answered from the engine and the session carries on | `test_the_routing_components_arrive_as_their_records` |
| `connectionStats()` counts the messages each way, carries the engine's byte counts, and raises while not connected | `test_connectionStats_counts_the_messages_each_way` |
| The competing-session warning is logged on `ib_async_dx.ib` | `test_the_competing_session_warning_is_this_packages_own` |
| A connect that completed is not failed by the warning after it: not by a stamp that does not parse, nor by a handler that disconnected | `test_a_completed_connect_is_not_failed_by_what_follows_it` |
| `priceBasedVol` is a bool, `False` when the venue did not state it | `test_priceBasedVol_is_a_bool_when_unstated` |
| The size kept for a quote goes with its subscription, cancelled or a snapshot answered | `test_a_subscription_over_leaves_no_size_behind` |
| ib_async's own `Watchdog`, with this package's `IBC`, connects and connects again when the session ends | `test_an_unmodified_watchdog_keeps_the_session_up` |
| `reqCorporateActions` asks by the contract's id, takes the answer, withdraws a query given up, and refuses a contract with no id | `test_reqCorporateActions_asks_by_the_contracts_id_and_takes_the_answer`, `test_reqCorporateActions_given_up_withdraws_the_query`, `test_reqCorporateActions_needs_the_contracts_id` |
| `reqSpreadScan` takes the first answer and cancels, its quotes go to the underlying's ticker, unanswered in time it found nothing, and refused it cancels nothing | `test_reqSpreadScan_takes_the_first_answer_and_cancels`, `test_reqSpreadScan_unanswered_in_time_found_nothing`, `test_reqSpreadScan_refused_cancels_nothing` |
| A `reqCorporateActions` or `reqSpreadScan` the program disconnects under raises `ConnectionError` and withdraws nothing | `test_a_request_the_program_disconnects_under_ends_with_the_session` (2) |
| Holdings elsewhere and their figures are read apart from the account's own, and a set is one of three | `test_holdings_elsewhere_are_kept_apart`, `test_accountValuesElsewhere_names_one_of_three_sets` |

## The session's life

All offline, in `tests/python/test_a_session_opens_and_closes_on_the_loop.py`.
Where a login has to be held open, the test engine keeps the engine's own rule:
a disconnect counted during a login drops the session it opens.

| What holds | Test |
| --- | --- |
| A connect cancelled during its login tells the engine, and leaves no session | `test_a_connect_cancelled_during_its_login_leaves_no_session` |
| A `disconnect()` during the login ends the connect with `ConnectionError`, leaves no session, and returns at once while the login holds the engine | `test_a_disconnect_during_the_login_ends_it` |
| Of two connects on one `IB`, the later one's session is left open and the earlier ends at once, on `ib_async_dx.IB` and on an attached `ib_async.IB` | `test_a_later_connect_retires_an_earlier_one_still_logging_in` (2) |
| Only the connect's own startup request for positions is skipped | `test_only_the_connects_own_startup_request_for_positions_is_skipped` |
| A connect overtaken once logged in, still waiting on the venue, fails and leaves the later session open, on `ib_async_dx.IB` and on an attached `ib_async.IB` | `test_a_connect_overtaken_after_its_login_leaves_the_later_session_open` (2) |
| A connect overtaken in ib_async's startup sync fails, and leaves the later session open with `connectedEvent` said once, whatever `raiseSyncErrors` says | `test_a_connect_overtaken_in_its_startup_sync_fails_and_leaves_the_later_open` (2) |
| Of three connects made at once, the last is left open | `test_of_three_connects_at_once_the_last_is_left_open` |
| A `disconnect()` from a handler the connect runs ends the connect | `test_a_disconnect_from_a_handler_during_the_connect_ends_it` |
| A login given up on does not hold the program open | `test_a_login_given_up_on_does_not_hold_the_program_open` |
| Under `util.startLoop()` a handler can wait on the session: a blocking request made in an `errorEvent` handler is answered, and a connect made in a `disconnectedEvent` handler of an attached `ib_async.IB` logs in, whether the engine ended the session or its `IBC` was terminated. Skipped on Python 3.14, where nest_asyncio cannot run asyncio's timeouts | `test_a_handler_can_wait_on_the_session_under_startLoop` |
| A login given up on reaches nothing and keeps nothing, when the engine installs its session after the disconnect, and its engine's close does not end the later session | `test_a_login_given_up_on_reaches_nothing_and_keeps_nothing` (2) |
| An interrupted `connect()` opens nothing later | `test_an_interrupted_connect_opens_nothing_later` |
| A handler that ends the session ends the pass: no held price and no held refusal reaches the cleared wrapper | `test_a_handler_that_ends_the_session_mid_pass_ends_the_pass`, `test_a_handler_that_ends_the_session_leaves_the_refusals_behind_it` |
| Their wrapper is called on the loop's thread only, what the engine announces from inside the login among it, and hears `connectAck`, `managedAccounts` and `nextValidId` there | `test_their_wrapper_is_called_on_the_loops_thread_only` |
| A program away from its loop queues no passes, and passes go on while the loop runs | `test_a_program_that_leaves_the_loop_queues_no_passes` |
| A loop closed without `disconnect()` leaves no pass running | `test_a_loop_closed_without_disconnect_leaves_nothing_running` |
| A session that ends as it opens fails the connect, says why on `apiError`, and nothing more is done for it | `test_a_session_that_ends_as_it_opens_fails_the_connect_and_stops` |
| A refused login raises `ConnectionError`, says why on `apiError`, and leaves the client disconnected | `test_a_login_refused_raises_ConnectionError_and_says_so` |
| `timeout` does not bound the login; were the engine to wait for the next id after it, `timeout` would bound that (a guard: the engine answers at once) | `test_timeout_bounds_the_wait_after_the_login_not_the_login` |
| Connecting an attached `IB` that is connected ends that session and opens a new one | `test_connecting_an_attached_ib_that_is_connected_opens_a_new_session` |
| `serverVersion()` is 0 until connected | `test_serverVersion_is_nought_until_connected` |
| `attach` to a connected `IB` ends its session first, and unties the old client | `test_attach_to_a_connected_ib_ends_its_session_first` |
| `attach` names no client id | `test_attach_names_no_client_id` |
| `connect`, `run`, `reset`, `MaxRequests`, `RequestsInterval` and `events` on the client mean what they mean on theirs | `test_the_client_level_names_mean_what_they_mean_in_ib_async` |
| A request reached by name on the client takes keywords | `test_a_request_reached_by_name_takes_keywords` |
| A pass that raises ends the session once | `test_a_pass_that_raises_ends_the_session_once` |
| What one pass delivers is one batch, on the loop | `test_what_one_pass_delivers_is_one_batch` |

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
| A field at ib_async's default is carried as ib_async sends it; what it sends empty is left to the engine | `test_ib_async_transport.py::test_a_field_at_their_default_is_carried` |
| An order condition joined by or is carried | `test_ib_async_transport.py::test_a_condition_joined_by_or_is_carried` |
| A fill's cost reaches them as their own `CommissionReport` | `test_ib_async_bridge_carries_everything.py::test_a_fills_cost_arrives_as_the_record_their_wrapper_reads`, `test_ib_async_depth.py::test_a_commission_report_reaches_them_as_their_own_type` |
| A histogram reaches them as their `HistogramData` | `test_a_histogram_crosses_the_bridge_in_their_type.py` (2) |
| A historical tick reaches them as their own record | `test_ib_async_depth.py::test_a_historical_tick_is_handed_over_as_their_own_record` |
| A record tuple is rebuilt field by field, and a record named as theirs and of another type is rebuilt as theirs | `test_ib_async_bridge_carries_everything.py::test_a_record_tuple_is_rebuilt_field_by_field`, `::test_a_record_named_as_theirs_and_not_theirs_is_rebuilt_as_theirs` |
| A book level reaches their ticker, on both sides and below the top | `test_ib_async_depth.py::test_a_book_level_reaches_their_ticker`, `::test_the_other_side_and_a_deeper_level` |
| A size with no price beside it is a size tick | `test_ib_async_bridge_carries_everything.py::test_a_size_with_no_price_beside_it_is_a_size_tick` |
| A bar carries its average price, a condition how it joins the next, and a record ib_async builds whole arrives whole | `test_ib_async_bridge_carries_everything.py::test_a_bar_carries_its_average_price`, `::test_a_condition_arrives_saying_how_it_joins_the_next`, `::test_a_record_theirs_builds_whole_arrives_whole` |
| A callback that cannot be rebuilt is logged and passed over | `test_ib_async_bridge_carries_everything.py::test_a_callback_that_cannot_be_rebuilt_is_logged_and_passed_over` |
| A refusal reaches their `error` in the four-argument shape it declares | `test_a_refusal_reaches_their_wrapper.py` (3) |
| Every account the login holds crosses over | `test_ib_async_bridge_carries_everything.py::test_every_account_the_login_holds_crosses_over` |
| A wide order id placed elsewhere leaves requests numberable, and no id past the widest a request carries is handed out | `test_an_order_id_wider_than_a_request_leaves_requests_numberable.py` (2), `test_ib_async_depth.py::test_a_seeded_order_id_is_the_next_one_their_client_issues` |
| `readonly` reaches the session | `test_ib_async_transport.py::test_readonly_reaches_the_session_through_the_adapter` |
| An outage leaves the session connected; a session the engine ends fires `disconnectedEvent` once, stops delivery and refuses requests | `test_ib_async_transport.py::test_an_outage_leaves_the_session_open_and_an_end_closes_it` |
| Ending a session is not reported as a session that went away | `test_ib_async_transport.py::test_ending_a_session_is_not_a_session_that_went_away` |
| The handshake their client can send, `verifyRequest` and the three after it, is taken and answered by nothing, as over a gateway | `test_ib_async_transport.py::test_the_handshake_goes_unanswered_as_over_a_gateway` |
| Their suite's own `ib_async.IB()` is this package's in the runner | `test_ib_async_transport.py::test_their_suite_builds_this_packages_IB_where_it_builds_its_own` |
| Every request their client writes as a message reads back into the message it was, an order field for field, and reaches the engine as the request it names, an order whole whatever parts of the message it writes; an order carries what their client writes and nothing else; a message that does not read is refused with 320, and one naming no request is logged and unanswered; nothing is sent while not connected | `test_a_raw_message_is_the_request_it_names.py` (171) |
| Each of ib_async's eight notebook subjects has a notebook here, and each is Python that connects through `ib_async_dx` on a paper session | `test_the_notebooks_are_ib_asyncs_subjects.py` (2) |

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
