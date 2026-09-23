"""Run ib_async's own test suite against this engine.

`ib_async_dx.IB` is ib_async's `IB` with the engine in place of its
`Client`/`Connection`, so their library runs unmodified. Their suite is the
strongest available statement of whether it does.

Their tests are not vendored. Point the run at a checkout of theirs:

    git clone https://github.com/ib-api-reloaded/ib_async /tmp/ib_async
    git -C /tmp/ib_async checkout ab629f34c1    # 2.1.0
    cp tests/ib_async_upstream/conftest.py /tmp/ib_async/tests/
    IB_USERNAME=… IB_PASSWORD=… pytest /tmp/ib_async/tests \\
        -o asyncio_mode=auto \\
        -o asyncio_default_fixture_loop_scope=session \\
        -o asyncio_default_test_loop_scope=session

Both loop scopes are needed. Their session-scoped connection fixture and their
tests must share one event loop, or the callbacks land on a loop that is not
running while the test waits on them. pandas has to be installed too: their
`test_contract.py` imports it, and without it the run stops at collection.
"""
import os

import pytest_asyncio

import ib_async_dx


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def ib():
    ib = ib_async_dx.IB()
    await ib.connectAsync(
        username=os.environ["IB_USERNAME"],
        password=os.environ["IB_PASSWORD"],
    )
    yield ib
    ib.disconnect()
