#!/usr/bin/env python3
"""The published test counts against the suite they describe.

The book, the README and the evidence page state how many tests the suite
collects, how many of them run offline, how many need a live login, how many
a Python version condition excludes and how many need no session. This
derives the same figures from the suite itself — by collecting it, without
running it — and fails when a published figure differs, naming the file and
the stale figure.

    python3 scripts/check_published_counts.py

Needs pytest, and `ib_async_dx` importable as the suite's own collection
needs it; the tests workflow runs this against the installed wheel. The
figures come from what the tests carry, not from what one interpreter makes
of them: a test counts as needing a live login, or as version-excluded, by
the condition it is marked with, so the figures hold on every Python the
workflow runs.
"""

import contextlib
import io
import os
import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent

# Each entry: a page, the sentence carrying its figures, and what each
# captured number states. A sentence that moves or is reworded fails the
# check as a stale figure does, so this cannot quietly stop matching.
PUBLISHED = (
    (
        "docs/book/src/evidence.md",
        r"\| `tests/python` \| (\d+) \| (\d+) of them\. The other (\d+) run offline, "
        r"and (\d+) a Python version condition excludes \|",
        ("collected", "live", "offline", "excluded"),
    ),
    (
        "docs/book/src/introduction.md",
        r"suite: (\d+)\ntests, (\d+) of which run offline[\s\S]*?(\d+) that need a live\n"
        r"login, and (\d+) a Python version condition excludes",
        ("collected", "offline", "live", "excluded"),
    ),
    ("README.md", r"\| Python \| (\d+) \| No \|", ("no_session",)),
    ("README.md", r"\| Python, live \| (\d+) \| Yes \|", ("live",)),
    ("docs/book/src/drop-in.md", r"The other (\d+) tests need no session", ("no_session",)),
)

STATED = {
    "collected": "the suite collects {} tests",
    "offline": "{} of them run offline",
    "live": "{} need a live login",
    "excluded": "a Python version condition excludes {}",
    "no_session": "{} need no session",
}


class _Census:
    """What the suite collects, and what the collected tests carry."""

    def __init__(self):
        self.collected = 0
        self.live = 0
        self.excluded = 0

    def pytest_collection_modifyitems(self, items):
        self.collected = len(items)
        for item in items:
            reasons = [
                marker.kwargs.get("reason", "")
                for marker in item.own_markers
                if marker.name == "skipif"
            ]
            if any("IB_USERNAME" in reason for reason in reasons):
                self.live += 1
            elif reasons:
                self.excluded += 1


def derive():
    census = _Census()
    os.chdir(ROOT)
    with contextlib.redirect_stdout(io.StringIO()):
        status = pytest.main(
            ["tests/python", "--collect-only", "-q", "-p", "no:cacheprovider"],
            plugins=[census],
        )
    if status != 0 or census.collected == 0:
        sys.exit("collecting tests/python failed; the figures cannot be derived")
    return {
        "collected": census.collected,
        "live": census.live,
        "excluded": census.excluded,
        "offline": census.collected - census.live - census.excluded,
        "no_session": census.collected - census.live,
    }


def main() -> int:
    actual = derive()
    stale = []
    for path, sentence, figures in PUBLISHED:
        text = (ROOT / path).read_text()
        match = re.search(sentence, text)
        if match is None:
            stale.append(f"{path}: the sentence stating its counts no longer matches")
            continue
        for number, figure in zip(match.groups(), figures, strict=True):
            if int(number) != actual[figure]:
                stale.append(
                    f"{path} states {number}; {STATED[figure].format(actual[figure])}"
                )
    for line in stale:
        print(line)
    if stale:
        return 1
    print(
        f"{actual['collected']} tests collected: {actual['offline']} run offline, "
        f"{actual['live']} need a live login, a Python version condition excludes "
        f"{actual['excluded']}, {actual['no_session']} need no session — the "
        "published counts agree"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
