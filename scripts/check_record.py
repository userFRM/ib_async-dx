#!/usr/bin/env python3
"""Every engine callback ib_async handles reaches ib_async-dx.

    python scripts/check_record.py

Each method of the engine's `Wrapper` trait has an empty default body, so a callback the
crate forgot to override would be dropped without a word. This lists the methods of
`pub trait Wrapper` in the engine the crate is built against, and of `impl Wrapper for
Capture` in src/record.rs, and fails on any difference outside IGNORED. It also fails on an
IGNORED name that the crate now overrides or the engine no longer has, so the list stays
the list of what is left out.
"""

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

IGNORED = {
    # ib_async's handler is `pass`.
    "connect_ack": "ib_async's handler is pass",
    "update_account_time": "ib_async's handler is pass",
    "position_multi": "ib_async's handler is pass",
    "position_multi_end": "ib_async's handler is pass",
    "order_bound": "ib_async's handler is pass",
    "delta_neutral_validation": "ib_async's handler is pass",
    "soft_dollar_tiers": "ib_async's handler is pass",
    "family_codes": "ib_async's handler is pass",
    # ib_async has no handler.
    "display_group_list": "no ib_async handler",
    "display_group_updated": "no ib_async handler",
    "verify_message_api": "no ib_async handler",
    "verify_completed": "no ib_async handler",
    "verify_and_auth_message_api": "no ib_async handler",
    "verify_and_auth_completed": "no ib_async handler",
    "reroute_mkt_data_req": "no ib_async handler",
    "reroute_mkt_depth_req": "no ib_async handler",
    "win_error": "no ib_async handler",
    "replace_fa_end": "no ib_async handler",
    # ib_async's nextValidId is pass, and no question waits for it.
    "next_valid_id": "ib_async's handler is pass, and nothing waits for it",
    # error_from, which says what each error is about, supersedes it.
    "error": "error_from supersedes it",
}


def block(text, header):
    """The methods declared at one indent inside the block that `header` opens."""
    m = re.search(header, text, re.M)
    if not m:
        sys.exit(f"check_record: no {header!r}")
    end = re.search(r"^\}", text[m.end() :], re.M)
    return set(re.findall(r"^    fn (\w+)", text[m.end() : m.end() + end.start()], re.M))


def engine_wrapper():
    meta = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(ROOT / "Cargo.toml")],
        check=True,
        capture_output=True,
        text=True,
    )
    pkg = next(p for p in json.loads(meta.stdout)["packages"] if p["name"] == "ibkr-dx")
    return Path(pkg["manifest_path"]).parent / "src" / "api" / "wrapper.rs"


def main():
    trait = block(engine_wrapper().read_text(), r"^pub trait Wrapper \{")
    capture = block((ROOT / "src" / "record.rs").read_text(), r"^impl (?:e::)?Wrapper for Capture \{")
    problems = []
    for name in sorted(trait - capture - IGNORED.keys()):
        problems.append(f"{name}: the engine calls it and Capture does not override it")
    for name in sorted(capture - trait):
        problems.append(f"{name}: Capture overrides what the engine's Wrapper does not declare")
    for name in sorted(IGNORED.keys() & capture):
        problems.append(f"{name}: ignored, but Capture overrides it")
    for name in sorted(IGNORED.keys() - trait):
        problems.append(f"{name}: ignored, but the engine's Wrapper no longer declares it")
    if problems:
        sys.exit("check_record:\n  " + "\n  ".join(problems))
    print(f"check_record: {len(capture)} of the engine's {len(trait)} callbacks recorded, {len(IGNORED)} left out")


if __name__ == "__main__":
    main()
