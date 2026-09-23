"""The notebooks are ib_async's own subjects, written against ib_async_dx.

ib_async ships eight notebooks. Each has one here, written as ib_async's own is,
with the one cell that differs: the import names `ib_async_dx`, and `connect`
takes the login and opens a paper session.
"""

import json
import pathlib

NOTEBOOKS = pathlib.Path(__file__).parents[2] / "notebooks"

#: ib_async's own notebooks, at 2.1.0.
THEIRS = [
    "bar_data", "basics", "contract_details", "market_depth", "option_chain",
    "ordering", "scanners", "tick_data",
]


def _code(notebook):
    cells = json.loads(notebook.read_text())["cells"]
    return ["".join(cell["source"]) for cell in cells if cell["cell_type"] == "code"]


def test_each_of_their_subjects_has_a_notebook():
    assert sorted(n.stem for n in NOTEBOOKS.glob("*.ipynb")) == THEIRS


def test_each_notebook_is_python_that_connects_through_ib_async_dx():
    for notebook in NOTEBOOKS.glob("*.ipynb"):
        code = _code(notebook)
        for cell in code:
            compile(cell, str(notebook), "exec")
        assert "from ib_async_dx import IB, util" in code[0], notebook.stem
        assert "paper=True" in code[0], notebook.stem
        assert not any("import ib_async\n" in c or "from ib_async " in c for c in code), notebook.stem
