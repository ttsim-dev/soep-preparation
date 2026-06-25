"""Guard: no cleaning or combine module produces an integer-coded categorical.

Integer-coded categoricals fail on two counts (issues #63 and #66):

- a reader must look up what `0/1/2` mean, even though the integer *is* the value
  for the only cases this applied to — month numbers and genuine rating scales; and
- a pandas `category` column renders a missing value as numpy `NaN`, not the
  pyarrow `<NA>` the rest of the pipeline uses, so missings read as real data.

Both go away by storing these variables as plain `int[pyarrow]` (via
`object_to_int`, optionally with a `renaming`): the integer carries the meaning and
missings display as `<NA>`. This test AST-scans the cleaning and combine modules and
fails if `object_to_int_categorical` is called anywhere, so the dtype cannot return.
"""

import ast
from pathlib import Path

_SRC = Path(__file__).parent.parent / "src" / "soep_preparation"
_MODULE_DIRS = ("clean_modules", "combine_modules")


def _module_files() -> list[Path]:
    return [
        path
        for module_dir in _MODULE_DIRS
        for path in sorted((_SRC / module_dir).glob("*.py"))
        if path.name not in ("__init__.py", "task.py")
    ]


def _modules_calling_int_categorical() -> list[str]:
    callers: list[str] = []
    for path in _module_files():
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "object_to_int_categorical"
            ):
                callers.append(path.stem)
                break
    return callers


def test_no_module_uses_int_categorical() -> None:
    """No cleaning or combine module calls `object_to_int_categorical`."""
    assert _modules_calling_int_categorical() == []
