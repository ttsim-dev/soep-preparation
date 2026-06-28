"""Guard: no cleaning or combine module produces an integer-coded categorical.

Integer-coded categoricals fail on two counts (issues #63 and #66):

- a reader must look up what `0/1/2` mean, even though the integer *is* the value
  for the only cases this applies to — month numbers and genuine rating scales; and
- a pandas `category` column renders a missing value as numpy `NaN`, not the
  pyarrow `<NA>` the rest of the pipeline uses, so missings read as real data.

Two AST guards keep the dtype out:

- `object_to_int_categorical` is removed, so no module may call it.
- `object_to_int(renaming={...})` with a *small* inline label→int dict is the same
  anti-pattern through the back door: a few meaningfully-named outcomes coded to
  integers. Per #66 these belong in a string `category` (`object_to_str_categorical`),
  not an integer code. Genuine rating/Likert scales are exempt — they have more than
  `_MAX_INT_CODED_OUTCOMES` points and the integer is the scale value — so only inline
  renamings with at most that many entries are flagged.

The second guard is a **conservative heuristic tripwire, not a semantic proof**: an
outcome count cannot decide whether an integer genuinely *is* the value. It can
false-positive on a real ≤5-point numeric rating scale and false-negative on a
>5-outcome qualitative coding or a renaming passed by name (which it does not inspect).
`test_int_renaming_threshold_*` characterise those edges. The robust replacement is
explicit per-variable scale metadata (a `scale_is_numeric_value` flag with range and
direction) plus a registered allowlist of known scales; until then, treat a guard
failure as a prompt to check the variable, not a proof of misuse.
"""

import ast
from pathlib import Path

_SRC = Path(__file__).parent.parent / "src" / "soep_preparation"
_MODULE_DIRS = ("clean_modules", "combine_modules")

# Above this many outcomes a label→int mapping is a genuine rating/Likert scale, where
# the integer is the scale value (#66's stated counterexample). At or below it, a few
# meaningfully-named outcomes coded to integers is the banned int-categorical pattern.
_MAX_INT_CODED_OUTCOMES = 5


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


def _small_int_renaming_lines(tree: ast.AST) -> list[int]:
    """Line numbers of `object_to_int` calls with a small inline label→int renaming.

    Flags a call when its `renaming` argument is an inline dict literal of at most
    `_MAX_INT_CODED_OUTCOMES` entries that maps string labels to integer codes. A
    `renaming` passed by name (e.g. `month_mapping.de`) is not an inline dict and is
    never flagged.
    """
    violations: list[int] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "object_to_int"
        ):
            continue
        for keyword in node.keywords:
            if keyword.arg != "renaming" or not isinstance(keyword.value, ast.Dict):
                continue
            entries = keyword.value
            keys_are_strings = all(
                isinstance(key, ast.Constant) and isinstance(key.value, str)
                for key in entries.keys
            )
            values_are_ints = all(
                isinstance(value, ast.Constant)
                and isinstance(value.value, int)
                # `bool` is an `int` subclass; a True/False map is a different thing.
                and not isinstance(value.value, bool)
                for value in entries.values
            )
            if (
                keys_are_strings
                and values_are_ints
                and 0 < len(entries.keys) <= _MAX_INT_CODED_OUTCOMES
            ):
                violations.append(node.lineno)
    return violations


def _modules_coding_small_categorical_as_int() -> list[str]:
    return [
        path.stem
        for path in _module_files()
        if _small_int_renaming_lines(ast.parse(path.read_text()))
    ]


def test_no_module_uses_int_categorical() -> None:
    """No cleaning or combine module calls `object_to_int_categorical`."""
    assert _modules_calling_int_categorical() == []


def test_no_module_codes_small_categorical_as_int() -> None:
    """No module codes a small meaningfully-named categorical to integers (#66)."""
    assert _modules_coding_small_categorical_as_int() == []


def test_detector_flags_small_meaningful_int_renaming() -> None:
    """A four-outcome label→int `object_to_int` renaming is flagged as a violation."""
    snippet = (
        'out["some_small_scale"] = object_to_int(\n'
        '    series=raw_data["somevar"],\n'
        "    renaming={\n"
        '        "[1] Label one": 1,\n'
        '        "[2] Label two": 2,\n'
        '        "[3] Label three": 3,\n'
        '        "[4] Label four": 4,\n'
        "    },\n"
        ")\n"
    )
    assert len(_small_int_renaming_lines(ast.parse(snippet))) == 1


def test_detector_allows_genuine_rating_scale_renaming() -> None:
    """A seven-point rating scale (Likert) coded to integers is not flagged."""
    pairs = "\n".join(
        f'        "[{code}] Label {code}": {code},' for code in range(1, 8)
    )
    snippet = (
        'out["satisfaction_scale"] = object_to_int(\n'
        '    series=raw_data["plh0001"],\n'
        f"    renaming={{\n{pairs}\n    }},\n"
        ")\n"
    )
    assert _small_int_renaming_lines(ast.parse(snippet)) == []


def test_detector_allows_renaming_passed_by_name() -> None:
    """A `renaming` passed by name (e.g. a month map) is never flagged, any size."""
    snippet = (
        'out["birth_month"] = object_to_int(\n'
        '    raw_data["kidmon01"], renaming=month_mapping.de\n'
        ")\n"
    )
    assert _small_int_renaming_lines(ast.parse(snippet)) == []


def test_int_renaming_threshold_false_positive_on_a_five_point_numeric_scale() -> None:
    """Known limit: a real five-point numeric rating scale is flagged (false positive).

    The integer here *is* the value, so plain int is correct, yet the count heuristic
    flags it. This documents why a guard failure is a prompt to check, not a proof.
    """
    pairs = "\n".join(
        f'        "[{code}] Rating {code}": {code},' for code in range(1, 6)
    )
    snippet = f'out["rating"] = object_to_int(\n    renaming={{\n{pairs}\n    }},\n)\n'
    assert _small_int_renaming_lines(ast.parse(snippet)) == [1]


def test_int_renaming_threshold_false_negative_on_a_six_outcome_qualitative() -> None:
    """Known limit: a six-outcome qualitative coding is not flagged (false negative).

    Above the threshold the integer codes are arbitrary qualitative labels that #66
    would want as a string categorical, but the count heuristic waves them through.
    """
    pairs = "\n".join(
        f'        "[{code}] Qualitative {code}": {code},' for code in range(1, 7)
    )
    snippet = f'out["qual"] = object_to_int(\n    renaming={{\n{pairs}\n    }},\n)\n'
    assert _small_int_renaming_lines(ast.parse(snippet)) == []
