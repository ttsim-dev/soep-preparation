"""Guard: `object_to_int_categorical` is reserved for numeric scales and months.

Integer-coded categoricals force a reader to look up what `0/1/2` mean (issue #66). A
small categorical whose outcomes have meaningful names should therefore use
`object_to_str_categorical` (keep the labels as strings), NOT integer codes. The two
legitimate exceptions are:

- genuine numeric rating scales ("on a scale from 0 to 10 / 1 to 7 …"), where the
  integer *is* the value; and
- month mappings (1-12), where the integer is the natural, self-explanatory key
  (and unifies German and English month labels in the birth-month combine).

This test AST-scans the cleaning and combine modules and fails if the helper is used
for any output column not on the reviewed allow-list below. A new usage must be
justified (added here) or rewritten as a string-categorical.
"""

import ast
from pathlib import Path

_SRC = Path(__file__).parent.parent / "src" / "soep_preparation"
_MODULE_DIRS = ("clean_modules", "combine_modules")

# (module, output column) pairs allowed to use `object_to_int_categorical`: month
# mappings and numeric rating scales only.
_ALLOWED: frozenset[tuple[str, str]] = frozenset(
    {
        *{("biobirth", f"tmp_birth_month_child_{i}") for i in range(1, 17)},
        ("bioedu", "birth_month_bioedu"),
        ("ppathl", "birth_month_ppathl"),
        ("pgen", "month_interview"),
        ("pequiv", "med_health_satisfaction_pequiv"),
        ("pl", "political_spectrum_left_to_right"),
        ("pl", "life_satisfaction_low_to_high"),
        ("pl", "norm_child_suffers_under_6_low_to_high_2018"),
        ("pl", "norm_child_suffers_under_3_low_to_high_2018"),
        ("pl", "norm_marry_when_together_low_to_high_2018"),
        ("pl", "norm_genders_similar_low_to_high_2018"),
        ("pl", "trust_public_admin_low_to_high"),
        ("pl", "trust_government_low_to_high"),
    }
)


def _module_files() -> list[Path]:
    return [
        path
        for module_dir in _MODULE_DIRS
        for path in sorted((_SRC / module_dir).glob("*.py"))
        if path.name not in ("__init__.py", "task.py")
    ]


def collect_int_categorical_usages() -> set[tuple[str, str]]:
    """Return every `(module, column)` assigned via `object_to_int_categorical`."""
    usages: set[tuple[str, str]] = set()
    for path in _module_files():
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Assign) and len(node.targets) == 1):
                continue
            target = node.targets[0]
            uses_helper = any(
                isinstance(call.func, ast.Name)
                and call.func.id == "object_to_int_categorical"
                for call in ast.walk(node.value)
                if isinstance(call, ast.Call)
            )
            if (
                uses_helper
                and isinstance(target, ast.Subscript)
                and isinstance(target.slice, ast.Constant)
                and isinstance(target.slice.value, str)
            ):
                usages.add((path.stem, target.slice.value))
    return usages


def test_no_disallowed_int_categorical_usage() -> None:
    """Every `object_to_int_categorical` output is a reviewed scale or month var."""
    disallowed = sorted(collect_int_categorical_usages() - _ALLOWED)
    assert disallowed == []


def test_no_stale_allow_list_entries() -> None:
    """The allow-list contains no entry that no longer uses the helper."""
    stale = sorted(_ALLOWED - collect_int_categorical_usages())
    assert stale == []
