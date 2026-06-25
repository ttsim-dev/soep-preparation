"""Guard: every `create_dummy` comparison in pgen targets a real category.

A dummy built with `create_dummy` compares a categorical against a literal. If the
categorical's labels are translated to English (or kept under a German allow-list
label) but the literal is not updated in lockstep, the dummy silently becomes
all-`False`/NA: no exception, and — because the output stays boolean — no change to
the metadata inventory, so the pipeline gate cannot catch it.

This test reads the comparison literals straight from the source and checks each
against the category set of the series it compares:
- translated columns (`employment_status`, `labor_force_status`, `first_nationality`)
  against the English values of their translation dicts;
- the kept-German `occupation_status` against the committed metadata inventory.

Comparisons whose value is computed at runtime (e.g. `_self_employed_occupations`)
are skipped — they cannot be resolved statically.
"""

import ast
from pathlib import Path

import yaml

_SRC = Path(__file__).parent.parent / "src" / "soep_preparation"
_PGEN = _SRC / "clean_modules" / "pgen.py"
_MAPPING = _SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"

# Translated column -> the module-level dict whose English values are its categories.
_TRANSLATED_COLUMN_DICTS = {
    "employment_status": "_EMPLOYMENT_STATUS_EN",
    "labor_force_status": "_LABOR_FORCE_STATUS_EN",
    "first_nationality": "_FIRST_NATIONALITY_EN",
}


def _find_categories(node: object) -> set[str] | None:
    """Return the first `categories` list found anywhere in a metadata entry."""
    if isinstance(node, dict):
        categories = node.get("categories")
        if isinstance(categories, list):
            return {str(category) for category in categories}
        for value in node.values():
            found = _find_categories(value)
            if found is not None:
                return found
    return None


def _occupation_status_categories() -> set[str]:
    mapping = yaml.safe_load(_MAPPING.read_text())
    categories = _find_categories(mapping["occupation_status"])
    if categories is None:
        msg = "occupation_status has no categories in the metadata inventory."
        raise AssertionError(msg)
    return categories


def _dict_value_strings(dict_node: ast.Dict) -> set[str]:
    return {
        value.value
        for value in dict_node.values
        if isinstance(value, ast.Constant) and isinstance(value.value, str)
    }


def _out_column(node: ast.expr) -> str | None:
    """Return the column name if `node` is an `out["col"]` subscript, else None."""
    if (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == "out"
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
    ):
        return node.slice.value
    return None


def _allowed_categories(tree: ast.Module) -> dict[str, set[str]]:
    """Map each checkable column to its allowed category labels."""
    dict_values: dict[str, set[str]] = {}
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Dict)
        ):
            dict_values[node.targets[0].id] = _dict_value_strings(node.value)
    allowed = {
        column: dict_values[dict_name]
        for column, dict_name in _TRANSLATED_COLUMN_DICTS.items()
    }
    allowed["occupation_status"] = _occupation_status_categories()
    return allowed


def _helper_param_columns(tree: ast.Module) -> dict[str, dict[str, str]]:
    """Map `func_name -> {param: column}` from calls passing `out["col"]` by keyword."""
    param_columns: dict[str, dict[str, str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            for keyword in node.keywords:
                column = _out_column(keyword.value)
                if keyword.arg is not None and column is not None:
                    param_columns.setdefault(node.func.id, {})[keyword.arg] = column
    return param_columns


def _resolve_series_column(
    series_node: ast.expr,
    func: ast.FunctionDef,
    param_columns: dict[str, dict[str, str]],
) -> str | None:
    column = _out_column(series_node)
    if column is not None:
        return column
    if isinstance(series_node, ast.Name):
        return param_columns.get(func.name, {}).get(series_node.id)
    return None


def _resolve_literals(
    value_node: ast.expr, local_lists: dict[str, list[str]]
) -> list[str] | None:
    if isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
        return [value_node.value]
    if isinstance(value_node, ast.List):
        return [
            element.value
            for element in value_node.elts
            if isinstance(element, ast.Constant) and isinstance(element.value, str)
        ]
    if isinstance(value_node, ast.Name):
        return local_lists.get(value_node.id)
    return None


def _local_string_lists(func: ast.FunctionDef) -> dict[str, list[str]]:
    lists: dict[str, list[str]] = {}
    for node in func.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.List)
        ):
            lists[node.targets[0].id] = [
                element.value
                for element in node.value.elts
                if isinstance(element, ast.Constant) and isinstance(element.value, str)
            ]
    return lists


def _call_kwargs(call: ast.Call) -> dict[str, ast.expr]:
    return {keyword.arg: keyword.value for keyword in call.keywords if keyword.arg}


def _invalid_comparisons_for_call(
    call: ast.Call,
    func: ast.FunctionDef,
    allowed: dict[str, set[str]],
    param_columns: dict[str, dict[str, str]],
    local_lists: dict[str, list[str]],
) -> list[str]:
    kwargs = _call_kwargs(call)
    series_node = kwargs.get("series")
    value_node = kwargs.get("value_for_comparison")
    if series_node is None or value_node is None:
        return []
    column = _resolve_series_column(series_node, func, param_columns)
    if column not in allowed:
        return []
    literals = _resolve_literals(value_node, local_lists)
    if literals is None:
        return []
    type_node = kwargs.get("comparison_type")
    comparison_type = (
        type_node.value if isinstance(type_node, ast.Constant) else "equal"
    )
    categories = allowed[column]
    problems: list[str] = []
    for literal in literals:
        if comparison_type == "startswith" and not any(
            category.startswith(literal) for category in categories
        ):
            problems.append(f"{column}: no category starts with {literal!r}")
        elif comparison_type in ("equal", "neq", "isin") and literal not in categories:
            problems.append(f"{column}: {literal!r} is not a category")
    return problems


def collect_invalid_comparisons() -> list[str]:
    """List `create_dummy` comparisons whose literal is not a real category."""
    tree = ast.parse(_PGEN.read_text())
    allowed = _allowed_categories(tree)
    param_columns = _helper_param_columns(tree)

    problems: list[str] = []
    for func in (node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)):
        local_lists = _local_string_lists(func)
        for call in (node for node in ast.walk(func) if isinstance(node, ast.Call)):
            if isinstance(call.func, ast.Name) and call.func.id == "create_dummy":
                problems.extend(
                    _invalid_comparisons_for_call(
                        call, func, allowed, param_columns, local_lists
                    )
                )
    return problems


def test_pgen_dummy_comparisons_target_existing_categories() -> None:
    """Every static `create_dummy` literal in pgen matches a real category label."""
    assert collect_invalid_comparisons() == []
