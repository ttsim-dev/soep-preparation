"""Guard: every static `create_dummy` comparison targets a real category.

`create_dummy(series, value_for_comparison, comparison_type)` builds a boolean column
by comparing a categorical against a literal. If the categorical's labels and the
comparison literal drift apart — a label translated to English while the literal stays
German, a renamed parenthetical — the dummy silently becomes all-`False`/NA: no
exception, and (because the output stays boolean) no change to the metadata catalogue,
so the pipeline gate cannot catch it.

This data-free guard scans every cleaning and combine module, resolves each
`create_dummy`'s series to an `out["col"]` output column (directly or through a helper
parameter), and checks the comparison literal against that column's category set in the
committed metadata catalogue. Both keyword and positional call forms are handled.

Comparisons whose series cannot be resolved to a categorical output column, or whose
value is computed at runtime (e.g. `_self_employed_occupations(...)`), are skipped — a
static scan cannot check them. A runtime fail-closed check inside `create_dummy` would
close that remaining gap.
"""

import ast
from pathlib import Path

import yaml

_SRC = Path(__file__).parent.parent / "src" / "soep_preparation"
_MAPPING = _SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
_MODULE_DIRS = ("clean_modules", "combine_modules")
_CATEGORY_COMPARISONS = ("equal", "neq", "isin", "startswith")


def _module_files() -> list[Path]:
    return [
        path
        for module_dir in _MODULE_DIRS
        for path in sorted((_SRC / module_dir).glob("*.py"))
        if path.name not in ("__init__.py", "task.py")
    ]


def _find_categories(node: object) -> set[str] | None:
    if isinstance(node, dict):
        categories = node.get("categories")
        if isinstance(categories, list):
            return {str(category) for category in categories}
        for value in node.values():
            found = _find_categories(value)
            if found is not None:
                return found
    return None


def _categorical_categories() -> dict[str, set[str]]:
    """Map every categorical final variable to its category set, from the catalogue."""
    mapping = yaml.safe_load(_MAPPING.read_text())
    out: dict[str, set[str]] = {}
    for variable, metadata in mapping.items():
        categories = _find_categories(metadata)
        if categories is not None:
            out[variable] = categories
    return out


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


def _helper_param_columns(tree: ast.Module) -> dict[str, dict[str, str]]:
    """Map `func_name -> {param: column}` from calls passing `out["col"]` arguments."""
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


def _create_dummy_arguments(
    call: ast.Call,
) -> tuple[ast.expr | None, ast.expr | None, str]:
    """Return (series, value, comparison_type) for keyword or positional calls."""
    kwargs = {keyword.arg: keyword.value for keyword in call.keywords if keyword.arg}
    series = kwargs.get("series")
    if series is None and len(call.args) >= 1:
        series = call.args[0]
    value = kwargs.get("value_for_comparison")
    if value is None and len(call.args) >= 2:
        value = call.args[1]
    type_node = kwargs.get("comparison_type")
    if type_node is None and len(call.args) >= 3:
        type_node = call.args[2]
    comparison_type = (
        type_node.value
        if isinstance(type_node, ast.Constant) and isinstance(type_node.value, str)
        else "equal"
    )
    return series, value, comparison_type


def _invalid_comparisons_for_call(
    call: ast.Call,
    func: ast.FunctionDef,
    categories_by_column: dict[str, set[str]],
    param_columns: dict[str, dict[str, str]],
    local_lists: dict[str, list[str]],
) -> list[str]:
    series_node, value_node, comparison_type = _create_dummy_arguments(call)
    if series_node is None or value_node is None:
        return []
    if comparison_type not in _CATEGORY_COMPARISONS:
        return []
    column = _resolve_series_column(series_node, func, param_columns)
    if column not in categories_by_column:
        return []
    literals = _resolve_literals(value_node, local_lists)
    if literals is None:
        return []
    categories = categories_by_column[column]
    problems: list[str] = []
    for literal in literals:
        if comparison_type == "startswith" and not any(
            category.startswith(literal) for category in categories
        ):
            problems.append(f"{column}: no category starts with {literal!r}")
        elif comparison_type != "startswith" and literal not in categories:
            problems.append(f"{column}: {literal!r} is not a category")
    return problems


def collect_invalid_comparisons() -> list[str]:
    """List `create_dummy` comparisons whose literal is not a real category."""
    categories_by_column = _categorical_categories()
    problems: list[str] = []
    for path in _module_files():
        tree = ast.parse(path.read_text())
        param_columns = _helper_param_columns(tree)
        for func in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)):
            local_lists = _local_string_lists(func)
            for call in (n for n in ast.walk(func) if isinstance(n, ast.Call)):
                if isinstance(call.func, ast.Name) and call.func.id == "create_dummy":
                    problems.extend(
                        _invalid_comparisons_for_call(
                            call,
                            func,
                            categories_by_column,
                            param_columns,
                            local_lists,
                        )
                    )
    return problems


def test_dummy_comparisons_target_existing_categories() -> None:
    """Every static `create_dummy` literal matches a real category label."""
    assert collect_invalid_comparisons() == []
