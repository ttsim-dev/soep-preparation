"""Generate `variables.md` from the variable-to-metadata mapping at docs-build time.

Run from the `docs/` directory before `jupyter book build`/`start` (the `build-docs` /
`view-docs` pixi tasks do this). Reads the committed
`variable_to_metadata_mapping.yaml` and renders one table per module, so the variable
catalogue in the built docs always reflects the current pipeline output. The generated
page is build output and is not committed.
"""

from pathlib import Path

import yaml

_MAPPING_PATH = (
    Path(__file__).parent.parent
    / "src"
    / "soep_preparation"
    / "create_metadata"
    / "variable_to_metadata_mapping.yaml"
)
_OUTPUT_PATH = Path(__file__).parent / "variables.md"


def _dtype_label(dtype: object) -> str:
    """Render a variable's dtype as a short label.

    Categorical dtypes are serialized as a dict in the mapping; collapse them to one
    word.
    """
    if isinstance(dtype, dict):
        return "categorical"
    return str(dtype)


def _survey_years_label(survey_years: list[int] | None) -> str:
    """Render the available survey years as a compact first-to-last range.

    Returns "—" for a variable with no per-wave availability: those from
    biographical/design modules, which have no `survey_year` dimension (`None`), and
    those with no non-missing observation in any wave (`[]`).
    """
    if not survey_years:
        return "—"
    return f"{min(survey_years)}-{max(survey_years)}"


def _render(mapping: dict[str, dict]) -> str:
    """Render the mapping as a Markdown page with one sorted table per module."""
    by_module: dict[str, list[tuple[str, str, str]]] = {}
    for name, meta in sorted(mapping.items()):
        module = meta.get("module", "")
        by_module.setdefault(module, []).append(
            (
                name,
                _dtype_label(meta.get("dtype")),
                _survey_years_label(meta.get("survey_years")),
            )
        )
    lines = [
        "# Variables",
        "",
        f"{len(mapping)} final variables across {len(by_module)} modules, generated "
        "from `variable_to_metadata_mapping.yaml`.",
        "",
        "A **—** in the *Survey years* column marks a variable with no per-wave "
        "availability: biographical and design modules have no `survey_year` "
        "dimension.",
        "",
    ]
    for module in sorted(by_module):
        lines += [
            f"## `{module}`",
            "",
            "| Variable | Dtype | Survey years |",
            "| --- | --- | --- |",
        ]
        lines += [
            f"| `{name}` | {dtype} | {years} |"
            for name, dtype, years in by_module[module]
        ]
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    """Read the mapping and write the rendered variables page."""
    mapping = yaml.safe_load(_MAPPING_PATH.read_text(encoding="utf-8"))
    _OUTPUT_PATH.write_text(_render(mapping), encoding="utf-8")


if __name__ == "__main__":
    main()
