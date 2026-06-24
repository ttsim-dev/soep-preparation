"""Build a disclosure-safe survey-year alignment report (issue #44).

For every variable that carries `survey_year`, summarize its value distribution per
survey year. Comparing the year-over-year movement against the questionnaire and
external benchmarks reveals whether a variable is aligned to the survey year or to the
previous year, so its reference period can be assigned. Only year-cells with at least
`min_cell` non-missing observations are described; smaller cells are suppressed.
"""

from collections.abc import Mapping

import pandas as pd
from pandas.api import types as pdtypes

from soep_preparation.config import POTENTIAL_INDEX_VARIABLES

_QUANTILES = (0.25, 0.5, 0.75)


def build_alignment_report(
    modules: Mapping[str, pd.DataFrame],
    min_cell: int = 30,
) -> dict:
    """Summarize each variable's value distribution per survey year.

    Args:
        modules: Cleaned modules keyed by module name.
        min_cell: Minimum non-missing observations for a year-cell to be summarized.

    Returns:
        A mapping from variable name to its module and per-survey-year summary. Numeric
        variables report count plus mean, median, and the quartiles; categorical and
        boolean variables report count plus category shares. Year-cells below `min_cell`
        report only their count and a `suppressed` flag.
    """
    report: dict[str, dict] = {}
    for module_name, module in modules.items():
        if "survey_year" not in module.columns:
            continue
        variables = [
            column
            for column in module.columns
            if column not in POTENTIAL_INDEX_VARIABLES
        ]
        for variable in variables:
            report[variable] = {
                "module": module_name,
                "by_survey_year": _summaries_by_survey_year(
                    survey_year=module["survey_year"],
                    values=module[variable],
                    min_cell=min_cell,
                ),
            }
    return report


def _summaries_by_survey_year(
    survey_year: pd.Series,
    values: pd.Series,
    min_cell: int,
) -> dict[int, dict]:
    frame = pd.DataFrame({"survey_year": survey_year, "value": values}).dropna(
        subset=["value"]
    )
    summaries: dict[int, dict] = {}
    for year, group in frame.groupby("survey_year", observed=True):
        year_key = int(year)  # ty: ignore[invalid-argument-type]
        count = len(group)
        if count < min_cell:
            summaries[year_key] = {"n": count, "suppressed": True}
        else:
            summaries[year_key] = _summarize_values(group["value"], count)
    return summaries


def _summarize_values(values: pd.Series, count: int) -> dict:
    if _is_categorical(values):
        shares = values.value_counts(normalize=True)
        return {
            "n": count,
            "shares": {
                str(category): round(share, 4) for category, share in shares.items()
            },
        }
    numeric = values.astype("float64")
    summary = {"n": count, "mean": round(float(numeric.mean()), 4)}
    for quantile, label in zip(_QUANTILES, ("p25", "median", "p75"), strict=True):
        summary[label] = round(float(numeric.quantile(quantile)), 4)
    return summary


def _is_categorical(values: pd.Series) -> bool:
    return (
        isinstance(values.dtype, pd.CategoricalDtype)
        or pdtypes.is_bool_dtype(values)
        or pdtypes.is_string_dtype(values)
        or not pdtypes.is_numeric_dtype(values)
    )
