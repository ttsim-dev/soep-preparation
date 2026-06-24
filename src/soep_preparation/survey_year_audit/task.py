"""Task: write the disclosure-safe survey-year alignment report (issue #44)."""

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import Product

from soep_preparation.config import BLD, MODULES
from soep_preparation.survey_year_audit.report import build_alignment_report


def task_survey_year_alignment_report(
    modules: Annotated[dict[str, pd.DataFrame], MODULES._entries],
    out_path: Annotated[Path, Product] = BLD
    / "survey_year_audit"
    / "alignment_report.json",
) -> None:
    """Build and write the per-variable, per-survey-year distribution report.

    Reads every cleaned module and emits a disclosure-safe summary that an analyst uses
    to decide each variable's reference period (survey year vs previous year, #44).

    Args:
        modules: All cleaned modules, keyed by module name.
        out_path: Destination for the JSON report.
    """
    report = build_alignment_report(modules=modules)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as file:
        json.dump(
            report, file, indent=2, ensure_ascii=False, sort_keys=True, default=str
        )
