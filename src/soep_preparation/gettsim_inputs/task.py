"""Task to emit a GETTSIM-ready dataset and a documented input mapping."""

import json
from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import Product, task

from soep_preparation.config import BLD, MODULES, SURVEY_YEARS
from soep_preparation.final_dataset import create_final_dataset
from soep_preparation.gettsim_inputs.build import (
    build_gettsim_inputs,
    create_mapping_report,
)
from soep_preparation.gettsim_inputs.mapping import (
    BASE_MAPPING,
    INDEX_VARIABLES,
    get_soep_to_gettsim,
    mapping_periods,
)

_MAPPED_SOEP_VARIABLES = sorted(
    {
        soep_variable
        for soep_variable in BASE_MAPPING.values()
        if soep_variable is not None and soep_variable not in INDEX_VARIABLES
    }
)


@task(after="create_metadata")
def task_write_mapping_report(
    out_path: Annotated[Path, Product] = BLD / "gettsim_inputs" / "mapping_report.json",
) -> None:
    """Write the GETTSIM mapping report: union coverage plus a per-period breakdown.

    The `union` section reports the date-invariant `BASE_MAPPING`; the `periods` section
    reports, for each policy-date period, how many of that period's in-scope qnames have
    a SOEP source.

    Args:
        out_path: Where to write the JSON report.
    """
    report = {
        "union": create_mapping_report(mapping=BASE_MAPPING),
        "periods": [
            {
                "start_date": period.start_date.isoformat(),
                "end_date": period.end_date.isoformat(),
                "low_confidence": period.low_confidence,
                **{
                    key: create_mapping_report(
                        mapping=get_soep_to_gettsim(period.start_date)
                    )[key]
                    for key in (
                        "n_inputs_total",
                        "n_inputs_mapped",
                        "n_inputs_unmapped",
                    )
                },
            }
            for period in mapping_periods()
        ],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )


@task(after="create_metadata")
def task_build_gettsim_inputs(
    modules: Annotated[dict[str, pd.DataFrame], MODULES._entries],
    variables: Annotated[list[str], _MAPPED_SOEP_VARIABLES],
    survey_years: Annotated[list[int], SURVEY_YEARS],
    out_path: Annotated[Path, Product] = BLD
    / "gettsim_inputs"
    / "gettsim_inputs.arrow",
) -> None:
    """Build a GETTSIM-ready dataset from the mapped SOEP final variables.

    Merges the SOEP variables that `BASE_MAPPING` pairs with a GETTSIM qname into a
    final dataset, renames them to their GETTSIM qnames, and writes a flat `.arrow`
    file. The union mapping is used so the artifact carries every candidate column
    across the survey range; the date-specific subset is selected via
    `get_soep_to_gettsim(policy_date)` when handing data to GETTSIM.

    Args:
        modules: The cleaned and combined SOEP modules.
        variables: The mapped SOEP variables to merge into the final dataset.
        survey_years: Survey years to include.
        out_path: Where to write the GETTSIM-ready `.arrow` dataset.
    """
    final_dataset = create_final_dataset(
        modules=modules,
        variables=variables,
        survey_years=survey_years,
    )
    gettsim_inputs = build_gettsim_inputs(final_dataset, mapping=BASE_MAPPING)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gettsim_inputs.to_feather(out_path)
