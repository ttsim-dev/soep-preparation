"""Configuration of the soep preparation."""

SOEP_VERSION = "V41"
SURVEY_YEARS = [*range(1984, 2024 + 1)]


import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pytask import DataCatalog

from soep_preparation.utilities.general import get_combine_module_names as gcmn
from soep_preparation.utilities.general import get_raw_data_file_names as grdfn
from soep_preparation.utilities.general import load_script

SRC = Path(__file__).parent.resolve()
ROOT = SRC.parent.parent.resolve()
BLD = ROOT.joinpath("bld").resolve()
DATA_ROOT = ROOT.joinpath("data").resolve()
TEST_DIR = ROOT.joinpath("tests").resolve()

# Modules whose raw data file is not part of every SOEP distribution. A missing file
# skips the module (and any combine script drawing on it) with a warning; for every
# other module it aborts the run.
OPTIONAL_RAW_DATA_MODULES = frozenset({"cirdef"})


def get_raw_data_file_names() -> list[str]:
    """Get the modules processed in this run.

    Returns:
        Names of the cleaning scripts whose raw data file is present.
    """
    return grdfn(
        directory=SRC / "clean_modules",
        data_root=DATA_ROOT,
        soep_version=SOEP_VERSION,
        optional_modules=OPTIONAL_RAW_DATA_MODULES,
    )


def get_combine_module_names() -> list[str]:
    """Get the combine scripts processed in this run.

    Returns:
        Names of the combine scripts all of whose modules are processed.
    """
    return gcmn(
        directory=SRC / "combine_modules",
        available_modules=get_raw_data_file_names(),
    )


RAW_DATA_FILES = DataCatalog(name="raw_pandas")
MODULES = DataCatalog(name="modules")


_METADATA_DTYPE = dict[
    str,
    dict[
        Literal["module", "dtype", "survey_years", "reference"],
        dict[str, Any] | list[int] | str,
    ],
]
METADATA: _METADATA_DTYPE = yaml.safe_load(
    (SRC / "create_metadata" / "variable_to_metadata_mapping.yaml").open(
        "r", encoding="utf-8"
    )
)

POTENTIAL_INDEX_VARIABLES = ["hh_id", "hh_id_original", "p_id", "survey_year"]

# Opt-in gate for the (eventually expensive) wealth-imputation subsystem. Off by
# default so `pixi run pytask` skips it; enable with `SOEP_WEALTH_IMPUTATION=1`
# (or the `pixi run wealth` task). Wealth task modules define their tasks only when
# this is True, so nothing wealth-related is collected by default.
RUN_WEALTH_IMPUTATION = os.environ.get("SOEP_WEALTH_IMPUTATION", "0") != "0"

# Opt-in gate for the wealth-imputation layer-ablation diagnostic. Off even when the
# wealth subsystem runs, because it re-runs the projection once per layer configuration
# (one full set of refits each). Enable with `SOEP_WEALTH_LAYER_ABLATION=1` to attribute
# the projection spread to its layers.
RUN_WEALTH_LAYER_ABLATION = os.environ.get("SOEP_WEALTH_LAYER_ABLATION", "0") != "0"


__all__ = [
    "BLD",
    "DATA_ROOT",
    "MODULES",
    "OPTIONAL_RAW_DATA_MODULES",
    "RAW_DATA_FILES",
    "ROOT",
    "RUN_WEALTH_IMPUTATION",
    "RUN_WEALTH_LAYER_ABLATION",
    "SOEP_VERSION",
    "SRC",
    "SURVEY_YEARS",
    "get_combine_module_names",
    "get_raw_data_file_names",
    "load_script",
]
