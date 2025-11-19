"""Configuration of the soep preparation."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

from soep_preparation.utilities.general import (
    get_combine_module_names,
    get_data_file_names,
)

pd.set_option("mode.copy_on_write", True)  # noqa: FBT003
pd.set_option("future.infer_string", True)  # noqa: FBT003
pd.set_option("future.no_silent_downcasting", True)  # noqa: FBT003
pd.set_option("plotting.backend", "plotly")

SRC = Path(__file__).parent.resolve()
ROOT = SRC.parent.parent.resolve()
BLD = ROOT.joinpath("bld").resolve()
DATA_ROOT = ROOT.joinpath("data").resolve()
TEST_DIR = ROOT.joinpath("tests").resolve()

SOEP_VERSION = "V38"

if SOEP_VERSION == "V38":
    SURVEY_YEARS = [*range(1984, 2021 + 1)]
else:
    SURVEY_YEARS = [*range(1984, 2020 + 1)]


MODULE_STRUCTURE = {
    "cleaned_modules": get_data_file_names(
        directory=SRC / "clean_modules",
        data_root=DATA_ROOT,
        soep_version=SOEP_VERSION,
    ),
    "combined_modules": get_combine_module_names(directory=SRC / "combine_modules"),
}


DATA_CATALOGS = {
    "raw_pandas": DataCatalog(name="raw_pandas"),
    "cleaned_modules": DataCatalog(name="cleaned_modules"),
    "combined_modules": DataCatalog(name="combined_modules"),
    "metadata": DataCatalog(name="metadata"),
    "merged": DataCatalog(name="merged"),
}

__all__ = [
    "BLD",
    "DATA_CATALOGS",
    "DATA_ROOT",
    "MODULE_STRUCTURE",
    "ROOT",
    "SOEP_VERSION",
    "SRC",
    "SURVEY_YEARS",
    "TEST_DIR",
]
