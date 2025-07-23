"""Configuration of the soep preparation."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

pd.set_option("mode.copy_on_write", True)  # noqa: FBT003
pd.set_option("future.infer_string", True)  # noqa: FBT003
pd.set_option("future.no_silent_downcasting", True)  # noqa: FBT003
pd.set_option("plotting.backend", "plotly")

SRC = Path(__file__).parent.resolve()
ROOT = SRC.parent.parent.resolve()
DATA_ROOT = ROOT.joinpath("data").resolve()
TEST_DIR = ROOT.joinpath("tests").resolve()

SOEP_VERSION = "V38"

if SOEP_VERSION == "V38":
    SURVEY_YEARS = [*range(1984, 2021 + 1)]
else:
    SURVEY_YEARS = [*range(1984, 2020 + 1)]

DATA_CATALOGS = {
    "raw_pandas": DataCatalog(name="raw_pandas"),
    "cleaned_variables": DataCatalog(name="cleaned_variables"),
    "combined_variables": DataCatalog(name="combined_variables"),
    "metadata": DataCatalog(name="metadata"),
    "merged": DataCatalog(name="merged"),
}

__all__ = [
    "DATA_CATALOGS",
    "DATA_ROOT",
    "ROOT",
    "SOEP_VERSION",
    "SRC",
    "SURVEY_YEARS",
    "TEST_DIR",
]
