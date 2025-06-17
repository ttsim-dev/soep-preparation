"""Configuration of the soep preparation."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

from soep_preparation.utilities.general import (
    get_stems_if_corresponding_raw_data_file_exists,
)

pd.set_option("mode.copy_on_write", True)  # noqa: FBT003
pd.set_option("future.infer_string", True)  # noqa: FBT003
pd.set_option("future.no_silent_downcasting", True)  # noqa: FBT003
pd.set_option("plotting.backend", "plotly")

SRC = Path(__file__).parent.resolve()
ROOT = SRC.parent.parent.resolve()
DATA = ROOT.joinpath("data").resolve()
TEST_DIR = ROOT.joinpath("tests").resolve()

SOEP_VERSION = "V38"

if SOEP_VERSION == "V38":
    SURVEY_YEARS = [*range(1984, 2021 + 1)]
else:
    SURVEY_YEARS = [*range(1984, 2020 + 1)]


DATA_FILE_NAMES = get_stems_if_corresponding_raw_data_file_exists(
    directory=SRC / "clean_existing_variables"
)

DATA_CATALOGS = {
    "data_files": {
        data_file_name: DataCatalog(name=data_file_name)
        for data_file_name in DATA_FILE_NAMES
    },
    "derived_variables": DataCatalog(name="derived_variables"),
    "metadata": DataCatalog(name="metadata"),
    "merged": DataCatalog(name="merged"),
}

__all__ = [
    "DATA",
    "DATA_CATALOGS",
    "DATA_FILE_NAMES",
    "ROOT",
    "SOEP_VERSION",
    "SRC",
    "SURVEY_YEARS",
    "TEST_DIR",
]
