"""Configuration of the soep preparation."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

from soep_preparation.utilities.general import (
    get_data_file_names,
)

pd.set_option("mode.copy_on_write", True)  # noqa: FBT003
pd.set_option("future.infer_string", True)  # noqa: FBT003
pd.set_option("future.no_silent_downcasting", True)  # noqa: FBT003
pd.set_option("plotting.backend", "plotly")

SRC = Path(__file__).parent.resolve()
ROOT = SRC.parent.parent.resolve()
DATA_ROOT = ROOT.joinpath("data").resolve()
DATA_ROOT = ROOT.joinpath("data").resolve()
TEST_DIR = ROOT.joinpath("tests").resolve()

ID_VARIABLES = ["hh_id", "hh_id_original", "p_id", "survey_year"]

SOEP_VERSION = "V38"

if SOEP_VERSION == "V38":
    SURVEY_YEARS = [*range(1984, 2021 + 1)]
else:
    SURVEY_YEARS = [*range(1984, 2020 + 1)]


DATA_FILE_NAMES = get_data_file_names(
    directory=SRC / "clean_variables",
    data_root=DATA_ROOT,
    soep_version=SOEP_VERSION,
)

DATA_CATALOGS = {
    "data_files": {
        data_file_name: DataCatalog(name=data_file_name)
        for data_file_name in DATA_FILE_NAMES
    },
    "combined_variables": DataCatalog(name="combined_variables"),
    "metadata": DataCatalog(name="metadata"),
    "merged": DataCatalog(name="merged"),
}

__all__ = [
    "DATA_CATALOGS",
    "DATA_FILE_NAMES",
    "DATA_ROOT",
    "ID_VARIABLES",
    "ROOT",
    "SOEP_VERSION",
    "SRC",
    "SURVEY_YEARS",
    "TEST_DIR",
]
