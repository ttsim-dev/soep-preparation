"""Configuration of the soep preparation."""

SOEP_VERSION = "V38"
SURVEY_YEARS = [*range(1984, 2021 + 1)]


import functools
from pathlib import Path

import pandas as pd
from pytask import DataCatalog

from soep_preparation.utilities.general import get_combine_module_names as gcmn
from soep_preparation.utilities.general import get_raw_data_file_names as grdfn
from soep_preparation.utilities.general import load_script

pd.set_option("mode.copy_on_write", True)  # noqa: FBT003
pd.set_option("future.infer_string", True)  # noqa: FBT003
pd.set_option("future.no_silent_downcasting", True)  # noqa: FBT003
pd.set_option("plotting.backend", "plotly")

SRC = Path(__file__).parent.resolve()
ROOT = SRC.parent.parent.resolve()
BLD = ROOT.joinpath("bld").resolve()
DATA_ROOT = ROOT.joinpath("data").resolve()
TEST_DIR = ROOT.joinpath("tests").resolve()

get_raw_data_file_names = functools.partial(
    grdfn,
    directory=SRC / "clean_modules",
    data_root=DATA_ROOT,
    soep_version=SOEP_VERSION,
)
get_combine_module_names = functools.partial(gcmn, directory=SRC / "combine_modules")

RAW_DATA_FILES = DataCatalog(name="raw_pandas")
MODULES = DataCatalog(name="modules")

__all__ = [
    "BLD",
    "DATA_ROOT",
    "MODULES",
    "RAW_DATA_FILES",
    "ROOT",
    "SOEP_VERSION",
    "SRC",
    "SURVEY_YEARS",
    "get_combine_module_names",
    "get_raw_data_file_names",
    "load_script",
]
