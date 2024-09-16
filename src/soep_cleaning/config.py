"""All the general configuration of the project."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

pd.set_option("mode.copy_on_write", True)
pd.set_option("future.infer_string", True)
pd.set_option("future.no_silent_downcasting", True)
pd.set_option("plotting.backend", "plotly")

SRC = Path(__file__).parent.resolve()
BLD = SRC.joinpath("..", "..", "bld").resolve()

TEST_DIR = SRC.joinpath("..", "..", "tests").resolve()

SOEP_VERSION = "V38"


DATA_CATALOG = {
    name: DataCatalog(name=name) for name in ["raw", "cleaned", "manipulated", "final"]
}

__all__ = ["BLD", "SRC", "TEST_DIR", "SOEP_VERSION", "DATA_CATALOG"]
