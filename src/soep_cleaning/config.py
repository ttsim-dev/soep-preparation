"""All the general configuration of the project."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True
pd.options.future.no_silent_downcasting = True
pd.options.plotting.backend = "plotly"

SRC = Path(__file__).parent.resolve()
BLD = SRC.joinpath("..", "..", "bld").resolve()

TEST_DIR = SRC.joinpath("..", "..", "tests").resolve()

SOEP_VERSION = "V38"

DATASETS = [
    "biobirth",
    "bioedu",
    "biol",
    "design",
    "hgen",
    "hl",
    "hpathl",
    "hwealth",
    "kidlong",
    "pbrutto",
    "pequiv",
    "pgen",
    "pkal",
    "pl",  # TODO: finalize cleaning task (extra large)
    "ppathl",
]


data_catalog = {
    name: DataCatalog(name=name)
    for name in ["raw", "cleaned", "manipulated", "final", "infos"]
}

data_catalog["infos"].add(
    "relevant_soep_columns",
    node=Path("data/relevant_soep_columns.csv"),
)

__all__ = ["BLD", "SRC", "TEST_DIR", "SOEP_VERSION", "DATASETS", "data_catalog"]
