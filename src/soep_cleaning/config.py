"""All the general configuration of the project."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True
pd.options.plotting.backend = "plotly"

SRC = Path(__file__).parent.resolve()
BLD = SRC.joinpath("..", "..", "bld").resolve()

TEST_DIR = SRC.joinpath("..", "..", "tests").resolve()

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
    # "pl",
    "ppathl",
]

# TODO: data catalog in dict hierarchy with multiple data catalogs
data_catalog = DataCatalog()
for dataset in DATASETS:
    data_catalog.add(dataset, Path(f"data/V37/{dataset}.dta"))

__all__ = ["BLD", "SRC", "TEST_DIR", "DATASETS", "data_catalog"]
