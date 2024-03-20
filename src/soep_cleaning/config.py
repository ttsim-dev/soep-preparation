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

MONTH_MAPPING = {
    "[1] Januar": 1,
    "[2] Februar": 2,
    "[3] Maerz": 3,
    "[4] April": 4,
    "[5] Mai": 5,
    "[6] Juni": 6,
    "[7] Juli": 7,
    "[8] August": 8,
    "[9] September": 9,
    "[10] Oktober": 10,
    "[11] November": 11,
    "[12] Dezember": 12,
}

DATASETS = [
    "biobirth",
    # "bioedu", TODO: Fix str_categorical ValueError: items in new_categories are not the same as in old categories
    "biol",
    "design",
    "hgen",
    "hl",
    "hpathl",
    "hwealth",
    "kidlong",
    "pbrutto",
    # "pequiv", TODO: Fix str_categorical ValueError: items in new_categories are not the same as in old categories
    "pgen",
    "pkal",
    # "pl",
    # "ppathl",
]

data_catalog = DataCatalog()
for dataset in DATASETS:
    data_catalog.add(dataset, Path(f"data/V37/{dataset}.dta"))

__all__ = ["BLD", "SRC", "TEST_DIR", "MONTH_MAPPING", "DATASETS", "data_catalog"]
