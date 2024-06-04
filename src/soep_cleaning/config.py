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

data_catalog = {name: DataCatalog(name=name) for name in ["orig", "cleaned", "final"]}
for dataset in [
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
    "pl",
    "ppathl",
]:
    data_catalog["orig"].add(name=dataset, node=Path(f"data/V37/{dataset}.dta"))

__all__ = ["BLD", "SRC", "TEST_DIR", "data_catalog"]
