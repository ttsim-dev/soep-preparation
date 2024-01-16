"""Tasks for managing the data."""

from pathlib import Path

import pandas as pd

from data_management_von_gaudecker.config import BLD, SRC
from data_management_von_gaudecker.data_management import clean_data
from data_management_von_gaudecker.utilities import read_yaml

clean_data_deps = {
    "scripts": Path("clean_data.py"),
    "data_info": SRC / "data_management" / "data_info.yaml",
    "data": SRC / "data" / "data.csv",
}


def task_clean_data_python(
    depends_on=clean_data_deps,
    produces=BLD / "python" / "data" / "data_clean.csv",
):
    """Clean the data (Python version)."""
    data_info = read_yaml(depends_on["data_info"])
    data = pd.read_csv(depends_on["data"])
    data = clean_data(data, data_info)
    data.to_csv(produces, index=False)
