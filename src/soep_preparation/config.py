"""All the general configuration of the project."""

from pathlib import Path

import pandas as pd
from pytask import DataCatalog

pd.set_option("mode.copy_on_write", True)  # noqa: FBT003
pd.set_option("future.infer_string", True)  # noqa: FBT003
pd.set_option("future.no_silent_downcasting", True)  # noqa: FBT003
pd.set_option("plotting.backend", "plotly")

SRC = Path(__file__).parent.resolve()
DATA = SRC.joinpath("..", "..", "data").resolve()

TEST_DIR = SRC.joinpath("..", "..", "tests").resolve()

SOEP_VERSION = "V38"


def get_dataset_names(directory: Path) -> list[str]:
    """Get all dataset names based on the script names in the given directory.

    Args:
        directory (Path): The directory to search for dataset names.

    Returns:
        list[str]: A list of dataset names.

    """
    return [
        file.stem
        for file in directory.glob("*.py")
        if file.name not in ["__init__.py", "task.py"]
        and (DATA / f"{SOEP_VERSION}" / f"{file.stem}.dta").exists()
    ]


DATA_CATALOGS = {
    "single_datasets": {
        dataset_name: DataCatalog(name=dataset_name)
        for dataset_name in get_dataset_names(SRC / "clean_existing_variables")
    },
    "merged": DataCatalog(name="merged"),
}

__all__ = [
    "DATA",
    "SRC",
    "TEST_DIR",
    "SOEP_VERSION",
    "DATA_CATALOGS",
    "get_dataset_names",
]
