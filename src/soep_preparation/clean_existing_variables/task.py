"""Module to clean existing variables in SOEP files."""

from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS, SRC
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type
from soep_preparation.utilities.general import load_module


def _fail_if_cleaning_module_missing(module_path):
    module = load_module(module_path)

    if not hasattr(module, "clean"):
        msg = f"""The cleaning module {module_path}
          does not contain expected cleaning function."""
        raise AttributeError(
            msg,
        )


for file_name, file_catalog in DATA_CATALOGS["data_files"].items():

    @task(id=file_name)
    def task_clean_one_file(
        raw_data: Annotated[Path, file_catalog["raw"]],
        module_path: Annotated[
            Path,
            SRC / "clean_existing_variables" / f"{file_name}.py",
        ],
    ) -> Annotated[pd.DataFrame, file_catalog["cleaned"]]:
        """Cleans variables of a data file using the corresponding cleaning module.

        Cleaning modules contain function `clean` taking the raw pandas DataFrame
        as input and assigning variables with adequate data types and values to
        meaningful variable names. The cleaned DataFrame is returned.
        The result is stored in the corresponding DataCatalog for further processing.

        Parameters:
            raw_data: The path to the file to be cleaned.
            module_path: The path to the cleaning module.

        Returns:
                The cleaned data to be saved to the DataCatalog.

        Raises:
            ImportError: If there is an error loading the cleaning module.
            TypeError: If raw_data is not a pandas.DataFrame or
            module_path is not a pathlib.Path object.
            AttributeError: If cleaning module module does not
            contain expected function.
        """
        _error_handling_task(raw_data, module_path)
        module = load_module(module_path)
        return module.clean(
            raw_data=raw_data,
        )


def _error_handling_task(data, module_path):
    fail_if_input_has_invalid_type(data, ["pandas.core.frame.DataFrame"])
    fail_if_input_has_invalid_type(module_path, ["pathlib._local.PosixPath"])
    _fail_if_cleaning_module_missing(module_path)
