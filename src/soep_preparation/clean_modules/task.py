"""Script to clean existing variables in SOEP data files."""

from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS, SRC
from soep_preparation.utilities.error_handling import (
    fail_if_expected_function_missing,
    fail_if_input_has_invalid_type,
)
from soep_preparation.utilities.general import load_script

for data_file_name, raw_data in DATA_CATALOGS["raw_pandas"]._entries.items():  # noqa: SLF001

    @task(id=data_file_name)
    def task_clean_one_data_file(
        raw_data: Annotated[Path, raw_data],
        script_path: Annotated[
            Path,
            SRC / "clean_modules" / f"{data_file_name}.py",
        ],
    ) -> Annotated[pd.DataFrame, DATA_CATALOGS["cleaned_modules"][data_file_name]]:
        """Cleans variables of a module using the corresponding cleaning script.

        Cleaning scripts contain function `clean` taking the raw pandas DataFrame
        as input and assigning variables with adequate data types and values to
        meaningful variable names. The cleaned DataFrame is returned.
        The result is stored in the corresponding DataCatalog for further processing.

        Parameters:
            raw_data: The path to the data file to be cleaned.
            script_path: The path to the cleaning script.

        Returns:
                The cleaned data to be saved to the DataCatalog.

        Raises:
            ImportError: If there is an error loading the cleaning script.
            TypeError: If raw_data is not a pandas.DataFrame or
            script_path is not a pathlib.Path object.
            AttributeError: If cleaning script does not
            contain expected function.
        """
        fail_if_input_has_invalid_type(
            input_=raw_data, expected_dtypes=["pandas.core.frame.DataFrame"]
        )
        fail_if_input_has_invalid_type(
            input_=script_path, expected_dtypes=["pathlib._local.PosixPath"]
        )
        fail_if_expected_function_missing(
            script_path=script_path, expected_function="clean"
        )
        script = load_script(script_path)
        return script.clean(
            raw_data=raw_data,
        )
