"""Script to clean existing variables in SOEP data files."""

from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import MODULE_STRUCTURE, MODULES, RAW_DATA_FILES, SRC
from soep_preparation.utilities.general import load_script

for data_file_name in MODULE_STRUCTURE["cleaned_modules"]:

    @task(id=data_file_name)
    def task_clean_one_data_file(
        raw_data: Annotated[pd.DataFrame, RAW_DATA_FILES[data_file_name]],
        script_path: Annotated[
            Path,
            SRC / "clean_modules" / f"{data_file_name}.py",
        ],
    ) -> Annotated[pd.DataFrame, MODULES[data_file_name]]:
        """Cleans variables of a module using the corresponding cleaning script.

        Cleaning scripts contain function `clean` taking the raw pandas DataFrame
        as input and assigning variables with adequate data types and values to
        meaningful variable names. The cleaned DataFrame is returned.
        The result is stored in the corresponding DataCatalog for further processing.

        Parameters:
            raw_data: The raw pandas DataFrame to be cleaned.
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
        script = load_script(script_path, expected_function="clean")
        return script.clean(raw_data=raw_data)
