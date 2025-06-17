"""Task to read STATA data and store as pandas DataFrames."""

import inspect
import re
from pathlib import Path
from typing import Annotated

import pandas as pd
from pandas.io.stata import StataReader
from pytask import task

from soep_preparation.config import (
    DATA,
    DATA_CATALOGS,
    SOEP_VERSION,
    SRC,
)
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type
from soep_preparation.utilities.general import load_module


def _get_relevant_column_names(script_path: Path) -> list[str]:
    module = load_module(script_path)
    function_with_docstring = inspect.getsource(module.clean)
    # Remove the docstring, if existent.
    function_content = re.sub(
        r'""".*?"""|\'\'\'.*?\'\'\'',
        "",
        function_with_docstring,
        flags=re.DOTALL,
    )
    # Find all occurrences of raw["column_name"] or ['column_name'].
    pattern = r'raw_data\["([^"]+)"\]|\[\'([^\']+)\'\]'
    matches = [match[0] or match[1] for match in re.findall(pattern, function_content)]
    # Return unique matches in the order that they appear.
    return list(dict.fromkeys(matches))


def _iteratively_read_one_data_file(
    iterator: StataReader,
    relevant_columns: list[str],
) -> pd.DataFrame:
    categorical_mapping = {
        k: v for k, v in iterator.value_labels().items() if k in relevant_columns
    }
    processed_chunks = []

    for chunk in iterator:
        chunk_with_categorical_values = chunk.replace(categorical_mapping)
        processed_chunks.append(chunk_with_categorical_values)
    return pd.concat(processed_chunks)


if DATA_CATALOGS["data_files"]:
    for data_file_name, data_file_catalog in DATA_CATALOGS["data_files"].items():

        @task(id=data_file_name)
        def task_read_one_data_file(
            stata_data_file: Annotated[
                Path, DATA / f"{SOEP_VERSION}" / f"{data_file_name}.dta"
            ],
            cleaning_script: Annotated[
                Path,
                SRC / "clean_existing_variables" / f"{data_file_name}.py",
            ],
        ) -> Annotated[pd.DataFrame, data_file_catalog["raw"]]:
            """Saves the raw data file to the data catalog.

            Parameters:
                stata_data_file: The path to the original STATA data file.
                cleaning_script: The path to the respective cleaning script.

            Returns:
                    The raw data to be saved to the data data_file_catalog.

            Raises:
                TypeError: If input data or script path is not of expected type.
            """
            _error_handling_task(stata_data_file, cleaning_script)
            relevant_columns = _get_relevant_column_names(cleaning_script)
            with StataReader(
                stata_data_file,
                chunksize=100_000,
                columns=relevant_columns,
                convert_categoricals=False,
            ) as stata_iterator:
                return _iteratively_read_one_data_file(stata_iterator, relevant_columns)

else:

    @task
    def _raise_no_data_files_found() -> None:
        msg = """Please add at least one raw data file to the data directory under
        the specified SOEP version and create a cleaning script for it.
        For further instructions, please refer to the README file."""
        raise FileNotFoundError(msg)


def _error_handling_task(data, script_path):
    fail_if_input_has_invalid_type(data, ["pathlib._local.PosixPath"])
    fail_if_input_has_invalid_type(script_path, ["pathlib._local.PosixPath"])
