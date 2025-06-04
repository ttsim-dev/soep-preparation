"""Task to read STATA datasets and store them as pandas DataFrames."""

import inspect
import re
from importlib.machinery import SourceFileLoader
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
from soep_preparation.utilities.general import load_module
from soep_preparation.utilities.error_handling import fail_if_invalid_input


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


def _iteratively_read_one_dataset(
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

if len(DATA_CATALOGS["data_files"]) == 0:
    msg = """Please add at least one raw dataset to the data directory under
    the specified SOEP version and create a cleaning script for it.
    For further instructions, please refer to the README file."""
    raise FileNotFoundError(msg)

for name, catalog in DATA_CATALOGS["data_files"].items():
    @task(id=name)
    def task_read_one_dataset(
        stata_data: Annotated[Path, DATA / f"{SOEP_VERSION}" / f"{name}.dta"],
        cleaning_script: Annotated[
            Path,
            SRC / "clean_existing_variables" / f"{name}.py",
        ],
    ) -> Annotated[pd.DataFrame, catalog["raw"]]:
        """Saves the raw dataset to the data catalog.

        Parameters:
            stata_data (Path): The path to the original STATA dataset.
            cleaning_script (Path): The path to the respective cleaning script.

        Returns:
            pd.DataFrame: A pandas DataFrame to be saved to the data catalog.

        Raises:
            TypeError: If input data or script path is not of expected type.
        """
        _error_handling_task(stata_data, cleaning_script)
        relevant_columns = _get_relevant_column_names(cleaning_script)
        with StataReader(
            stata_data,
            chunksize=100_000,
            columns=relevant_columns,
            convert_categoricals=False,
        ) as stata_iterator:
            return _iteratively_read_one_dataset(stata_iterator, relevant_columns)


def _error_handling_task(data, script_path):
    fail_if_invalid_input(data, "pathlib._local.PosixPath")
    fail_if_invalid_input(script_path, "pathlib._local.PosixPath")
