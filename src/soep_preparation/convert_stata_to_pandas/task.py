"""Task to read STATA data and store as pandas DataFrames."""

from pathlib import Path
from typing import Annotated, Any

import pandas as pd
from pandas.io.stata import StataReader
from pytask import task

from soep_preparation.config import (
    DATA_CATALOGS,
    DATA_ROOT,
    SOEP_VERSION,
    SRC,
)
from soep_preparation.utilities.error_handling import fail_if_input_has_invalid_type
from soep_preparation.utilities.general import (
    get_data_file_names,
    get_relevant_column_names,
)


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


DATA_FILE_NAMES = get_data_file_names(
    directory=SRC / "clean_variables",
    data_root=DATA_ROOT,
    soep_version=SOEP_VERSION,
)

for data_file_name in DATA_FILE_NAMES:

    @task(id=data_file_name)
    def task_read_one_data_file(
        stata_data_file: Annotated[
            Path, DATA_ROOT / f"{SOEP_VERSION}" / f"{data_file_name}.dta"
        ],
        cleaning_script: Annotated[
            Path,
            SRC / "clean_variables" / f"{data_file_name}.py",
        ],
    ) -> Annotated[pd.DataFrame, DATA_CATALOGS["raw_pandas"][data_file_name]]:
        """Saves the raw data file to the data catalog.

        Parameters:
            stata_data_file: The path to the original STATA data file.
            cleaning_script: The path to the respective cleaning script.

        Returns:
                The raw data to be saved to the data data_file_catalog.

        Raises:
            TypeError: If input data or script path is not of expected type.
        """
        _error_handling_task(data=stata_data_file, script_path=cleaning_script)
        relevant_columns = get_relevant_column_names(cleaning_script)
        with StataReader(
            stata_data_file,
            chunksize=100_000,
            columns=relevant_columns,
            convert_categoricals=False,
        ) as stata_iterator:
            return _iteratively_read_one_data_file(
                iterator=stata_iterator, relevant_columns=relevant_columns
            )


if not DATA_FILE_NAMES:

    @task
    def _raise_no_data_files_found() -> None:
        msg = """Please add at least one raw data file to the data directory under
        the specified SOEP version and create a cleaning script for it.
        For further instructions, please refer to the README file."""
        raise FileNotFoundError(msg)


def _error_handling_task(data: Any, script_path: Any) -> None:
    fail_if_input_has_invalid_type(
        input_=data, expected_dtypes=["pathlib._local.PosixPath"]
    )
    fail_if_input_has_invalid_type(
        input_=script_path, expected_dtypes=["pathlib._local.PosixPath"]
    )
