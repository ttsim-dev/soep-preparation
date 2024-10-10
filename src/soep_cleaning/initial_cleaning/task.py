import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pytask import task

from soep_cleaning.config import DATA_CATALOG, SRC, pd


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def _fail_if_cleaning_module_missing(script_path):
    module_name = script_path.stem
    spec = importlib.util.spec_from_file_location(module_name, str(script_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "clean"):
        msg = f"The cleaning script {script_path} does not contain the expected cleaning function."
        raise AttributeError(
            msg,
        )


for dataset in DATA_CATALOG["raw"].entries:

    @task(id=dataset)
    def task_clean_one_dataset(
        raw_data: Annotated[Path, DATA_CATALOG["raw"][dataset]],
        cleaning_script: Annotated[
            Path,
            SRC / "initial_cleaning" / f"{dataset}.py",
        ],
    ) -> Annotated[pd.DataFrame, DATA_CATALOG["cleaned"][dataset]]:
        """Cleans a dataset using a specified cleaning script.

        Parameters:
            raw_data (Path): The path to the dataset to be cleaned.
            cleaning_script (Path): The path to the cleaning script.

        Returns:
            pd.DataFrame: A cleaned pandas DataFrame to be saved to the data catalog.

        Raises:
            ImportError: If there is an error loading the cleaning script module.
            TypeError: If the input data is not a pandas DataFrame or the script path is not a pathlib.Path object.
            AttributeError: If the cleaning script module does not contain the expected function.

        """
        _error_handling_task(raw_data, cleaning_script)
        module = SourceFileLoader(
            cleaning_script.stem,
            str(cleaning_script),
        ).load_module()
        return module.clean(
            raw_data,
        )


def _error_handling_task(data, cleaning_script):
    _fail_if_invalid_input(data, "pandas.core.frame.DataFrame")
    _fail_if_invalid_input(cleaning_script, "pathlib.PosixPath")
    _fail_if_cleaning_module_missing(cleaning_script)
