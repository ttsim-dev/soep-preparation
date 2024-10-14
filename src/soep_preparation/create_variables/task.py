from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pytask import task

from soep_preparation.config import DATA_CATALOGS, SRC, get_datasets, pd


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


for dataset in get_datasets((SRC / "create_variables").resolve()):
    if dataset in DATA_CATALOGS["cleaned"]._entries:

        @task(id=dataset)
        def task_manipulate_one_dataset(
            clean_data: Annotated[Path, DATA_CATALOGS["cleaned"][dataset]],
            script_path: Annotated[
                Path,
                SRC / "create_variables" / f"{dataset}.py",
            ],
        ) -> Annotated[pd.DataFrame, DATA_CATALOGS["manipulated"][dataset]]:
            """Manipulates a dataset using a specified cleaning script.

            Parameters:
                clean_data (pd.DataFrame): Cleaned dataset to be manipulated.
                script_path (Path): The path to the manipulation script.
                dataset (str): The name of the dataset.

            Returns:
                pd.DataFrame: A manipulated pandas DataFrame to be saved to the data catalog.

            Raises:
                FileNotFoundError: If the dataset file or cleaning script file does not exist.
                ImportError: If there is an error loading the manipulation script module.
                AttributeError: If the manipulation script module does not contain the expected function.

            """
            _error_handling_task(clean_data, script_path)
            module = SourceFileLoader(
                script_path.stem,
                str(script_path),
            ).load_module()
            return module.manipulate(clean_data)

    else:
        msg = f"Dataset {dataset} not found in cleaned data catalog."
        raise AttributeError(msg)


def _error_handling_task(data, script_path):
    _fail_if_invalid_input(data, "pandas.core.frame.DataFrame'")
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
