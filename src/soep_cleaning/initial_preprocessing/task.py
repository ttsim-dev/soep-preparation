from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pytask import task

from soep_cleaning.config import SRC, data_catalog, pd
from soep_cleaning.utilities import dataset_script_name


def _fail_if_missing_dependency(depends_on: dict[str, Path]):
    for key, value in depends_on.items():
        if not value.exists():
            msg = f"The dependency {key} does not exist at {value}."
            raise FileNotFoundError(
                msg,
            )


def _fail_if_invalid_dependency(depends_on: dict[str, Path]):
    for key, value in depends_on.items():
        if not isinstance(value, Path):
            msg = f"The dependency {key} is not a Path object, got {type(value)}."
            raise TypeError(
                msg,
            )


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


for dataset in data_catalog["orig"].entries:

    @task(id=dataset)
    def task_clean_one_dataset(
        orig_data: Annotated[Path, data_catalog["orig"][dataset]],
        script_path: Annotated[
            Path,
            Path(
                SRC.joinpath(
                    "initial_preprocessing",
                    f"{dataset_script_name(dataset)}_cleaner.py",
                ).resolve(),
            ),
        ],
        dataset: str = dataset,
    ) -> Annotated[pd.DataFrame, data_catalog["cleaned"][dataset]]:
        """Cleans a dataset using a specified cleaning script.

        Parameters:
            orig_data (Path): The path to the dataset to be cleaned.
            script_path (Path): The path to the cleaning script.
            dataset (str): The name of the dataset.

        Returns:
            pd.DataFrame: A cleaned pandas DataFrame to be saved to the data catalog.

        Raises:
            FileNotFoundError: If the dataset file or cleaning script file does not exist.
            ImportError: If there is an error loading the cleaning script module.
            AttributeError: If the cleaning script module does not contain the expected function.

        """
        _error_handling_task(orig_data, script_path)
        module = SourceFileLoader(
            script_path.stem,
            str(script_path),
        ).load_module()
        """With pd.read_stata(orig_data, chunksize=100_000) as itr:

        for chunk in itr:
            getattr(module, f"{dataset}")(chunk)

        """
        return getattr(module, f"{dataset}")(pd.read_stata(orig_data))


def _error_handling_task(data, script_path):
    _fail_if_invalid_input(data, "pathlib.PosixPath")
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
