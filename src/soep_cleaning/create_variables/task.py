from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from soep_cleaning.config import SRC, data_catalog, pd
from soep_cleaning.utilities import dataset_script_name


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


for dataset in data_catalog["orig"].entries:
    # @task(id=dataset)
    # def task_manipulate_one_dataset(
    def function(
        clean_data: Annotated[Path, data_catalog["cleaned"][dataset]],
        script_path: Annotated[
            Path,
            Path(
                SRC.joinpath(
                    "create_variables",
                    f"{dataset_script_name(dataset)}.py",
                ).resolve(),
            ),
        ],
        dataset: str = dataset,
    ) -> Annotated[pd.DataFrame, data_catalog["manipulated"][dataset]]:  #
        """Manipulates a dataset using a specified cleaning script.

        Parameters:
            clean_data (Path): The path to the cleaned dataset to be manipulated.
            script_path (Path): The path to the manipulation script.
            dataset (str): The name of the dataset.

        Returns:
            pd.DataFrame: A manipulated pandas DataFrame to be saved to the data catalog.

        Raises:
            FileNotFoundError: If the dataset file or cleaning script file does not exist.
            ImportError: If there is an error loading the manipulation script module.
            AttributeError: If the manipulation script module does not contain the expected function.

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


def _error_handling_task(orig_data, script_path):
    _fail_if_invalid_input(orig_data, "pathlib.PosixPath")
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
