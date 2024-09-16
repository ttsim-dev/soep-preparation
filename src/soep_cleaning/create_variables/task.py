from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pytask import task
from soep_cleaning.config import DATA_CATALOG, SRC, pd
from soep_cleaning.utilities import dataset_scripts


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


for dataset in DATA_CATALOG["cleaned"].entries:
    if dataset in dataset_scripts(
        Path(
            SRC.joinpath(
                "create_variables",
            ).resolve(),
        ),
    ):

        @task(id=dataset)
        def task_manipulate_one_dataset(
            clean_data: Annotated[Path, DATA_CATALOG["cleaned"][dataset]],
            script_path: Annotated[
                Path,
                Path(
                    SRC.joinpath(
                        "create_variables",
                        f"{dataset}.py",
                    ).resolve(),
                ),
            ],
            dataset: str = dataset,
        ) -> Annotated[pd.DataFrame, DATA_CATALOG["manipulated"][dataset]]:  #
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
        DATA_CATALOG["manipulated"].add(
            name=dataset,
            node=DATA_CATALOG["cleaned"][dataset],
        )


def _error_handling_task(data, script_path):
    _fail_if_invalid_input(data, "pandas.core.frame.DataFrame'")
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
