import inspect
import re
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pytask import task
from soep_cleaning.config import DATASETS, SOEP_VERSION, SRC, data_catalog, pd
from soep_cleaning.utilities import dataset_script_name


def _iteratively_read_one_dataset(
    itr: pd.io.stata.StataReader,
    list_of_columns: list,
) -> pd.DataFrame:
    """Read a dataset in chunks."""
    value_labels_dict = {col: {} for col in list_of_columns}
    out = pd.DataFrame()
    for chunk in itr:
        for col in list_of_columns:
            value_labels = {}
            if col in itr.value_labels():
                value_labels = itr.value_labels()[col]
            unique_values = {
                value: value
                for value in chunk[col].unique()
                if value not in value_labels.values()
            }
            value_labels_dict[col] = (
                value_labels_dict[col] | value_labels | unique_values
            )

        out = pd.concat([out, chunk], ignore_index=True)
    cat_dtypes = {
        col: pd.CategoricalDtype(
            categories=list(dict(sorted(value_dict.items())).values()),
            ordered=True,
        )
        for col, value_dict in value_labels_dict.items()
    }
    return out.astype(cat_dtypes)


def _list_functions_in_script(script_path: Path) -> list:
    module = SourceFileLoader(
        script_path.resolve().stem,
        str(script_path.resolve()),
    ).load_module()
    return [
        function
        for function in dir(module)
        if ("_" not in function) and ("pd" not in function)
    ]


def _list_columns_from_function(script_path: Path, function_name: str) -> list:
    module = SourceFileLoader(
        script_path.resolve().stem,
        str(script_path.resolve()),
    ).load_module()
    function_content = inspect.getsource(getattr(module, f"{function_name}"))

    pattern = r'raw\["([a-zA-Z0-9_]+)"\]'
    return re.findall(pattern, function_content)


def _list_of_columns_for_dataset(dataset_name: str) -> list:
    script_name = f"{dataset_script_name(dataset_name)}_cleaner.py"
    script_path = SRC.joinpath("initial_cleaning", script_name).resolve()
    for function in _list_functions_in_script(script_path):
        if function == dataset_name:
            return _list_columns_from_function(script_path, function)
    msg = f"No function found for dataset {dataset_name}."
    raise ValueError(msg)


for dataset in DATASETS:

    @task(id=dataset)
    def task_pickle_one_dataset(
        orig_data: Annotated[Path, SRC.joinpath(f"data/{SOEP_VERSION}/{dataset}.dta")],
        dataset: str = dataset,
    ) -> Annotated[pd.DataFrame, data_catalog["raw"][dataset]]:
        """Saves the raw dataset to the data catalog in a more efficient procedure.

        Parameters:
            orig_data (Path): The path to the original dataset.
            dataset (str): The name of the dataset.

        Returns:
            pd.DataFrame: A pandas DataFrame to be saved to the data catalog.

        """
        list_of_columns = _list_of_columns_for_dataset(dataset)
        with pd.read_stata(
            orig_data,
            chunksize=100_000,
            columns=list_of_columns,
            convert_categoricals=False,
        ) as itr:
            return _iteratively_read_one_dataset(itr, list_of_columns)
