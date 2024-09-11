import inspect
import re
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pytask import task
from soep_cleaning.config import SRC, data_catalog, pd
from soep_cleaning.utilities import list_scripts


def _dataset_mapping(dir: Path) -> pd.DataFrame:
    dataset_relevant_columns_mapping = {}
    for script in list_scripts(dir, scripts_kind="cleaner"):
        for function in _list_functions_in_script(script):
            dataset_relevant_columns_mapping[f"{function}"] = pd.Series(
                _list_columns_from_function(script, function),
            )
    return pd.DataFrame(dataset_relevant_columns_mapping)


def _list_columns_from_function(script_path: Path, function_name: str) -> list:
    """List the columns from a function.

    Args:
        script_path (Path): The path to the script.
        function_name (str): The name of the function.

    Returns:
        list: A list of the columns.

    """
    module = SourceFileLoader(
        script_path.resolve().stem,
        str(script_path.resolve()),
    ).load_module()
    function_content = inspect.getsource(getattr(module, f"{function_name}"))

    pattern = r'out\["([a-zA-Z0-9_]+)"\]\s*='
    return re.findall(pattern, function_content)


def _list_functions_in_script(script_path: Path) -> list:
    """List the functions in a script.

    Args:
        script_path (Path): The path to the script.

    Returns:
        list: A list of the functions.

    """
    module = SourceFileLoader(
        script_path.resolve().stem,
        str(script_path.resolve()),
    ).load_module()
    return [
        function
        for function in dir(module)
        if ("_" not in function) and ("pd" not in function)
    ]


@task
def task_dataset_mapping(
    specific_cleaner_dir: Annotated[Path, SRC.joinpath("initial_cleaning")],
) -> Annotated[pd.DataFrame, data_catalog["infos"]["dataset_mapping"]]:
    """Create a mapping of SOEP datasets to the relevant columns.

    Parameters:
        specific_cleaner_dir (Path): The directory containing the specific cleaning scripts.

    Returns:
        pd.DataFrame: A DataFrame containing the mapping of datasets to relevant columns.

    """
    return _dataset_mapping(specific_cleaner_dir)


if __name__ == "__main__":
    task_dataset_mapping(SRC.joinpath("initial_cleaning").resolve())
