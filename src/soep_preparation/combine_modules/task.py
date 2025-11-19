"""Functions to create combined variables."""

from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import DATA_CATALOGS, MODULE_STRUCTURE, SRC
from soep_preparation.utilities.error_handling import (
    fail_if_expected_function_missing,
    fail_if_input_has_invalid_type,
)
from soep_preparation.utilities.general import (
    load_script,
)

for script_name in MODULE_STRUCTURE["combined_modules"]:
    modules_to_combine = {
        module: DATA_CATALOGS["cleaned_modules"][module]
        for module in script_name.split("_")
    }

    @task(id=f"{script_name}")
    def task_combine_modules(
        modules_to_combine: Annotated[dict[str, pd.DataFrame], modules_to_combine],
        script_path: Annotated[Path, SRC / "combine_modules" / f"{script_name}.py"],
        script_name: Annotated[str, script_name],
    ) -> Annotated[pd.DataFrame, DATA_CATALOGS["combined_modules"][script_name]]:
        """Combine variables from multiple modules into one module.

        Args:
            modules_to_combine: A dictionary where keys are
                module names and values are the corresponding dataframes to be combined.
            script_path: The path to the script that contains the combine function.
            script_name: The name of the script being executed.

        Returns:
            The combined variables from the input modules.
        """
        fail_if_input_has_invalid_type(input_=script_path, expected_dtypes=["path"])
        fail_if_expected_function_missing(script_path, "combine")
        _fail_if_too_many_or_too_few_dataframes(
            dataframes=modules_to_combine, expected_entries=len(script_name.split("_"))
        )
        fail_if_input_has_invalid_type(
            input_=modules_to_combine, expected_dtypes=["dict"]
        )
        script = load_script(script_path)
        return script.combine(**modules_to_combine)


def _fail_if_too_many_or_too_few_dataframes(
    dataframes: dict, expected_entries: int
) -> None:
    if len(dataframes.keys()) != expected_entries:
        msg = f"Expected {expected_entries} dataframes, got {len(dataframes.keys())}"
        raise ValueError(
            msg,
        )
