"""Functions to create combined variables."""

from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import MODULE_STRUCTURE, MODULES, SRC
from soep_preparation.utilities.general import load_script

for script_name in MODULE_STRUCTURE["combined_modules"]:
    modules_to_combine = {module: MODULES[module] for module in script_name.split("_")}

    @task(id=f"{script_name}")
    def task_combine_modules(
        modules_to_combine: Annotated[dict[str, pd.DataFrame], modules_to_combine],
        script_path: Annotated[Path, SRC / "combine_modules" / f"{script_name}.py"],
        script_name: Annotated[str, script_name],
    ) -> Annotated[pd.DataFrame, MODULES[script_name]]:
        """Combine variables from multiple modules into one module.

        Args:
            modules_to_combine: A dictionary where keys are
                module names and values are the corresponding dataframes to be combined.
            script_path: The path to the script that contains the combine function.
            script_name: The name of the script being executed.

        Returns:
            The combined variables from the input modules.
        """
        _fail_if_too_many_or_too_few_dataframes(
            observed_number_of_modules=len(modules_to_combine.keys()),
            expected_number_of_entries=len(script_name.split("_")),
        )
        script = load_script(script_path, expected_function="combine")
        return script.combine(**modules_to_combine)


def _fail_if_too_many_or_too_few_dataframes(
    observed_number_of_modules: int, expected_number_of_entries: int
) -> None:
    if observed_number_of_modules != expected_number_of_entries:
        msg = f"""Expected {expected_number_of_entries} dataframes,"""
        f""" got {observed_number_of_modules}."""
        raise ValueError(
            msg,
        )
