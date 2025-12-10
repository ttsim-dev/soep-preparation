"""Functions to create combined variables."""

from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import task

from soep_preparation.config import MODULES, SRC, get_combine_module_names
from soep_preparation.utilities.general import load_script

for script_name in get_combine_module_names():
    modules_to_combine = {module: MODULES[module] for module in script_name.split("_")}

    @task(id=script_name)
    def task_combine_modules(
        modules_to_combine: Annotated[dict[str, pd.DataFrame], modules_to_combine],
        script_path: Annotated[Path, SRC / "combine_modules" / f"{script_name}.py"],
    ) -> Annotated[pd.DataFrame, MODULES[script_name]]:
        """Combine variables from multiple modules into one module.

        Args:
            modules_to_combine: A dictionary where keys are
                module names and values are the corresponding dataframes to be combined.
            script_path: The path to the script that contains the combine function.

        Returns:
            The combined variables from the input modules.
        """
        script = load_script(script_path, expected_function="combine")
        return script.combine(**modules_to_combine)
