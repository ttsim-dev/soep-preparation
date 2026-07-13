# Creating a dataset

The pipeline does not ship one fixed merged dataset. It provides the cleaned modules
(the `MODULES` catalogue), a metadata catalogue, and the `create_final_dataset` helper,
which merges the variables you ask for from whichever modules hold them, on the shared
index variables.

You select **variables** and **survey years**; the helper finds the modules. A task that
builds a dataset (place it as `task_*.py` under `src/soep_preparation/`):

```python
from pathlib import Path
from typing import Annotated

import pandas as pd

from soep_preparation.config import MODULES
from soep_preparation.final_dataset import create_final_dataset


def task_create_soep_dataset(
    variables: Annotated[list[str], ["birth_year", "bmi"]],
    survey_years: Annotated[list[int], [1988, 1990, 1992, 1994]],
    pbrutto: Annotated[pd.DataFrame, MODULES["pbrutto"]],
    pequiv_pl: Annotated[pd.DataFrame, MODULES["pequiv_pl"]],
) -> Annotated[pd.DataFrame, Path.cwd() / "soep_dataset.pkl"]:
    """Merge the requested variables into one dataset."""
    return create_final_dataset(
        variables=variables,
        survey_years=survey_years,
        modules={"pbrutto": pbrutto, "pequiv_pl": pequiv_pl},
    )
```

You need not name every module explicitly: passing `MODULES` (all modules) lets
`create_final_dataset` select the relevant ones. See
`sandbox/task_example_final_dataset.py` for a worked example.

To discover which variables exist and where they live, see [Variables](variables.md) and
the metadata catalogue (`create_metadata/variable_to_metadata_mapping.yaml`).

## Reference periods and GETTSIM

Every variable is stored under `survey_year` (the interview wave). If you need a value
aligned to the calendar year it actually refers to — e.g. income aligned to its policy
year for GETTSIM — derive `ryear` from the variable's `reference` metadata and merge on
it. See [Naming conventions → Reference period](naming_conventions.md).
