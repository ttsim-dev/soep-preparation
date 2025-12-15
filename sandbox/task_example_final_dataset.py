"""Example task to merge variables to dataset."""

from pathlib import Path
from typing import Annotated

import pandas as pd
from pytask import Product, task

from soep_preparation.config import BLD, MODULES
from soep_preparation.final_dataset import create_final_dataset

VARIABLES_TO_MERGE = [
    "survey_year",
    "hh_id",
    "age",
    "birth_month",
    "bmi",
    "hh_weighting_factor_new_only",
    "relationship_to_head_of_hh",
    "hh_strat",
    "number_of_children",
    "p_id_father",
    "frailty",
]

SURVEY_YEARS_TO_MERGE = [*range(1984, 2023 + 1)]


@task(after="create_metadata")
def task_create_final_dataset_catch_all(
    modules: Annotated[dict[str, pd.DataFrame], MODULES._entries],
    variables: Annotated[list[str], VARIABLES_TO_MERGE],
    survey_years: Annotated[list[int], SURVEY_YEARS_TO_MERGE],
    out_path: Annotated[Path, Product] = BLD
    / "example_final_dataset"
    / "from_catch_all_task.pickle",
) -> None:
    """Example task merging variables to dataset.

    Args:
        modules: The modules required to create the final dataset.
        variables: Variable names the dataset should contain.
        survey_years: Survey years the dataset should contain.
        out_path: The output path to save the dataset to.

    Returns:
        None
    """
    final_dataset = create_final_dataset(
        modules=modules,
        variables=variables,
        survey_years=survey_years,
    )
    final_dataset.to_pickle(out_path)


@task
def task_create_final_dataset_explicit(
    modules: Annotated[
        dict[str, pd.DataFrame],
        {
            "pequiv": MODULES["pequiv"],
            "ppathl_bioedu": MODULES["ppathl_bioedu"],
            "pequiv_pl": MODULES["pequiv_pl"],
            "hpathl": MODULES["hpathl"],
            "pbrutto": MODULES["pbrutto"],
            "design": MODULES["design"],
            "biobirth": MODULES["biobirth"],
            "bioparen": MODULES["bioparen"],
        },
    ],
    variables: Annotated[list[str], VARIABLES_TO_MERGE],
    survey_years: Annotated[list[int], SURVEY_YEARS_TO_MERGE],
    out_path: Annotated[Path, Product] = BLD
    / "example_final_dataset"
    / "from_explicit_task.pickle",
) -> None:
    """Example task merging variables to dataset.

    Args:
        modules: The modules required to create the final dataset.
        variables: Variable names the dataset should contain.
        survey_years: Survey years the dataset should contain.
        out_path: The output path to save the dataset to.

    Returns:
        None
    """
    final_dataset = create_final_dataset(
        modules=modules,
        variables=variables,
        survey_years=survey_years,
    )
    final_dataset.to_pickle(out_path)
