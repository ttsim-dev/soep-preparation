"""Example task to merge variables to dataset."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import yaml
from pytask import Product

from soep_preparation.config import MODULES, ROOT, SRC
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

SURVEY_YEARS_TO_MERGE = [*range(1984, 2021 + 1)]


# @pytask.mark.skip()
def task_merge_data(
    modules: Annotated[dict[str, pd.DataFrame], MODULES._entries],
    metadata_input_path: Annotated[
        Path, SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
    ],
    variables: Annotated[list[str], VARIABLES_TO_MERGE],
    survey_years: Annotated[list[int], SURVEY_YEARS_TO_MERGE],
    out_path: Annotated[Path, Product] = ROOT / "example_merged_dataset.pkl",
) -> None:
    """Example task merging based on variable names to create dataset.

    Args:
        modules: The modules created in the pipeline.
        metadata_input_path: Path to the variable to module mapping.
        variables: Variable names the dataset should contain.
        survey_years: Survey years the dataset should contain.
        out_path: The output path to save the dataset to.

    Returns:
        None
    """
    with Path.open(metadata_input_path, "r", encoding="utf-8") as file:
        variable_to_metadata = yaml.safe_load(file)
    final_dataset = create_final_dataset(
        modules=modules,
        variable_to_metadata=variable_to_metadata,
        variables=variables,
        survey_years=survey_years,
    )
    final_dataset.to_pickle(out_path)
