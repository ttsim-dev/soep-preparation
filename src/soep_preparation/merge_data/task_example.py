"""Example task to merge variables to dataset."""

from pathlib import Path
from typing import Annotated

import pandas as pd
import yaml
from pytask import Product

from soep_preparation.config import DATA_CATALOGS, ROOT, SRC
from soep_preparation.merge_data.helper import create_dataset

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


def task_merge_data(
    combined_modules: Annotated[
        dict[str, pd.DataFrame], DATA_CATALOGS["combined_modules"]._entries
    ],
    cleaned_modules: Annotated[
        dict[str, pd.DataFrame], DATA_CATALOGS["cleaned_modules"]._entries
    ],
    metadata_input_path: Annotated[
        Path, SRC / "create_metadata" / "variable_to_metadata_mapping.yaml"
    ],
    variables: Annotated[list[str], VARIABLES_TO_MERGE],
    survey_years: Annotated[list[int], SURVEY_YEARS_TO_MERGE],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]]:
    """Example task merging based on variable names to create dataset.

    Args:
        combined_modules: The combined modules created in the pipeline.
        cleaned_modules: The cleaned modules created in the pipeline.
        metadata_input_path: Path to the variable to module mapping.
        variables: Variable names the dataset should contain.
        survey_years: Survey years the dataset should contain.

    Returns:
        The variables merged into a dataset.

    Raises:
        TypeError: If input mapping or variables is not of expected type.
    """
    with Path.open(metadata_input_path, "r", encoding="utf-8") as file:
        variable_to_metadata = yaml.safe_load(file)
    return create_dataset(
        modules={**combined_modules, **cleaned_modules},
        variable_to_metadata=variable_to_metadata,
        variables=variables,
        min_survey_year=min(survey_years),
        max_survey_year=max(survey_years),
    )


def task_save_dataset_to_root(
    dataset: Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]],
    out_path: Annotated[Path, Product] = ROOT / "example_merged_dataset.pkl",
) -> None:
    """Save the merged dataset to root folder.

    Args:
        dataset: The merged dataset.
        out_path: The output path to save the dataset to.

    Returns:
        None
    """
    # TODO (@felixschmitz): Allow other formats than pickle.  # noqa: TD003
    dataset.to_pickle(out_path)
