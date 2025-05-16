"""Helper functions for merging datasets."""

from difflib import get_close_matches

import pandas as pd

from soep_preparation.config import DATA_CATALOGS, SURVEY_YEARS
from soep_preparation.utilities import (
    fail_if_invalid_input,
    fail_if_invalid_inputs,
    get_cleaned_and_potentially_merged_dataset,
)


def _fail_if_invalid_column(
    columns: list[str],
    columns_to_dataset_mapping: dict[str, list[str]],
):
    all_columns_to_dataset_mapping = (
        columns_to_dataset_mapping["single_datasets"]
        | columns_to_dataset_mapping["multiple_datasets"]
    )
    for column in columns:
        if (
            column not in columns_to_dataset_mapping["single_datasets"]
            and column not in columns_to_dataset_mapping["multiple_datasets"]
        ):
            closest_matches = get_close_matches(
                column,
                all_columns_to_dataset_mapping.keys(),
                n=3,
                cutoff=0.6,
            )
            matches_datasets = {
                match: all_columns_to_dataset_mapping[match]
                for match in closest_matches
            }
            msg = f"""Column {column} not found in any dataset.
            The closest matches with the corresponding dataset are:
            {matches_datasets}"""
            raise ValueError(msg)


def _fail_if_empty(input_: dict | list):
    if len(input_) == 0:
        msg = f"Expected {input_} to be non-empty."
        raise ValueError(msg)


def _fail_if_survey_years_not_valid(
    survey_years: list[int] | tuple[int],
    valid_survey_years: list[int],
):
    if not all(year in valid_survey_years for year in survey_years):
        msg = f"""Expected survey years to be in {valid_survey_years},
        got {survey_years} instead."""
        raise ValueError(msg)


def _fail_if_min_larger_max(min_and_max_survey_years: tuple[int, int]):
    if min_and_max_survey_years[0] > min_and_max_survey_years[1]:
        msg = f"""Expected min survey year to be smaller than max survey year,
        got {min_and_max_survey_years} instead."""
        raise ValueError(msg)


def _fail_if_invalid_merging_behavior(merging_behavior: str):
    valid_merging_behaviors = ["left", "right", "outer", "inner", "cross"]
    if merging_behavior not in valid_merging_behaviors:
        msg = f"""Expected merging behavior to be in {valid_merging_behaviors},
        got {merging_behavior} instead."""
        raise ValueError(msg)


def _get_dataset_to_columns_mapping(
    columns_to_dataset_mapping: dict[str, list[str]],
    columns: list[str],
) -> dict[str, list[str]]:
    dataset_columns_mapping = {}
    for column in columns:
        if column in columns_to_dataset_mapping:
            dataset = columns_to_dataset_mapping[column]
            if dataset not in dataset_columns_mapping:
                dataset_columns_mapping[dataset] = []
            dataset_columns_mapping[dataset].append(column)
    return dataset_columns_mapping


def _sort_dataset_merging_information(
    dataset_merging_information: dict[str, dict],
) -> dict[str, dict]:
    return dict(
        sorted(
            dataset_merging_information.items(),
            key=lambda item: len(item[1]["index_columns"]),
            reverse=True,
        ),
    )


def _fix_user_input(
    survey_years: list[int] | None,
    min_and_max_survey_years: tuple[int, int] | None,
    columns: list[str],
) -> tuple[list[int], list[str]]:
    if survey_years is None and min_and_max_survey_years is not None:
        survey_years = [
            *range(min_and_max_survey_years[0], min_and_max_survey_years[1] + 1),
        ]
    if any([["hh_id", "hh_id_orig", "p_id", "survey_year"] in columns]):
        columns = [
            col
            for col in columns
            if col not in ["hh_id", "hh_id_orig", "p_id", "survey_year"]
        ]
    return survey_years, columns


def _get_sorted_dataset_merging_information(
    columns_to_dataset_mapping: dict[str, dict],
    columns: list,
    survey_years: list[int],
) -> dict[str, dict]:
    datasets_mapping = {
        "single_datasets": _get_dataset_to_columns_mapping(
            columns_to_dataset_mapping["single_datasets"],
            columns,
        ),
        "multiple_datasets": _get_dataset_to_columns_mapping(
            columns_to_dataset_mapping["multiple_datasets"],
            columns,
        ),
    }

    dataset_merging_information = {}
    for dataset_kind, dataset_columns_mapping in datasets_mapping.items():
        for dataset, dataset_columns in dataset_columns_mapping.items():
            raw_data = get_cleaned_and_potentially_merged_dataset(
                DATA_CATALOGS[dataset_kind][dataset],
            ).load()
            index_columns = sorted(
                DATA_CATALOGS[dataset_kind][dataset]["metadata"]
                .load()["index_columns"]
                .keys(),
            )
            if "survey_year" in raw_data.columns:
                data = raw_data.query(
                    f"{min(survey_years)} <= survey_year <= {max(survey_years)}",
                )[index_columns + dataset_columns]
            else:
                data = raw_data[index_columns + dataset_columns]
            dataset_merging_information[dataset] = {
                "data": data,
                "index_columns": index_columns,
            }
    return _sort_dataset_merging_information(dataset_merging_information)


def _merge_datasets(
    merging_information: dict[str, dict],
    merging_behavior: str = "outer",
) -> pd.DataFrame:
    datasets = [dataset["data"] for dataset in merging_information.values()]
    out = pd.DataFrame()
    for dataset in datasets:
        out = dataset if out.empty else out.merge(dataset, how=merging_behavior)
    return out.reset_index(drop=True)


def create_panel_dataset(
    columns_to_dataset_mapping: dict[str, list[str]],
    columns: list[str],
    min_and_max_survey_years: tuple[int, int] | None = None,
    survey_years: list[int] | None = None,
    merging_behavior: str = "outer",
) -> pd.DataFrame:
    """Create a panel dataset by merging datasets based on column names.

    Args:
        columns_to_dataset_mapping (dict): A mapping of column names to dataset names.
        columns (list[str]): A list of column names to be used for merging.
        min_and_max_survey_years (tuple[int, int] | None): Range of survey years.
        survey_years (list[int] | None): Survey years to be included in the dataset.
        Either `survey_years` or `min_and_max_survey_years` must be provided.
        merging_behavior (str): The merging behavior to be used.
        Any out of "left", "right", "outer", "inner", or "cross".
        Defaults to "outer".

    Returns:
        pd.DataFrame: The panel dataset with specified columns and survey years.
    """
    _error_handling(
        columns_to_dataset_mapping,
        columns,
        min_and_max_survey_years,
        survey_years,
        merging_behavior,
    )

    survey_years, columns = _fix_user_input(
        survey_years,
        min_and_max_survey_years,
        columns,
    )

    dataset_merging_information = _get_sorted_dataset_merging_information(
        columns_to_dataset_mapping,
        columns,
        survey_years,
    )

    return _merge_datasets(
        merging_information=dataset_merging_information,
        merging_behavior=merging_behavior,
    )


def _error_handling(
    columns_to_dataset_mapping: dict[str, list[str]],
    columns: list[str],
    min_and_max_survey_years: tuple[int, int] | None,
    survey_years: list[int] | None,
    merging_behavior: str,
):
    fail_if_invalid_input(columns_to_dataset_mapping, "dict")
    fail_if_invalid_input(columns, "list")
    fail_if_invalid_inputs(min_and_max_survey_years, "tuple | None")
    fail_if_invalid_inputs(survey_years, "list | None")
    fail_if_invalid_input(merging_behavior, "str")
    _fail_if_empty(columns_to_dataset_mapping["single_datasets"])
    _fail_if_empty(columns_to_dataset_mapping["multiple_datasets"])
    _fail_if_empty(columns)
    if survey_years is not None:
        _fail_if_survey_years_not_valid(survey_years, SURVEY_YEARS)
    else:
        _fail_if_survey_years_not_valid(min_and_max_survey_years, SURVEY_YEARS)
        _fail_if_min_larger_max(min_and_max_survey_years)
    _fail_if_invalid_column(columns, columns_to_dataset_mapping)
    _fail_if_invalid_merging_behavior(merging_behavior)
