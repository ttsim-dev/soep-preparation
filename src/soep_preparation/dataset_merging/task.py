"""Functions to merge pre-processed SOEP datasets."""

from functools import reduce
from typing import Annotated

import pandas as pd
from pandas.api.types import union_categoricals

from soep_preparation.config import DATA_CATALOGS


def _datasets_to_merge(data_catalogs) -> dict[str, pd.DataFrame]:
    datasets = {}
    for name, catalog in data_catalogs["single_variables"].items():
        datasets[name] = (
            catalog["manipulated"]
            if "manipulated" in catalog._entries
            else catalog["cleaned"]
        )
    return datasets


def _dataset_level(dataset: pd.DataFrame) -> str:
    if "p_id" in dataset.columns:
        if "survey_year" in dataset.columns:
            return "individual-time-varying"
        return "individual-time-constant"
    if "hh_id" in dataset.columns or "hh_id_orig" in dataset.columns:
        if "survey_year" in dataset.columns:
            return "household-time-varying"
        return "household-time-constant"
    msg = "Dataset does not contain a valid level."
    raise ValueError(msg)


def _reorder_dict(original_dict, priority_keys):
    # Create a dictionary where *priority_keys* come first
    ordered_dict = {
        key: original_dict[key] for key in priority_keys if key in original_dict
    }

    # Add the remaining keys that were not in the priority list
    ordered_dict.update(
        {key: original_dict[key] for key in original_dict if key not in ordered_dict}
    )

    return ordered_dict


def structure_datasets_by_level(
    datasets: dict[str, pd.DataFrame],
) -> dict[str, dict[str, pd.DataFrame]]:
    """Create datastructure that sorts datasets by variation level.

    Args:
        datasets (dict): Dictionary containing the datasets to be merged.

    Returns:
        dict: Dictionary containing the datasets sorted by variation level.
    """
    datasets_by_level = {}
    for name, dataset in datasets.items():
        level = _dataset_level(dataset)
        if level not in datasets_by_level:
            datasets_by_level[level] = {name: dataset}
        else:
            datasets_by_level[level][name] = dataset
    # Change order of datasets to make sure datasets with most reliable information on
    # hh_id are merged first
    datasets_by_level["individual-time-varying"] = _reorder_dict(
        datasets_by_level["individual-time-varying"], ["pequiv", "pgen", "pl"]
    )
    return datasets_by_level


def _add_origin_dummy(
    datasets: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    for dataset_group in datasets:
        for dataset in datasets[dataset_group]:
            datasets[dataset_group][dataset][f"from_{dataset}"] = pd.Series(
                [1] * len(datasets[dataset_group][dataset]), dtype="uint8[pyarrow]"
            )
    return datasets


def combine_similar_information_from_different_columns(
    data_merged: pd.DataFrame,
) -> pd.DataFrame:
    """Combining variables from different datasets with similar information.

    Args:
        data_merged (pd.DataFrame): Merged dataset.

    Returns:
        pd.DataFrame: Merged dataset with combined variables.
    """
    out = data_merged.copy()
    out["birth_month"] = out["birth_month_from_bioedu"].fillna(
        out["birth_month_from_ppathl"]
    )

    categories = union_categoricals(
        [out["hh_soep_sample_from_design"], out["hh_soep_sample_from_hpathl"]]
    ).categories
    sr = (
        out["hh_soep_sample_from_design"]
        .astype("str[pyarrow]")
        .fillna(out["hh_soep_sample_from_hpathl"].astype("str[pyarrow]"))
    )
    categorical = pd.Categorical(sr, categories=categories, ordered=False)
    out["hh_soep_sample"] = pd.Series(categorical, index=out.index)
    return out.loc[
        :,
        ~out.columns.isin(["hh_soep_sample_from_design", "hh_soep_sample_from_hpathl"]),
    ]


def merge_and_fillna_shared_cols(
    df_left: pd.DataFrame, df_right: pd.DataFrame, merge_on=None, how="outer"
):
    """Merge two DataFrames on specified columns and fill missing values.

    Args:
        df_left (pd.DataFrame): Left DataFrame (values in this DataFrame have priority).
        df_right (pd.DataFrame): Right DataFrame.
        merge_on (list): Columns to merge on. If None, defaults to all shared columns.
        how (string): Merge method (e.g., 'outer', 'inner').

    Returns:
        pd.DataFrame: Merged DataFrame with filled values, without _x and _y columns.
    """
    # Default to all shared columns if merge_on is None
    if merge_on is None:
        merge_on = [c for c in df_left.columns if c in df_right.columns]

    if not merge_on:
        msg = "No shared columns specified or found to merge on."
        raise ValueError(msg)

    # Identify shared columns not in merge_on
    extra_shared_cols = [
        c for c in df_left.columns if c in df_right.columns and c not in merge_on
    ]

    # Perform the merge on the specified columns
    res = df_left.merge(df_right, on=merge_on, how=how, suffixes=("_x", "_y"))

    # Fill missing values for merged columns in merge_on
    for c in merge_on:
        if c in res.columns and f"{c}_y" in res.columns:
            res[c] = res[c].fillna(res[f"{c}_y"])
            res = res.filter(regex=r"^(?!.*(?:_y)$)")

    # Fill NaN values in '_x' columns with '_y' counterparts and drop the old columns
    for c in extra_shared_cols:
        res[c] = res[c + "_x"].fillna(res[c + "_y"])
        res = res.filter(regex=r"^(?!.*(?:_x|_y)$)")

    return res


def merge_datasets(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Merging datasets based on the level of the dataset.

    Args:
        datasets (dict): Dictionary in level structure with datasets to be merged.

    Returns:
        pd.DataFrame: Merged dataset.
    """
    dataset_individual_constant = reduce(
        lambda left, right: merge_and_fillna_shared_cols(left, right),
        datasets["individual-time-constant"].values(),
    )
    dataset_individual_varying = reduce(
        lambda left, right: merge_and_fillna_shared_cols(left, right),
        datasets["individual-time-varying"].values(),
    )
    dataset_individual_varying["hh_id_orig"] = dataset_individual_varying.groupby(
        "p_id"
    )["hh_id_orig"].ffill()
    dataset_household_varying = reduce(
        lambda left, right: merge_and_fillna_shared_cols(left, right),
        datasets["household-time-varying"].values(),
    )
    dataset_household_varying["hh_id_orig"] = dataset_household_varying.groupby(
        "hh_id"
    )["hh_id_orig"].ffill()
    dataset_household_constant = reduce(
        lambda left, right: merge_and_fillna_shared_cols(left, right),
        datasets["household-time-constant"].values(),
    )
    # Initialize with the first dataset
    data_merged = dataset_individual_varying.reset_index(drop=True)

    # Merge household-varying data
    data_merged = merge_and_fillna_shared_cols(
        data_merged,
        dataset_household_varying,
        merge_on=["hh_id", "survey_year"],
        how="left",
    )

    # Merge individual-constant data
    data_merged = merge_and_fillna_shared_cols(
        data_merged,
        dataset_individual_constant,
        merge_on=["hh_id_orig", "p_id"],
        how="left",
    )
    # Merge household-constant data
    data_merged = merge_and_fillna_shared_cols(
        data_merged,
        dataset_household_constant,
        merge_on=["hh_id"],
        how="left",
    )

    # Set the final index
    data_merged = data_merged.set_index(["p_id", "survey_year"]).sort_index()

    # Merge duplicate columns originating from distinct datasets
    return combine_similar_information_from_different_columns(data_merged)


DATASETS = _datasets_to_merge(DATA_CATALOGS)


def task_merge_datasets(
    datasets: Annotated[dict, DATASETS],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["merged_dataset"]]:
    """Merge initially on observational and time-varying level, then aggregate datasets.

    Args:
        datasets (dict): Dictionary containing the datasets to be merged.

    Returns:
        pd.DataFrame: Merged panel dataset of cleaned SOEP datasets.
    """
    datasets_by_level = structure_datasets_by_level(datasets)
    datasets_with_origin_dummy = _add_origin_dummy(datasets_by_level)
    return merge_datasets(datasets_with_origin_dummy)
