from functools import reduce
from typing import Annotated

import pandas as pd
from pandas.api.types import union_categoricals

from soep_preparation.config import DATA_CATALOGS


def datasets_to_merge(data_catalogs) -> dict[str, pd.DataFrame]:
    datasets = {}
    for name, catalog in data_catalogs["single_variables"].items():
        assert (
            "cleaned" in catalog._entries or "manipulated" in catalog._entries
        ), f"Neither cleaned nor manipulated dataset {name} found in the respective data catalog."
        datasets[name] = (
            catalog["manipulated"]
            if "manipulated" in catalog._entries
            else catalog["cleaned"]
        )
    return datasets


def dataset_category(dataset: pd.DataFrame) -> str:
    if "p_id" in dataset.columns:
        if "year" in dataset.columns:
            return "individual-time-varying"
        return "individual-time-constant"
    if "hh_id" in dataset.columns or "hh_id_orig" in dataset.columns:
        if "year" in dataset.columns:
            return "household-time-varying"
        return "household-time-constant"


def reorder_dict(original_dict, priority_keys):
    # Create an ordered dictionary starting with priority keys
    ordered_dict = {
        key: original_dict[key] for key in priority_keys if key in original_dict
    }

    # Add the remaining keys that were not in the priority list
    ordered_dict.update(
        {key: original_dict[key] for key in original_dict if key not in ordered_dict}
    )

    return ordered_dict


def categorize_datasets(
    datasets: dict[str, pd.DataFrame],
) -> dict[str, dict[str, pd.DataFrame]]:
    categorized_datasets = {}
    for dataset in datasets:
        category = dataset_category(datasets[dataset])
        if category not in categorized_datasets:
            categorized_datasets[category] = {dataset: datasets[dataset]}
        else:
            categorized_datasets[category][dataset] = datasets[dataset]
    # Change order of datasets to make sure datasets with most reliable information on hh_id are merged first
    categorized_datasets["individual-time-varying"] = reorder_dict(
        categorized_datasets["individual-time-varying"], ["pequiv", "pgen", "pl"]
    )
    return categorized_datasets


def categorize_datasets_with_origin_dummy(
    datasets: dict[str, dict[str, pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    for dataset_group in datasets:
        for dataset in datasets[dataset_group]:
            datasets[dataset_group][dataset][f"from_{dataset}"] = pd.Series(
                [1] * len(datasets[dataset_group][dataset]), dtype="uint8[pyarrow]"
            )
    return datasets


def merge_duplicate_columns(data_merged: pd.DataFrame) -> pd.DataFrame:
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
    out.drop(
        columns=["hh_soep_sample_from_design", "hh_soep_sample_from_hpathl"],
        inplace=True,
    )
    return out


def merge_and_fillna_shared_cols(df_left, df_right, merge_on=None, how="outer"):
    """Merge two DataFrames on specified columns and fill missing values.

    Args:
        df_left (DataFrame): Left DataFrame (values in this DataFrame have priority).
        df_right (DataFrame): Right DataFrame.
        merge_on (list): List of columns to merge on. If None, defaults to all shared columns.
        how (string): Merge method (e.g., 'outer', 'inner').

    Returns:
        DataFrame: Merged DataFrame with filled values, without _x and _y columns.
    """
    # Default to all shared columns if merge_on is None
    if merge_on is None:
        merge_on = [c for c in df_left.columns if c in df_right.columns]

    if not merge_on:
        raise ValueError("No shared columns specified or found to merge on.")

    # Identify shared columns not in merge_on
    extra_shared_cols = [
        c for c in df_left.columns if c in df_right.columns and c not in merge_on
    ]

    # Perform the merge on the specified columns
    res = pd.merge(df_left, df_right, on=merge_on, how=how, suffixes=("_x", "_y"))

    # Fill missing values for merged columns in merge_on
    for c in merge_on:
        if c in res.columns and f"{c}_y" in res.columns:
            res[c] = res[c].fillna(res[f"{c}_y"])
            res.drop(columns=f"{c}_y", inplace=True)

    # Fill NaN values in '_x' columns with '_y' counterparts and drop the old columns
    for c in extra_shared_cols:
        res[c] = res[c + "_x"].fillna(res[c + "_y"])
        res.drop(columns=[c + "_x", c + "_y"], inplace=True)

    return res


def merge_datasets(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
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
        data_merged, dataset_household_varying, merge_on=["hh_id", "year"], how="left"
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
    data_merged = data_merged.set_index(["p_id", "year"]).sort_index()

    # Merge duplicate columns originating from distinct datasets
    data_merged = merge_duplicate_columns(data_merged)

    return data_merged


DATASETS = datasets_to_merge(DATA_CATALOGS)


def task_merge_datasets(
    datasets: Annotated[dict, DATASETS],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["merged_dataset"]]:
    """Merge datasets, initially on obsvervational level and time-varying level, then aggregate.

    Args:
        datasets (dict): Dictionary containing the datasets to be merged.

    Returns:
        pd.DataFrame: Merged dataset.
    """
    categorized_datasets = categorize_datasets(datasets)
    datasets_with_origin_dummy = categorize_datasets_with_origin_dummy(
        categorized_datasets
    )
    return merge_datasets(datasets_with_origin_dummy)
