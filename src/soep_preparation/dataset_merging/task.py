from functools import reduce
from typing import Annotated

from soep_preparation.config import DATA_CATALOGS, SRC, get_datasets, pd


def get_cleaned_datasets(data_catalogs) -> dict[str, pd.DataFrame]:
    datasets = {}
    for dataset in get_datasets((SRC / "initial_cleaning").resolve()):
        if (
            f"{dataset}_cleaned" in data_catalogs["single_datasets"][dataset]._entries
            and f"{dataset}_manipulated"
            not in data_catalogs["single_datasets"][dataset]._entries
        ):
            datasets[dataset] = data_catalogs["single_datasets"][dataset][
                f"{dataset}_cleaned"
            ]
        elif (
            f"{dataset}_manipulated"
            in data_catalogs["single_datasets"][dataset]._entries
        ):
            datasets[dataset] = data_catalogs["single_datasets"][dataset][
                f"{dataset}_manipulated"
            ]
        else:
            msg = f"Neither cleaned nor manipulated dataset {dataset} found in data catalog."
            raise AttributeError(msg)
    return datasets


def get_dataset_kind(dataset: pd.DataFrame) -> str:
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


def get_datasets_kind(
    datasets: dict[str, pd.DataFrame],
) -> dict[str, dict[str, pd.DataFrame]]:
    datasets_kind = {}
    for dataset in datasets:
        kind = get_dataset_kind(datasets[dataset])
        if kind not in datasets_kind:
            datasets_kind[kind] = {dataset: datasets[dataset]}
        else:
            datasets_kind[kind][dataset] = datasets[dataset]
    # Change order of datasets to make sure datasets with most reliable information on hh_id are merged first
    datasets_kind["individual-time-varying"] = reorder_dict(
        datasets_kind["individual-time-varying"], ["pequiv", "pgen", "pl"]
    )
    return datasets_kind


def get_datasets_with_origin_dummy(
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
    return out


def merge_datasets(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    dataset_individual_constant = reduce(
        lambda left, right: left.combine_first(right),
        datasets["individual-time-constant"].values(),
    )
    dataset_individual_varying = reduce(
        lambda left, right: left.combine_first(right),
        datasets["individual-time-varying"].values(),
    )
    dataset_household_constant = reduce(
        lambda left, right: left.combine_first(right),
        datasets["household-time-constant"].values(),
    )
    dataset_household_varying = reduce(
        lambda left, right: left.combine_first(right),
        datasets["household-time-varying"].values(),
    )

    # Initialize with the first dataset
    data_merged = dataset_individual_varying.reset_index()

    # Merge household-varying data
    data_merged = pd.merge(
        data_merged, dataset_household_varying, on=["hh_id", "year"], how="left"
    )

    # Merge individual-constant data
    data_merged = pd.merge(
        data_merged, dataset_individual_constant, on=["p_id"], how="left"
    )

    # Merge household-constant data
    data_merged = pd.merge(
        data_merged, dataset_household_constant, on=["hh_id"], how="left"
    )

    # Set the final index
    data_merged = data_merged.set_index(["p_id", "year"]).sort_index()

    # Merge duplicate columns originating from distinct datasets
    data_merged = merge_duplicate_columns(data_merged)

    # Fill missing values with zeros for columns with pattern "in_dataset_*"
    cols_to_fill = data_merged.filter(regex="in_dataset_.*").columns
    data_merged[cols_to_fill] = data_merged[cols_to_fill].fillna(0)
    return data_merged


DATASETS = get_cleaned_datasets(DATA_CATALOGS)


def task_merge_datasets(
    datasets: Annotated[dict, DATASETS],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["merged_dataset"]]:
    """Merge datasets."""
    datasets_kind = get_datasets_kind(datasets)
    datasets_with_origin_dummy = get_datasets_with_origin_dummy(datasets_kind)
    return merge_datasets(datasets_with_origin_dummy)
