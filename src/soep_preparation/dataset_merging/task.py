from typing import Annotated

from soep_preparation.config import DATA_CATALOGS, SRC, get_datasets, pd


def get_cleaned_datasets(data_catalogs) -> dict[str, pd.DataFrame]:
    datasets = {}
    for dataset in get_datasets((SRC / "initial_cleaning").resolve()):
        if (
            dataset in data_catalogs["cleaned"]._entries
            and dataset not in data_catalogs["manipulated"]._entries
        ):
            datasets[dataset] = data_catalogs["cleaned"][dataset]
        elif dataset in data_catalogs["manipulated"]._entries:
            datasets[dataset] = data_catalogs["manipulated"][dataset]
        else:
            msg = f"Dataset {dataset} not found in cleaned or manipulated data catalog."
            raise AttributeError(msg)
    return datasets


def get_dataset_kind(dataset: pd.DataFrame) -> str:
    if "p_id" in dataset.columns:
        if "year" in dataset.columns:
            return "individual-time-varying"
        return "individual-time-constant"
    if "soep_hh_id" in dataset.columns:
        if "year" in dataset.columns:
            return "household-time-varying"
        return "household-time-constant"


DATASETS = get_cleaned_datasets(DATA_CATALOGS)


def task_merge_datasets(
    datasets: Annotated[dict, DATASETS],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["merged_dataset"]]:
    """Merge datasets."""
    return pd.DataFrame([])
