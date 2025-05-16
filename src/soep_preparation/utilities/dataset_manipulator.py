"""Utilities for dataset manipulation."""

from pytask import DataCatalog, PickleNode


def get_cleaned_and_potentially_merged_dataset(
    dataset_specific_datacatalog: dict[str, DataCatalog],
) -> PickleNode:
    """Get the cleaned and potentially merged with derived variables dataset.

    Args:
        dataset_specific_datacatalog (dict): The corresponding Datacatalog.

    Returns:
        pd.DataFrame: The cleaned and potentially merged dataset.
    """
    return (
        dataset_specific_datacatalog["merged"]
        if "merged" in dataset_specific_datacatalog._entries  # noqa: SLF001
        else dataset_specific_datacatalog["cleaned"]
    )
