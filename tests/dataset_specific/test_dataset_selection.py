"""Tests for dataset selection."""

from unittest.mock import MagicMock

from pytask import PickleNode

from soep_preparation.utilities.dataset_manipulator import (
    get_cleaned_and_potentially_merged_dataset,
)


def test_get_cleaned_and_potentially_merged_dataset_assert_dtype_merged():
    mock_catalog = MagicMock()
    entries = {"merged": MagicMock(), "cleaned": MagicMock()}
    mock_catalog.__getitem__.side_effect = (
        lambda key: PickleNode(path=f"{key}.pkl") if key in entries else None
    )

    result = get_cleaned_and_potentially_merged_dataset(mock_catalog)
    assert isinstance(result, PickleNode)


def test_get_cleaned_and_potentially_merged_dataset_assert_dtype_cleaned():
    mock_catalog = MagicMock()
    entries = {"cleaned": MagicMock()}
    mock_catalog.__getitem__.side_effect = (
        lambda key: PickleNode(path=f"{key}.pkl") if key in entries else None
    )

    result = get_cleaned_and_potentially_merged_dataset(mock_catalog)
    assert isinstance(result, PickleNode)
