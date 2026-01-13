import numpy as np
import pandas as pd
import pytest

from soep_preparation.final_dataset import (
    DatasetMergingInfo,
    _harmonize_variables,
    _merge_data,
)


@pytest.fixture
def merging_information() -> dict[str, DatasetMergingInfo]:
    return {
        "dataset1": {
            "data": pd.DataFrame(
                {"id1": [0, 1], "column1": [1, 2], "column2": [3, 4]},
            ),
            "index_variables": ["id1"],
        },
        "dataset2": {
            "data": pd.DataFrame(
                {
                    "id1": [0, 1, 2],
                    "id2": [2, 3, 4],
                    "column3": [5, 6, 7],
                    "column4": [8, 9, 10],
                },
            ),
            "index_variables": ["id1", "id2"],
        },
    }


def test_harmonize_variables_assert_variables():
    expected = ["column1", "column2"]
    user_input_ = {
        "variables": ["p_id", "hh_id", "column1", "column2"],
    }
    actual = _harmonize_variables(**user_input_)
    assert actual == expected


def test_merge_data_assert_type(
    merging_information: dict[str, DatasetMergingInfo],
):
    data = pd.DataFrame(
        {
            "id1": [0, 1, 2],
            "id2": [2, 3, 4],
            "column1": [1, 2, 3],
            "column2": [3, 4, 5],
            "column3": [5, 6, 7],
            "column4": [8, 9, 10],
        },
    )
    expected = type(data)
    actual = type(_merge_data(merging_information=merging_information))
    assert actual == expected


def test_merge_data_assert_data(
    merging_information: dict[str, DatasetMergingInfo],
):
    data = pd.DataFrame(
        {
            "id1": [0, 1, 2],
            "column1": [1, 2, np.nan],
            "column2": [3, 4, np.nan],
            "id2": [2, 3, 4],
            "column3": [5, 6, 7],
            "column4": [8, 9, 10],
        },
    )
    expected = data
    actual = _merge_data(
        merging_information=merging_information,
    )
    pd.testing.assert_frame_equal(actual, expected)
