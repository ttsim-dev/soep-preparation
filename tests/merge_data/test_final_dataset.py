import numpy as np
import pandas as pd

from soep_preparation.final_dataset import (
    _get_module_to_variable,
    _harmonize_variables,
    _merge_data,
)


def test_harmonize_variables_assert_type():
    expected_variables = ["column1", "column2"]
    expected = type(expected_variables)
    user_input_ = {
        "variables": ["column1", "column2"],
    }
    actual = type(_harmonize_variables(**user_input_))
    assert actual == expected


def test_harmonize_variables_assert_variables():
    expected = ["column1", "column2"]
    user_input_ = {
        "variables": ["p_id", "hh_id", "column1", "column2"],
    }
    actual = _harmonize_variables(**user_input_)
    assert actual == expected


def test_get_module_to_variable_assert_type():
    expected = type(
        {
            "dataset1": ["column1", "column3"],
            "dataset2": ["column2"],
        },
    )
    input_ = {
        "variable_to_metadata": {
            "column1": {"module": "dataset1"},
            "column2": {"module": "dataset2"},
            "column3": {"module": "dataset1"},
        },
        "variables": ["column1", "column2"],
    }
    actual = type(_get_module_to_variable(**input_))
    assert actual == expected


def test_get_module_to_variable_assert_mapping():
    expected = {
        "dataset1": ["column1", "column3"],
        "dataset2": ["column2"],
    }
    input_ = {
        "variable_to_metadata": {
            "column1": {"module": "dataset1"},
            "column2": {"module": "dataset2"},
            "column3": {"module": "dataset1"},
        },
        "variables": ["column1", "column2", "column3"],
    }
    actual = _get_module_to_variable(**input_)
    assert actual == expected


def test_get_module_to_variable_assert_modules():
    expected = ["dataset1", "dataset2"]
    input_ = {
        "variable_to_metadata": {
            "column1": {"module": "dataset1"},
            "column2": {"module": "dataset2"},
            "column3": {"module": "dataset1"},
        },
        "variables": ["column1", "column2"],
    }
    actual = _get_module_to_variable(**input_)
    assert sorted(actual.keys()) == sorted(expected)


def test_create_final_dataset_assert_type():
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
    input_ = {
        "merging_information": {
            "dataset1": {
                "data": pd.DataFrame(
                    {"id1": [0, 1], "column1": [1, 2], "column2": [3, 4]},
                ),
                "index_columns": ["id1"],
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
                "index_columns": ["id1", "id2"],
            },
        },
    }
    actual = type(_merge_data(**input_))
    assert actual == expected


def test_create_final_dataset_assert_data():
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
    input_ = {
        "merging_information": {
            "dataset1": {
                "data": pd.DataFrame(
                    {"id1": [0, 1], "column1": [1, 2], "column2": [3, 4]},
                ),
                "index_columns": ["id1"],
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
                "index_columns": ["id1", "id2"],
            },
        },
    }
    actual = _merge_data(**input_)
    pd.testing.assert_frame_equal(actual, expected)
