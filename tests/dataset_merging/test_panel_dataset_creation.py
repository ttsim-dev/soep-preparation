import numpy as np
import pandas as pd

from soep_preparation.dataset_merging.helper import (
    _fix_user_input,
    _get_dataset_to_columns_mapping,
    _merge_datasets,
)


def test_fix_user_input_assert_type():
    tuple_ = ([*range(2010, 2022)], ["column1", "column2"])
    expected = type(tuple_)
    user_input_ = {
        "survey_years": [*range(2010, 2022)],
        "min_and_max_survey_years": None,
        "columns": ["column1", "column2"],
    }
    actual = type(_fix_user_input(**user_input_))
    assert actual == expected


def test_fix_user_input_assert_survey_years_type():
    expected = list
    user_input_ = {
        "survey_years": [*range(2010, 2022)],
        "min_and_max_survey_years": None,
        "columns": ["column1", "column2"],
    }
    actual = _fix_user_input(**user_input_)
    assert isinstance(actual[0], expected)


def test_fix_user_input_assert_columns_type():
    expected = list
    user_input_ = {
        "survey_years": [*range(2010, 2022)],
        "min_and_max_survey_years": None,
        "columns": ["column1", "column2"],
    }
    actual = _fix_user_input(**user_input_)
    assert isinstance(actual[1], expected)


def test_fix_user_input_assert_survey_years():
    expected = ([*range(2010, 2022)], ["column1", "column2"])
    user_input_ = {
        "survey_years": [*range(2010, 2022)],
        "min_and_max_survey_years": None,
        "columns": ["column1", "column2"],
    }
    actual = _fix_user_input(**user_input_)
    assert actual[0] == expected[0]


def test_fix_user_input_assert_min_and_max_survey_years():
    expected = ([*range(2010, 2022)], ["column1", "column2"])
    user_input_ = {
        "survey_years": None,
        "min_and_max_survey_years": (2010, 2021),
        "columns": ["column1", "column2"],
    }
    actual = _fix_user_input(**user_input_)
    assert actual[0] == expected[0]


def test_fix_user_input_assert_columns():
    expected = ([*range(2010, 2022)], ["column1", "column2"])
    user_input_ = {
        "survey_years": [*range(2010, 2022)],
        "min_and_max_survey_years": None,
        "columns": ["column1", "column2"],
    }
    actual = _fix_user_input(**user_input_)
    assert actual[1] == expected[1]


def test_fix_user_input_assert_id_columns_dropped():
    expected = ([*range(2010, 2022)], ["column1", "column2"])
    user_input_ = {
        "survey_years": [*range(2010, 2022)],
        "min_and_max_survey_years": None,
        "columns": ["column1", "column2", "hh_id", "hh_id_orig", "p_id", "survey_year"],
    }
    actual = _fix_user_input(**user_input_)
    assert actual[1] == expected[1]


def test_get_dataset_to_columns_mapping_assert_type():
    expected = type(
        {
            "dataset1": ["column1", "column3"],
            "dataset2": ["column2"],
        },
    )
    input_ = {
        "columns_to_dataset_mapping": {
            "column1": "dataset1",
            "column2": "dataset2",
            "column3": "dataset1",
        },
        "columns": ["column1", "column2"],
    }
    actual = type(_get_dataset_to_columns_mapping(**input_))
    assert actual == expected


def test_get_dataset_to_columns_mapping_assert_mapping():
    expected = {
        "dataset1": ["column1", "column3"],
        "dataset2": ["column2"],
    }
    input_ = {
        "columns_to_dataset_mapping": {
            "column1": "dataset1",
            "column2": "dataset2",
            "column3": "dataset1",
        },
        "columns": ["column1", "column2", "column3"],
    }
    actual = _get_dataset_to_columns_mapping(**input_)
    assert actual == expected


def test_get_dataset_to_columns_mapping_assert_datasets():
    expected = ["dataset1", "dataset2"]
    input_ = {
        "columns_to_dataset_mapping": {
            "column1": "dataset1",
            "column2": "dataset2",
            "column3": "dataset1",
        },
        "columns": ["column1", "column2"],
    }
    actual = _get_dataset_to_columns_mapping(**input_)
    assert sorted(actual.keys()) == sorted(expected)


def test_merge_datasets_assert_type():
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
        "merging_behavior": "outer",
    }
    actual = type(_merge_datasets(**input_))
    assert actual == expected


def test_merge_datasets_assert_data():
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
        "merging_behavior": "outer",
    }
    actual = _merge_datasets(**input_)
    pd.testing.assert_frame_equal(actual, expected)
