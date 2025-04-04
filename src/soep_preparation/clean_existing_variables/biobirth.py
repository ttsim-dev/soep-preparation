"""Functions to pre-process variables for a raw biobirth dataset.

Functions:
- clean: Coordinates the pre-processing for the dataset.

Usage:
    Import this module and call clean to pre-process variables.
"""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities import (
    _fail_if_invalid_input,
    categorical_to_int_categorical,
    find_lowest_int_dtype,
    float_categorical_to_int,
    int_to_int_categorical,
)


def _wide_to_long(data_wide: pd.DataFrame) -> pd.DataFrame:
    _fail_if_invalid_input(data_wide, "pandas.core.frame.DataFrame")
    prev_wide_cols = ["birth_year_child", "p_id_child", "birth_month_child"]
    data_long = pd.wide_to_long(
        data_wide,
        stubnames=prev_wide_cols,
        i=["hh_id_orig", "p_id"],
        j="child_number",
        sep="_",
    ).reset_index()
    data_long_no_missings = data_long.dropna(subset=prev_wide_cols, how="all")
    return data_long_no_missings.astype(
        {
            "birth_year_child": find_lowest_int_dtype(
                data_long_no_missings["birth_year_child"],
            ),
            "p_id_child": find_lowest_int_dtype(data_long_no_missings["p_id_child"]),
            "birth_month_child": "category",
        },
    ).reset_index(drop=True)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["hh_id_orig"] = float_categorical_to_int(raw_data["cid"])
    out["p_id"] = float_categorical_to_int(raw_data["pid"])
    out["n_kids_total"] = int_to_int_categorical(
        float_categorical_to_int(raw_data["sumkids"]),
    )

    out["birth_year_child_1"] = float_categorical_to_int(
        raw_data["kidgeb01"],
    )
    out["birth_year_child_2"] = float_categorical_to_int(
        raw_data["kidgeb02"],
    )
    out["birth_year_child_3"] = float_categorical_to_int(
        raw_data["kidgeb03"],
    )
    out["birth_year_child_4"] = float_categorical_to_int(
        raw_data["kidgeb04"],
    )
    out["birth_year_child_5"] = float_categorical_to_int(
        raw_data["kidgeb05"],
    )
    out["birth_year_child_6"] = float_categorical_to_int(
        raw_data["kidgeb06"],
    )
    out["birth_year_child_7"] = float_categorical_to_int(
        raw_data["kidgeb07"],
    )
    out["birth_year_child_8"] = float_categorical_to_int(
        raw_data["kidgeb08"],
    )
    out["birth_year_child_9"] = float_categorical_to_int(
        raw_data["kidgeb09"],
    )
    out["birth_year_child_10"] = float_categorical_to_int(
        raw_data["kidgeb10"],
    )
    out["birth_year_child_11"] = float_categorical_to_int(
        raw_data["kidgeb11"],
    )
    out["birth_year_child_12"] = float_categorical_to_int(
        raw_data["kidgeb12"],
    )
    out["birth_year_child_13"] = float_categorical_to_int(
        raw_data["kidgeb13"],
    )
    out["birth_year_child_14"] = float_categorical_to_int(
        raw_data["kidgeb14"],
    )
    out["birth_year_child_15"] = float_categorical_to_int(
        raw_data["kidgeb15"],
    )
    out["birth_year_child_16"] = float_categorical_to_int(
        raw_data["kidgeb16"],
    )

    out["p_id_child_1"] = float_categorical_to_int(
        raw_data["kidpnr01"],
    )
    out["p_id_child_2"] = float_categorical_to_int(
        raw_data["kidpnr02"],
    )
    out["birth_month_child_1"] = categorical_to_int_categorical(
        raw_data["kidmon01"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_2"] = categorical_to_int_categorical(
        raw_data["kidmon02"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_3"] = categorical_to_int_categorical(
        raw_data["kidmon03"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_4"] = categorical_to_int_categorical(
        raw_data["kidmon04"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_5"] = categorical_to_int_categorical(
        raw_data["kidmon05"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_6"] = categorical_to_int_categorical(
        raw_data["kidmon06"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_7"] = categorical_to_int_categorical(
        raw_data["kidmon07"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_8"] = categorical_to_int_categorical(
        raw_data["kidmon08"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_9"] = categorical_to_int_categorical(
        raw_data["kidmon09"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_10"] = categorical_to_int_categorical(
        raw_data["kidmon10"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_11"] = categorical_to_int_categorical(
        raw_data["kidmon11"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_12"] = categorical_to_int_categorical(
        raw_data["kidmon12"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_13"] = categorical_to_int_categorical(
        raw_data["kidmon13"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_14"] = categorical_to_int_categorical(
        raw_data["kidmon14"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_15"] = categorical_to_int_categorical(
        raw_data["kidmon15"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_16"] = categorical_to_int_categorical(
        raw_data["kidmon16"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    return _wide_to_long(out)
