"""Clean and convert SOEP biobirth variables to appropriate data types."""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities.error_handling import (
    fail_if_invalid_input,
)
from soep_preparation.utilities.series_manipulator import (
    find_lowest_int_dtype,
    float_to_int,
    object_to_int,
    object_to_int_categorical,
)


def _wide_to_long(data_wide: pd.DataFrame) -> pd.DataFrame:
    fail_if_invalid_input(data_wide, "pandas.core.frame.DataFrame")
    prev_wide_cols = ["birth_year_child", "p_id_child", "birth_month_child"]
    data_long = pd.wide_to_long(
        data_wide,
        stubnames=prev_wide_cols,
        i=["hh_id_original", "p_id"],
        j="child_number",
        sep="_",
    ).reset_index()
    data_long_no_missings = data_long.dropna(subset=prev_wide_cols, how="all")
    return data_long_no_missings.astype(
        {
            "child_number": find_lowest_int_dtype(
                data_long_no_missings["child_number"],
            ),
            "birth_year_child": find_lowest_int_dtype(
                data_long_no_missings["birth_year_child"],
            ),
            "p_id_child": find_lowest_int_dtype(data_long_no_missings["p_id_child"]),
            "birth_month_child": "category",
        },
    ).reset_index(drop=True)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the biobirth file.

    Args:
        raw_data: The raw biobirth data.

    Returns:
        The processed biobirth data.
    """
    out = pd.DataFrame()
    out["hh_id_original"] = float_to_int(raw_data["cid"])
    out["p_id"] = float_to_int(raw_data["pid"])

    out["number_of_children"] = float_to_int(raw_data["sumkids"])
    # the personal id of children (kidpnr) only exists for the first 9 children
    out["p_id_child_1"] = object_to_int(raw_data["kidpnr01"])
    out["birth_year_child_1"] = object_to_int(raw_data["kidgeb01"])
    out["birth_month_child_1"] = object_to_int_categorical(
        raw_data["kidmon01"], renaming=month_mapping.de, ordered=True
    )
    out["p_id_child_2"] = object_to_int(raw_data["kidpnr02"])
    out["birth_year_child_2"] = object_to_int(raw_data["kidgeb02"])
    out["birth_month_child_2"] = object_to_int_categorical(
        raw_data["kidmon02"], renaming=month_mapping.de, ordered=True
    )
    out["p_id_child_3"] = object_to_int(raw_data["kidpnr03"])
    out["birth_year_child_3"] = object_to_int(raw_data["kidgeb03"])
    out["birth_month_child_3"] = object_to_int_categorical(
        raw_data["kidmon03"], renaming=month_mapping.de, ordered=True
    )
    out["p_id_child_4"] = object_to_int(raw_data["kidpnr04"])
    out["birth_year_child_4"] = object_to_int(raw_data["kidgeb04"])
    out["birth_month_child_4"] = object_to_int_categorical(
        raw_data["kidmon04"], renaming=month_mapping.de, ordered=True
    )
    out["p_id_child_5"] = object_to_int(raw_data["kidpnr05"])
    out["birth_year_child_5"] = object_to_int(raw_data["kidgeb05"])
    out["birth_month_child_5"] = object_to_int_categorical(
        raw_data["kidmon05"], renaming=month_mapping.de, ordered=True
    )
    out["p_id_child_6"] = object_to_int(raw_data["kidpnr06"])
    out["birth_year_child_6"] = object_to_int(raw_data["kidgeb06"])
    out["birth_month_child_6"] = object_to_int_categorical(
        raw_data["kidmon06"], renaming=month_mapping.de, ordered=True
    )
    out["p_id_child_7"] = object_to_int(raw_data["kidpnr07"])
    out["birth_year_child_7"] = object_to_int(raw_data["kidgeb07"])
    out["birth_month_child_7"] = object_to_int_categorical(
        raw_data["kidmon07"], renaming=month_mapping.de, ordered=True
    )

    out["p_id_child_8"] = object_to_int(raw_data["kidpnr08"])
    out["birth_year_child_8"] = object_to_int(raw_data["kidgeb08"])
    out["birth_month_child_8"] = object_to_int_categorical(
        raw_data["kidmon08"], renaming=month_mapping.de, ordered=True
    )
    out["p_id_child_9"] = object_to_int(raw_data["kidpnr09"])
    out["birth_year_child_9"] = object_to_int(raw_data["kidgeb09"])
    out["birth_month_child_9"] = object_to_int_categorical(
        raw_data["kidmon09"], renaming=month_mapping.de, ordered=True
    )
    out["birth_year_child_10"] = object_to_int(raw_data["kidgeb10"])
    out["birth_month_child_10"] = object_to_int_categorical(
        raw_data["kidmon10"], renaming=month_mapping.de, ordered=True
    )
    out["birth_year_child_11"] = object_to_int(raw_data["kidgeb11"])
    out["birth_month_child_11"] = object_to_int_categorical(
        raw_data["kidmon11"], renaming=month_mapping.de, ordered=True
    )
    out["birth_year_child_12"] = object_to_int(raw_data["kidgeb12"])
    out["birth_month_child_12"] = object_to_int_categorical(
        raw_data["kidmon12"], renaming=month_mapping.de, ordered=True
    )
    out["birth_year_child_13"] = object_to_int(raw_data["kidgeb13"])
    out["birth_month_child_13"] = object_to_int_categorical(
        raw_data["kidmon13"], renaming=month_mapping.de, ordered=True
    )
    out["birth_year_child_14"] = object_to_int(raw_data["kidgeb14"])
    out["birth_month_child_14"] = object_to_int_categorical(
        raw_data["kidmon14"], renaming=month_mapping.de, ordered=True
    )
    out["birth_year_child_15"] = object_to_int(raw_data["kidgeb15"])
    out["birth_month_child_15"] = object_to_int_categorical(
        raw_data["kidmon15"], renaming=month_mapping.de, ordered=True
    )
    out["birth_year_child_16"] = object_to_int(raw_data["kidgeb16"])
    out["birth_month_child_16"] = object_to_int_categorical(
        raw_data["kidmon16"], renaming=month_mapping.de, ordered=True
    )
    return _wide_to_long(out)
