"""Clean and convert SOEP biobirth variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities import month_mapping
from soep_preparation.utilities.data_manipulator import (
    float_to_int,
    object_to_int,
    object_to_int_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the biobirth data file.

    Args:
        raw_data: The raw biobirth data.

    Returns:
        The processed biobirth data.
    """
    wide = pd.DataFrame()
    wide["hh_id_original"] = float_to_int(raw_data["cid"])
    wide["p_id"] = float_to_int(raw_data["pid"])

    wide["tmp_number_of_children"] = float_to_int(raw_data["sumkids"])
    # the personal id of children (kidpnr) only exists for the first 9 children
    wide["tmp_p_id_child_1"] = object_to_int(raw_data["kidpnr01"])
    wide["tmp_birth_year_child_1"] = object_to_int(raw_data["kidgeb01"])
    wide["tmp_birth_month_child_1"] = object_to_int_categorical(
        raw_data["kidmon01"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_p_id_child_2"] = object_to_int(raw_data["kidpnr02"])
    wide["tmp_birth_year_child_2"] = object_to_int(raw_data["kidgeb02"])
    wide["tmp_birth_month_child_2"] = object_to_int_categorical(
        raw_data["kidmon02"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_p_id_child_3"] = object_to_int(raw_data["kidpnr03"])
    wide["tmp_birth_year_child_3"] = object_to_int(raw_data["kidgeb03"])
    wide["tmp_birth_month_child_3"] = object_to_int_categorical(
        raw_data["kidmon03"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_p_id_child_4"] = object_to_int(raw_data["kidpnr04"])
    wide["tmp_birth_year_child_4"] = object_to_int(raw_data["kidgeb04"])
    wide["tmp_birth_month_child_4"] = object_to_int_categorical(
        raw_data["kidmon04"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_p_id_child_5"] = object_to_int(raw_data["kidpnr05"])
    wide["tmp_birth_year_child_5"] = object_to_int(raw_data["kidgeb05"])
    wide["tmp_birth_month_child_5"] = object_to_int_categorical(
        raw_data["kidmon05"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_p_id_child_6"] = object_to_int(raw_data["kidpnr06"])
    wide["tmp_birth_year_child_6"] = object_to_int(raw_data["kidgeb06"])
    wide["tmp_birth_month_child_6"] = object_to_int_categorical(
        raw_data["kidmon06"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_p_id_child_7"] = object_to_int(raw_data["kidpnr07"])
    wide["tmp_birth_year_child_7"] = object_to_int(raw_data["kidgeb07"])
    wide["tmp_birth_month_child_7"] = object_to_int_categorical(
        raw_data["kidmon07"], renaming=month_mapping.de, ordered=True
    )

    wide["tmp_p_id_child_8"] = object_to_int(raw_data["kidpnr08"])
    wide["tmp_birth_year_child_8"] = object_to_int(raw_data["kidgeb08"])
    wide["tmp_birth_month_child_8"] = object_to_int_categorical(
        raw_data["kidmon08"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_p_id_child_9"] = object_to_int(raw_data["kidpnr09"])
    wide["tmp_birth_year_child_9"] = object_to_int(raw_data["kidgeb09"])
    wide["tmp_birth_month_child_9"] = object_to_int_categorical(
        raw_data["kidmon09"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_birth_year_child_10"] = object_to_int(raw_data["kidgeb10"])
    wide["tmp_birth_month_child_10"] = object_to_int_categorical(
        raw_data["kidmon10"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_birth_year_child_11"] = object_to_int(raw_data["kidgeb11"])
    wide["tmp_birth_month_child_11"] = object_to_int_categorical(
        raw_data["kidmon11"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_birth_year_child_12"] = object_to_int(raw_data["kidgeb12"])
    wide["tmp_birth_month_child_12"] = object_to_int_categorical(
        raw_data["kidmon12"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_birth_year_child_13"] = object_to_int(raw_data["kidgeb13"])
    wide["tmp_birth_month_child_13"] = object_to_int_categorical(
        raw_data["kidmon13"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_birth_year_child_14"] = object_to_int(raw_data["kidgeb14"])
    wide["tmp_birth_month_child_14"] = object_to_int_categorical(
        raw_data["kidmon14"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_birth_year_child_15"] = object_to_int(raw_data["kidgeb15"])
    wide["tmp_birth_month_child_15"] = object_to_int_categorical(
        raw_data["kidmon15"], renaming=month_mapping.de, ordered=True
    )
    wide["tmp_birth_year_child_16"] = object_to_int(raw_data["kidgeb16"])
    wide["tmp_birth_month_child_16"] = object_to_int_categorical(
        raw_data["kidmon16"], renaming=month_mapping.de, ordered=True
    )

    # Transforming the wide data to long format
    prev_wide_variables = [
        "tmp_birth_year_child",
        "tmp_p_id_child",
        "tmp_birth_month_child",
    ]
    # We remove rows that are completely empty that is,
    # all non-existent children of the potential enumeration are dropped
    # (individual with 2 children only takes two rows in the final data)
    tmp_long = (
        pd.wide_to_long(
            wide,
            stubnames=prev_wide_variables,
            i=["hh_id_original", "p_id"],
            j="tmp_child_number",
            sep="_",
        )
        .dropna(subset=prev_wide_variables, how="all")
        .reset_index()
    )

    long = pd.DataFrame()
    long["hh_id_original"] = tmp_long["hh_id_original"]
    long["p_id"] = tmp_long["p_id"]
    long["number_of_children"] = tmp_long["tmp_number_of_children"]
    long["child_number"] = tmp_long["tmp_child_number"]
    long["p_id_child"] = tmp_long["tmp_p_id_child"]
    long["birth_year_child"] = tmp_long["tmp_birth_year_child"]
    long["birth_month_child"] = tmp_long["tmp_birth_month_child"]

    return long
