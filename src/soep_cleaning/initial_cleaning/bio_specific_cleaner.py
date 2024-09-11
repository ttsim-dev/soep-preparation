from soep_cleaning.config import pd
from soep_cleaning.initial_cleaning import month_mapping
from soep_cleaning.initial_cleaning.helper import (
    biobirth_wide_to_long,
    bool_categorical,
    categorical_to_int_categorical,
    float_categorical_to_int,
    int_categorical_to_int,
    int_to_int_categorical,
    str_categorical,
)
from soep_cleaning.utilities import apply_lowest_int_dtype


def biobirth(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(
        float_categorical_to_int(raw["cid"]),
    )
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["n_kids_total"] = int_to_int_categorical(
        float_categorical_to_int(raw["sumkids"]),
    )

    out["birth_year_child_1"] = float_categorical_to_int(
        raw["kidgeb01"],
    )
    out["birth_year_child_2"] = float_categorical_to_int(
        raw["kidgeb02"],
    )
    out["birth_year_child_3"] = float_categorical_to_int(
        raw["kidgeb03"],
    )
    out["birth_year_child_4"] = float_categorical_to_int(
        raw["kidgeb04"],
    )
    out["birth_year_child_5"] = float_categorical_to_int(
        raw["kidgeb05"],
    )
    out["birth_year_child_6"] = float_categorical_to_int(
        raw["kidgeb06"],
    )
    out["birth_year_child_7"] = float_categorical_to_int(
        raw["kidgeb07"],
    )
    out["birth_year_child_8"] = float_categorical_to_int(
        raw["kidgeb08"],
    )
    out["birth_year_child_9"] = float_categorical_to_int(
        raw["kidgeb09"],
    )
    out["birth_year_child_10"] = float_categorical_to_int(
        raw["kidgeb10"],
    )
    out["birth_year_child_11"] = float_categorical_to_int(
        raw["kidgeb11"],
    )
    out["birth_year_child_12"] = float_categorical_to_int(
        raw["kidgeb12"],
    )
    out["birth_year_child_13"] = float_categorical_to_int(
        raw["kidgeb13"],
    )
    out["birth_year_child_14"] = float_categorical_to_int(
        raw["kidgeb14"],
    )
    out["birth_year_child_15"] = float_categorical_to_int(
        raw["kidgeb15"],
    )
    out["birth_year_child_16"] = float_categorical_to_int(
        raw["kidgeb16"],
    )

    out["p_id_child_1"] = float_categorical_to_int(
        raw["kidpnr01"],
    )
    out["p_id_child_2"] = float_categorical_to_int(
        raw["kidpnr02"],
    )

    out["birth_month_child_1"] = categorical_to_int_categorical(
        raw["kidmon01"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_2"] = categorical_to_int_categorical(
        raw["kidmon02"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_3"] = categorical_to_int_categorical(
        raw["kidmon03"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_4"] = categorical_to_int_categorical(
        raw["kidmon04"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_5"] = categorical_to_int_categorical(
        raw["kidmon05"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_6"] = categorical_to_int_categorical(
        raw["kidmon06"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_7"] = categorical_to_int_categorical(
        raw["kidmon07"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_8"] = categorical_to_int_categorical(
        raw["kidmon08"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_9"] = categorical_to_int_categorical(
        raw["kidmon09"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_10"] = categorical_to_int_categorical(
        raw["kidmon10"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_11"] = categorical_to_int_categorical(
        raw["kidmon11"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_12"] = categorical_to_int_categorical(
        raw["kidmon12"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_13"] = categorical_to_int_categorical(
        raw["kidmon13"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_14"] = categorical_to_int_categorical(
        raw["kidmon14"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_15"] = categorical_to_int_categorical(
        raw["kidmon15"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    out["birth_month_child_16"] = categorical_to_int_categorical(
        raw["kidmon16"],
        renaming=month_mapping.de,
        filter_renaming=True,
        ordered=False,
    )
    return biobirth_wide_to_long(out)


def bioedu(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the bioedu dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])

    out["birth_month"] = categorical_to_int_categorical(
        raw["gebmonat"],
        ordered=False,
        renaming=month_mapping.en,
    )
    return out


def biol(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = int_categorical_to_int(raw["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])

    out["year"] = int_categorical_to_int(raw["syear"])

    out["birthplace"] = str_categorical(
        raw["lb0013_h"],
        ordered=False,
    )
    out["res_childhood"] = str_categorical(
        raw["lb0058"],
        ordered=False,
    )
    out["birthplace_father"] = bool_categorical(
        raw["lb0084_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["birthplace_mother"] = bool_categorical(
        raw["lb0085_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["religion_father"] = str_categorical(
        raw["lb0124_h"],
        ordered=False,
    )
    out["religion_mother"] = str_categorical(
        raw["lb0125_h"],
        ordered=False,
    )
    return out
