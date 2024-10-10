from soep_cleaning.config import pd
from soep_cleaning.initial_cleaning import month_mapping
from soep_cleaning.utilities import (
    _fail_if_invalid_input,
    categorical_to_int_categorical,
    find_lowest_int_dtype,
    float_categorical_to_int,
    int_to_int_categorical,
)


def _wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    _fail_if_invalid_input(df, "pandas.core.frame.DataFrame")
    prev_wide_cols = ["birth_year_child", "p_id_child", "birth_month_child"]
    df = pd.wide_to_long(
        df,
        stubnames=prev_wide_cols,
        i=["soep_initial_hh_id", "p_id"],
        j="child_number",
        sep="_",
    ).reset_index()
    df = df.dropna(subset=prev_wide_cols, how="all")
    return df.astype(
        {
            "birth_year_child": find_lowest_int_dtype(df["birth_year_child"]),
            "p_id_child": find_lowest_int_dtype(df["p_id_child"]),
            "birth_month_child": "category",
        },
    ).reset_index(drop=True)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = float_categorical_to_int(raw["cid"])
    out["p_id"] = float_categorical_to_int(raw["pid"])
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
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_2"] = categorical_to_int_categorical(
        raw["kidmon02"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_3"] = categorical_to_int_categorical(
        raw["kidmon03"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_4"] = categorical_to_int_categorical(
        raw["kidmon04"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_5"] = categorical_to_int_categorical(
        raw["kidmon05"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_6"] = categorical_to_int_categorical(
        raw["kidmon06"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_7"] = categorical_to_int_categorical(
        raw["kidmon07"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_8"] = categorical_to_int_categorical(
        raw["kidmon08"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_9"] = categorical_to_int_categorical(
        raw["kidmon09"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_10"] = categorical_to_int_categorical(
        raw["kidmon10"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_11"] = categorical_to_int_categorical(
        raw["kidmon11"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_12"] = categorical_to_int_categorical(
        raw["kidmon12"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_13"] = categorical_to_int_categorical(
        raw["kidmon13"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_14"] = categorical_to_int_categorical(
        raw["kidmon14"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_15"] = categorical_to_int_categorical(
        raw["kidmon15"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    out["birth_month_child_16"] = categorical_to_int_categorical(
        raw["kidmon16"],
        renaming=month_mapping.de,
        ordered=False,
        filter_renaming=True,
    )
    return _wide_to_long(out)
