"""Functions to pre-process variables for a raw pkal dataset."""

import numpy as np
import pandas as pd

from soep_preparation.utilities import (
    _fail_if_invalid_input,
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_int,
    object_to_str_categorical,
)


def _mutterschaftsgeld_monate_prev(
    monate_prev: "pd.Series[pd.Categorical]",
    bezogen_prev: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    out = pd.Series(
        np.maximum(
            (object_to_int(monate_prev) - 3),
            0,
        ),
    )
    return apply_lowest_int_dtype(out.where(bezogen_prev != 0, 0))


def _categorical_dtype_ordered(df: pd.DataFrame, col_group: str) -> pd.CategoricalDtype:
    columns = [f"{col_group}_{i}" for i in range(1, 13)]
    return pd.CategoricalDtype(
        [value for col in columns for value in df[col].cat.categories.to_numpy()],
    )


def _wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    _fail_if_invalid_input(df, "pandas.core.frame.DataFrame")
    prev_wide_cols = [
        "full_empl_v1_prev",
        "full_empl_v2_prev",
        "half_empl_prev",
        "mini_job_prev",
    ]
    out = pd.wide_to_long(
        df,
        stubnames=prev_wide_cols,
        i=["hh_id_orig", "hh_id", "p_id", "survey_year"],
        j="month",
        sep="_",
    ).reset_index()
    out = out.dropna(subset=prev_wide_cols, how="all")
    return out.astype(
        {
            "full_empl_v1_prev": _categorical_dtype_ordered(df, "full_empl_v1_prev"),
            "full_empl_v2_prev": _categorical_dtype_ordered(df, "full_empl_v2_prev"),
            "half_empl_prev": _categorical_dtype_ordered(df, "half_empl_prev"),
            "mini_job_prev": _categorical_dtype_ordered(df, "mini_job_prev"),
        },
    ).reset_index(drop=True)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pkal dataset."""
    out = pd.DataFrame()

    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["hh_id_orig"] = apply_lowest_int_dtype(raw_data["cid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])

    out["full_empl_v1_prev_1"] = object_to_str_categorical(
        raw_data["kal1a001_v1"],
        renaming={"[1] Ja": "Jan Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_2"] = object_to_str_categorical(
        raw_data["kal1a002_v1"],
        renaming={"[1] Ja": "Feb Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_3"] = object_to_str_categorical(
        raw_data["kal1a003_v1"],
        renaming={"[1] Ja": "Mar Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_4"] = object_to_str_categorical(
        raw_data["kal1a004_v1"],
        renaming={"[1] Ja": "Apr Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_5"] = object_to_str_categorical(
        raw_data["kal1a005_v1"],
        renaming={"[1] Ja": "Mai Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_6"] = object_to_str_categorical(
        raw_data["kal1a006_v1"],
        renaming={"[1] Ja": "Jun Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_7"] = object_to_str_categorical(
        raw_data["kal1a007_v1"],
        renaming={"[1] Ja": "Jul Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_8"] = object_to_str_categorical(
        raw_data["kal1a008_v1"],
        renaming={"[1] Ja": "Aug Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_9"] = object_to_str_categorical(
        raw_data["kal1a009_v1"],
        renaming={"[1] Ja": "Sep Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_10"] = object_to_str_categorical(
        raw_data["kal1a010_v1"],
        renaming={"[1] Ja": "Okt Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_11"] = object_to_str_categorical(
        raw_data["kal1a011_v1"],
        renaming={"[1] Ja": "Nov Vollzeit erwerbst."},
    )
    out["full_empl_v1_prev_12"] = object_to_str_categorical(
        raw_data["kal1a012_v1"],
        renaming={"[1] Ja": "Dez Vollzeit erwerbst."},
    )

    out["full_empl_v2_prev_1"] = object_to_str_categorical(raw_data["kal1a001_v2"])
    out["full_empl_v2_prev_2"] = object_to_str_categorical(raw_data["kal1a002_v2"])
    out["full_empl_v2_prev_3"] = object_to_str_categorical(raw_data["kal1a003_v2"])
    out["full_empl_v2_prev_4"] = object_to_str_categorical(raw_data["kal1a004_v2"])
    out["full_empl_v2_prev_5"] = object_to_str_categorical(raw_data["kal1a005_v2"])
    out["full_empl_v2_prev_6"] = object_to_str_categorical(raw_data["kal1a006_v2"])
    out["full_empl_v2_prev_7"] = object_to_str_categorical(raw_data["kal1a007_v2"])
    out["full_empl_v2_prev_8"] = object_to_str_categorical(raw_data["kal1a008_v2"])
    out["full_empl_v2_prev_9"] = object_to_str_categorical(raw_data["kal1a009_v2"])
    out["full_empl_v2_prev_10"] = object_to_str_categorical(raw_data["kal1a010_v2"])
    out["full_empl_v2_prev_11"] = object_to_str_categorical(raw_data["kal1a011_v2"])
    out["full_empl_v2_prev_12"] = object_to_str_categorical(raw_data["kal1a012_v2"])

    out["half_empl_prev_1"] = object_to_str_categorical(raw_data["kal1b001"])
    out["half_empl_prev_2"] = object_to_str_categorical(raw_data["kal1b002"])
    out["half_empl_prev_3"] = object_to_str_categorical(raw_data["kal1b003"])
    out["half_empl_prev_4"] = object_to_str_categorical(raw_data["kal1b004"])
    out["half_empl_prev_5"] = object_to_str_categorical(raw_data["kal1b005"])
    out["half_empl_prev_6"] = object_to_str_categorical(raw_data["kal1b006"])
    out["half_empl_prev_7"] = object_to_str_categorical(raw_data["kal1b007"])
    out["half_empl_prev_8"] = object_to_str_categorical(raw_data["kal1b008"])
    out["half_empl_prev_9"] = object_to_str_categorical(raw_data["kal1b009"])
    out["half_empl_prev_10"] = object_to_str_categorical(raw_data["kal1b010"])
    out["half_empl_prev_11"] = object_to_str_categorical(raw_data["kal1b011"])
    out["half_empl_prev_12"] = object_to_str_categorical(raw_data["kal1b012"])

    out["mini_job_prev_1"] = object_to_str_categorical(
        raw_data["kal1n001"],
        renaming={1: "Jan"},
    )
    out["mini_job_prev_2"] = object_to_str_categorical(
        raw_data["kal1n002"],
        renaming={1: "Feb"},
    )
    out["mini_job_prev_3"] = object_to_str_categorical(
        raw_data["kal1n003"],
        renaming={1: "Mar"},
    )
    out["mini_job_prev_4"] = object_to_str_categorical(
        raw_data["kal1n004"],
        renaming={1: "Apr"},
    )
    out["mini_job_prev_5"] = object_to_str_categorical(
        raw_data["kal1n005"],
        renaming={1: "Mai"},
    )
    out["mini_job_prev_6"] = object_to_str_categorical(
        raw_data["kal1n006"],
        renaming={1: "Jun"},
    )
    out["mini_job_prev_7"] = object_to_str_categorical(
        raw_data["kal1n007"],
        renaming={1: "Jul"},
    )
    out["mini_job_prev_8"] = object_to_str_categorical(
        raw_data["kal1n008"],
        renaming={1: "Aug"},
    )
    out["mini_job_prev_9"] = object_to_str_categorical(
        raw_data["kal1n009"],
        renaming={1: "Sep"},
    )
    out["mini_job_prev_10"] = object_to_str_categorical(
        raw_data["kal1n010"],
        renaming={1: "Okt"},
    )
    out["mini_job_prev_11"] = object_to_str_categorical(
        raw_data["kal1n011"],
        renaming={1: "Nov"},
    )
    out["mini_job_prev_12"] = object_to_str_categorical(
        raw_data["kal1n012"],
        renaming={1: "Dez"},
    )

    out["unempl_months_prev"] = object_to_int(raw_data["kal1d02"]).fillna(0)
    out["rente_monate_prev"] = object_to_int(raw_data["kal1e02"])
    out["m_alg_prev"] = object_to_int(raw_data["kal2f02"])
    out["mutterschaftsgeld_bezogen_prev"] = object_to_bool_categorical(
        raw_data["kal2j01_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )  # replaced var kal2j01 by kal2j01_h
    out["mutterschaftsgeld_monate_prev_inconsistent"] = object_to_int(
        raw_data["kal2j02"],
    )
    out["mutterschaftsgeld_monate_prev"] = _mutterschaftsgeld_monate_prev(
        raw_data["kal2j02"],
        out["mutterschaftsgeld_bezogen_prev"],
    )

    return _wide_to_long(out)
