"""Clean and convert SOEP pkal variables to appropriate data types."""

import numpy as np
import pandas as pd

from soep_preparation.utilities.error_handling import fail_if_invalid_input
from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_int,
    object_to_str_categorical,
)


def _mutterschaftsgeld_monate(
    monate: "pd.Series[pd.Categorical]",
    bezug: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    out = pd.Series(
        np.maximum(
            (object_to_int(monate) - 3),
            0,
        ),
    )
    return apply_lowest_int_dtype(out.where(bezug != 0, 0))


def _categorical_dtype_ordered(
    dataframe: pd.DataFrame,
    var_name: str,
) -> pd.CategoricalDtype:
    variable_names = [f"{var_name}_{i}" for i in range(1, 13)]
    return pd.CategoricalDtype(
        [
            value
            for variable_name in variable_names
            for value in dataframe[variable_name].cat.categories.to_numpy()
        ],
    )


def _wide_to_long(dataframe: pd.DataFrame) -> pd.DataFrame:
    fail_if_invalid_input(dataframe, "pandas.core.frame.DataFrame")
    prev_wide_variables = [
        "ft_employed_v1",
        "ft_employed_v2",
        "pt_employed",
        "minijob_employed",
    ]
    out = pd.wide_to_long(
        dataframe,
        stubnames=prev_wide_variables,
        i=["hh_id_original", "hh_id", "p_id", "survey_year"],
        j="month",
        sep="_",
    ).reset_index()
    out = out.dropna(subset=prev_wide_variables, how="all")
    return out.astype(
        {
            "ft_employed_m_v1": _categorical_dtype_ordered(
                dataframe,
                "ft_employed_v1",
            ),
            "ft_employed_m_v2": _categorical_dtype_ordered(
                dataframe,
                "ft_employed_v2",
            ),
            "pt_employed": _categorical_dtype_ordered(dataframe, "pt_employed"),
            "minijob_employed": _categorical_dtype_ordered(
                dataframe, "minijob_employed"
            ),
        },
    ).reset_index(drop=True)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the pkal file.

    Args:
        raw_data: The raw pkal data.

    Returns:
        The processed pkal data.
    """
    out = pd.DataFrame()

    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["hh_id_original"] = apply_lowest_int_dtype(raw_data["cid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])

    # individual status previous calendar year
    out["unemployed_months_last_year"] = object_to_int(raw_data["kal1d02"]).fillna(0)
    out["early_retirement_pension_months_last_year"] = object_to_int(
        raw_data["kal1e02"]
    )
    out["unemployment_benefits_months_last_year"] = object_to_int(raw_data["kal2f02"])
    out["mutterschaftsgeld_bezug_pkal"] = object_to_bool_categorical(
        raw_data["kal2j01_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["mutterschaftsgeld_anzahl_monate_bezug"] = _mutterschaftsgeld_monate(
        raw_data["kal2j02"],
        out["mutterschaftsgeld_bezug_pkal"],
    )

    # individual employment status by month
    # January
    out["ft_employed_v1_1"] = object_to_str_categorical(
        raw_data["kal1a001_v1"],
        renaming={"[1] Ja": "Jan Vollzeit erwerbst."},
    )
    out["ft_employed_v2_1"] = object_to_str_categorical(raw_data["kal1a001_v2"])
    out["pt_employed_1"] = object_to_str_categorical(raw_data["kal1b001"])
    out["minijob_employed_1"] = object_to_str_categorical(
        raw_data["kal1n001"],
        renaming={1: "Jan"},
    )

    # February
    out["ft_employed_v1_2"] = object_to_str_categorical(
        raw_data["kal1a002_v1"],
        renaming={"[1] Ja": "Feb Vollzeit erwerbst."},
    )
    out["ft_employed_v2_2"] = object_to_str_categorical(raw_data["kal1a002_v2"])
    out["pt_employed_2"] = object_to_str_categorical(raw_data["kal1b002"])
    out["minijob_employed_2"] = object_to_str_categorical(
        raw_data["kal1n002"],
        renaming={1: "Feb"},
    )

    # March
    out["ft_employed_v1_3"] = object_to_str_categorical(
        raw_data["kal1a003_v1"],
        renaming={"[1] Ja": "Mar Vollzeit erwerbst."},
    )
    out["ft_employed_v2_3"] = object_to_str_categorical(raw_data["kal1a003_v2"])
    out["pt_employed_3"] = object_to_str_categorical(raw_data["kal1b003"])
    out["minijob_employed_3"] = object_to_str_categorical(
        raw_data["kal1n003"],
        renaming={1: "Mar"},
    )

    # April
    out["ft_employed_v1_4"] = object_to_str_categorical(
        raw_data["kal1a004_v1"],
        renaming={"[1] Ja": "Apr Vollzeit erwerbst."},
    )
    out["ft_employed_v2_4"] = object_to_str_categorical(raw_data["kal1a004_v2"])
    out["pt_employed_4"] = object_to_str_categorical(raw_data["kal1b004"])
    out["minijob_employed_4"] = object_to_str_categorical(
        raw_data["kal1n004"],
        renaming={1: "Apr"},
    )

    # May
    out["ft_employed_v1_5"] = object_to_str_categorical(
        raw_data["kal1a005_v1"],
        renaming={"[1] Ja": "Mai Vollzeit erwerbst."},
    )
    out["ft_employed_v2_5"] = object_to_str_categorical(raw_data["kal1a005_v2"])
    out["pt_employed_5"] = object_to_str_categorical(raw_data["kal1b005"])
    out["minijob_employed_5"] = object_to_str_categorical(
        raw_data["kal1n005"],
        renaming={1: "Mai"},
    )

    # June
    out["ft_employed_v1_6"] = object_to_str_categorical(
        raw_data["kal1a006_v1"],
        renaming={"[1] Ja": "Jun Vollzeit erwerbst."},
    )
    out["ft_employed_v2_6"] = object_to_str_categorical(raw_data["kal1a006_v2"])
    out["pt_employed_6"] = object_to_str_categorical(raw_data["kal1b006"])
    out["minijob_employed_6"] = object_to_str_categorical(
        raw_data["kal1n006"],
        renaming={1: "Jun"},
    )

    # July
    out["ft_employed_v1_7"] = object_to_str_categorical(
        raw_data["kal1a007_v1"],
        renaming={"[1] Ja": "Jul Vollzeit erwerbst."},
    )
    out["ft_employed_v2_7"] = object_to_str_categorical(raw_data["kal1a007_v2"])
    out["pt_employed_7"] = object_to_str_categorical(raw_data["kal1b007"])
    out["minijob_employed_7"] = object_to_str_categorical(
        raw_data["kal1n007"],
        renaming={1: "Jul"},
    )

    # August
    out["ft_employed_v1_8"] = object_to_str_categorical(
        raw_data["kal1a008_v1"],
        renaming={"[1] Ja": "Aug Vollzeit erwerbst."},
    )
    out["ft_employed_v2_8"] = object_to_str_categorical(raw_data["kal1a008_v2"])
    out["pt_employed_8"] = object_to_str_categorical(raw_data["kal1b008"])
    out["minijob_employed_8"] = object_to_str_categorical(
        raw_data["kal1n008"],
        renaming={1: "Aug"},
    )

    # September
    out["ft_employed_v1_9"] = object_to_str_categorical(
        raw_data["kal1a009_v1"],
        renaming={"[1] Ja": "Sep Vollzeit erwerbst."},
    )
    out["ft_employed_v2_9"] = object_to_str_categorical(raw_data["kal1a009_v2"])
    out["pt_employed_9"] = object_to_str_categorical(raw_data["kal1b009"])
    out["minijob_employed_9"] = object_to_str_categorical(
        raw_data["kal1n009"],
        renaming={1: "Sep"},
    )

    # October
    out["ft_employed_v1_10"] = object_to_str_categorical(
        raw_data["kal1a010_v1"],
        renaming={"[1] Ja": "Okt Vollzeit erwerbst."},
    )
    out["ft_employed_v2_10"] = object_to_str_categorical(raw_data["kal1a010_v2"])
    out["pt_employed_10"] = object_to_str_categorical(raw_data["kal1b010"])
    out["minijob_employed_10"] = object_to_str_categorical(
        raw_data["kal1n010"],
        renaming={1: "Okt"},
    )

    # November
    out["ft_employed_v1_11"] = object_to_str_categorical(
        raw_data["kal1a011_v1"],
        renaming={"[1] Ja": "Nov Vollzeit erwerbst."},
    )
    out["ft_employed_v2_11"] = object_to_str_categorical(raw_data["kal1a011_v2"])
    out["pt_employed_11"] = object_to_str_categorical(raw_data["kal1b011"])
    out["minijob_employed_11"] = object_to_str_categorical(
        raw_data["kal1n011"],
        renaming={1: "Nov"},
    )

    # December
    out["ft_employed_v1_12"] = object_to_str_categorical(
        raw_data["kal1a012_v1"],
        renaming={"[1] Ja": "Dez Vollzeit erwerbst."},
    )
    out["ft_employed_v2_12"] = object_to_str_categorical(raw_data["kal1a012_v2"])
    out["pt_employed_12"] = object_to_str_categorical(raw_data["kal1b012"])
    out["minijob_employed_12"] = object_to_str_categorical(
        raw_data["kal1n012"],
        renaming={1: "Dez"},
    )
    return _wide_to_long(out)
