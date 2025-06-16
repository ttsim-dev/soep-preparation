"""Clean and convert SOEP pkal variables to appropriate data types."""

import numpy as np
import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_int_dtype,
    object_to_bool_categorical,
    object_to_int,
    object_to_str_categorical,
)


def _mutterschaftsgeld_monate(
    monate: "pd.Series[pd.Categorical]",
    bezug: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    wide = pd.Series(
        np.maximum(
            (object_to_int(monate) - 3),
            0,
        ),
    )
    return apply_smallest_int_dtype(wide.where(bezug != 0, 0))


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


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the pkal file.

    Args:
        raw_data: The raw pkal data.

    Returns:
        The processed pkal data.
    """
    wide = pd.DataFrame()

    wide["tmp_p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    wide["tmp_hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    wide["tmp_hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    wide["tmp_survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    # individual status previous calendar year
    wide["tmp_unemployed_m"] = object_to_int(raw_data["kal1d02"]).fillna(0)
    wide["tmp_early_retirement_pension_m"] = object_to_int(raw_data["kal1e02"])
    wide["tmp_unemployment_benefits_m"] = object_to_int(raw_data["kal2f02"])
    wide["tmp_mutterschaftsgeld_bezug_pkal"] = object_to_bool_categorical(
        raw_data["kal2j01_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    wide["tmp_mutterschaftsgeld_bezug_anzahl_m"] = _mutterschaftsgeld_monate(
        raw_data["kal2j02"],
        wide["tmp_mutterschaftsgeld_bezug_pkal"],
    )

    # individual employment status by month
    # January
    wide["tmp_ft_employed_m_v1_1"] = object_to_str_categorical(
        series=raw_data["kal1a001_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_1"] = object_to_str_categorical(
        series=raw_data["kal1a001_v2"],
        renaming={
            "[1] Jan Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jan Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_1"] = object_to_str_categorical(
        series=raw_data["kal1b001"],
        renaming={
            "[1] Jan Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Jan Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_1"] = object_to_str_categorical(
        raw_data["kal1n001"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # February
    wide["tmp_ft_employed_m_v1_2"] = object_to_str_categorical(
        series=raw_data["kal1a002_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_2"] = object_to_str_categorical(
        series=raw_data["kal1a002_v2"],
        renaming={
            "[1] Feb Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Feb Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_2"] = object_to_str_categorical(
        series=raw_data["kal1b002"],
        renaming={
            "[1] Feb Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Feb Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_2"] = object_to_str_categorical(
        raw_data["kal1n002"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # March
    wide["tmp_ft_employed_m_v1_3"] = object_to_str_categorical(
        series=raw_data["kal1a003_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_3"] = object_to_str_categorical(
        series=raw_data["kal1a003_v2"],
        renaming={
            "[1] Mrz Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Mrz Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_3"] = object_to_str_categorical(
        series=raw_data["kal1b003"],
        renaming={
            "[1] Mrz Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Mrz Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_3"] = object_to_str_categorical(
        raw_data["kal1n003"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # April
    wide["tmp_ft_employed_m_v1_4"] = object_to_str_categorical(
        series=raw_data["kal1a004_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_4"] = object_to_str_categorical(
        series=raw_data["kal1a004_v2"],
        renaming={
            "[1] Apr Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Apr Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_4"] = object_to_str_categorical(
        series=raw_data["kal1b004"],
        renaming={
            "[1] Apr Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Apr Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_4"] = object_to_str_categorical(
        raw_data["kal1n004"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # May
    wide["tmp_ft_employed_m_v1_5"] = object_to_str_categorical(
        series=raw_data["kal1a005_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_5"] = object_to_str_categorical(
        series=raw_data["kal1a005_v2"],
        renaming={
            "[1] Mai Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Mai Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_5"] = object_to_str_categorical(
        series=raw_data["kal1b005"],
        renaming={
            "[1] Mai Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Mai Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_5"] = object_to_str_categorical(
        raw_data["kal1n005"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # June
    wide["tmp_ft_employed_m_v1_6"] = object_to_str_categorical(
        series=raw_data["kal1a006_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_6"] = object_to_str_categorical(
        series=raw_data["kal1a006_v2"],
        renaming={
            "[1] Jun Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jun Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_6"] = object_to_str_categorical(
        series=raw_data["kal1b006"],
        renaming={
            "[1] Jun Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Jun Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_6"] = object_to_str_categorical(
        raw_data["kal1n006"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # July
    wide["tmp_ft_employed_m_v1_7"] = object_to_str_categorical(
        series=raw_data["kal1a007_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_7"] = object_to_str_categorical(
        series=raw_data["kal1a007_v2"],
        renaming={
            "[1] Jul Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jul Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_7"] = object_to_str_categorical(
        series=raw_data["kal1b007"],
        renaming={
            "[1] Jul Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Jul Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_7"] = object_to_str_categorical(
        raw_data["kal1n007"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # August
    wide["tmp_ft_employed_m_v1_8"] = object_to_str_categorical(
        series=raw_data["kal1a008_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_8"] = object_to_str_categorical(
        series=raw_data["kal1a008_v2"],
        renaming={
            "[1] Aug Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Aug Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_8"] = object_to_str_categorical(
        series=raw_data["kal1b008"],
        renaming={
            "[1] Aug Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Aug Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_8"] = object_to_str_categorical(
        raw_data["kal1n008"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # September
    wide["tmp_ft_employed_m_v1_9"] = object_to_str_categorical(
        series=raw_data["kal1a009_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_9"] = object_to_str_categorical(
        series=raw_data["kal1a009_v2"],
        renaming={
            "[1] Sep Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Sep Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_9"] = object_to_str_categorical(
        series=raw_data["kal1b009"],
        renaming={
            "[1] Sep Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Sep Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_9"] = object_to_str_categorical(
        raw_data["kal1n009"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # October
    wide["tmp_ft_employed_m_v1_10"] = object_to_str_categorical(
        series=raw_data["kal1a010_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_10"] = object_to_str_categorical(
        series=raw_data["kal1a010_v2"],
        renaming={
            "[1] Okt Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Okt Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_10"] = object_to_str_categorical(
        series=raw_data["kal1b010"],
        renaming={
            "[1] Okt Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Okt Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_10"] = object_to_str_categorical(
        raw_data["kal1n010"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # November
    wide["tmp_ft_employed_m_v1_11"] = object_to_str_categorical(
        series=raw_data["kal1a011_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_11"] = object_to_str_categorical(
        series=raw_data["kal1a011_v2"],
        renaming={
            "[1] Nov Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Nov Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_11"] = object_to_str_categorical(
        series=raw_data["kal1b011"],
        renaming={
            "[1] Nov Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Nov Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_11"] = object_to_str_categorical(
        raw_data["kal1n011"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # December
    wide["tmp_ft_employed_m_v1_12"] = object_to_str_categorical(
        series=raw_data["kal1a012_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    wide["tmp_ft_employed_m_v2_12"] = object_to_str_categorical(
        series=raw_data["kal1a012_v2"],
        renaming={
            "[1] Dez Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Dez Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_pt_employed_m_12"] = object_to_str_categorical(
        series=raw_data["kal1b012"],
        renaming={
            "[1] Dez Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Dez Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",
        },
    )
    wide["tmp_minijob_employed_m_12"] = object_to_str_categorical(
        raw_data["kal1n012"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Transforming the wide data to long format
    # (the following columns were in wide format)
    prev_wide_cols = [
        "tmp_ft_employed_m_v1",
        "tmp_ft_employed_m_v2",
        "tmp_pt_employed_m",
        "tmp_minijob_employed_m",
    ]
    # Notice that no rows have been dropped here, since the other columns are meaningful
    # in themselves, even when employment information is missing.
    tmp_long = pd.wide_to_long(
        wide,
        stubnames=prev_wide_cols,
        i=["tmp_hh_id_original", "tmp_hh_id", "tmp_p_id", "tmp_survey_year"],
        j="tmp_month_numerical",
        sep="_",
    ).reset_index()

    long = pd.DataFrame()

    long["p_id"] = tmp_long["tmp_p_id"]
    long["hh_id"] = tmp_long["tmp_hh_id"]
    long["hh_id_original"] = tmp_long["tmp_hh_id_original"]
    long["survey_year"] = tmp_long["tmp_survey_year"]

    long["month_numerical"] = tmp_long["tmp_month_numerical"]

    long["unemployed_m"] = tmp_long["tmp_unemployed_m"]
    long["early_retirement_pension_m"] = tmp_long["tmp_early_retirement_pension_m"]
    long["unemployment_benefits_m"] = tmp_long["tmp_unemployment_benefits_m"]
    long["mutterschaftsgeld_bezug_pkal"] = tmp_long["tmp_mutterschaftsgeld_bezug_pkal"]
    long["mutterschaftsgeld_bezug_anzahl_m"] = apply_smallest_int_dtype(
        tmp_long["tmp_mutterschaftsgeld_bezug_anzahl_m"]
    )

    long["ft_employed_m_v1"] = tmp_long["tmp_ft_employed_m_v1"]
    long["ft_employed_m_v2"] = tmp_long["tmp_ft_employed_m_v2"]
    long["pt_employed_m"] = tmp_long["tmp_pt_employed_m"]
    long["minijob_employed_m"] = tmp_long["tmp_minijob_employed_m"]
    return long
