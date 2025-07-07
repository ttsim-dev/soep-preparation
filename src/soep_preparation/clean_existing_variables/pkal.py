"""Clean and convert SOEP pkal variables to appropriate data types."""

import numpy as np
import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    combine_first_and_make_categorical,
    object_to_bool_categorical,
    object_to_int,
    object_to_str_categorical,
)


def _mutterschaftsgeld_monate(
    monate: "pd.Series[pd.Categorical]",
    bezug: "pd.Series[pd.Categorical]",
) -> "pd.Series[int]":
    sr = pd.Series(
        np.maximum(
            (object_to_int(monate) - 3),
            0,
        ),
    )
    return apply_smallest_int_dtype(sr.where(bezug != 0, 0))


def _number_of_months_employed(
    data: pd.DataFrame,
) -> pd.Series:
    months = range(1, 13)
    kinds = ["ft", "pt", "minijob"]

    # Per-month: employed if any of the types has a category in [0,1]
    employed = pd.concat(
        [
            pd.concat(
                [
                    data[f"{kind}_employed_m_{month}"].cat.codes.between(0, 1)
                    for kind in kinds
                ],
                axis=1,
            ).any(axis=1)
            for month in months
        ],
        axis=1,
    )

    # Compute per-month non-missingness
    nonmissing = pd.concat(
        [
            data[[f"{kind}_employed_m_{month}" for kind in ["ft", "pt", "minijob"]]]
            .notna()
            .any(axis=1)
            for month in months
        ],
        axis=1,
    )

    # Mask employed months where all 3 types were NA
    employed_masked = employed.where(nonmissing, pd.NA)

    # Mask full-row NA
    valid_rows = nonmissing.any(axis=1)

    return apply_smallest_int_dtype(
        employed_masked.sum(axis=1).where(valid_rows, pd.NA)
    )


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the pkal data file.

    Args:
        raw_data: The raw pkal data.

    Returns:
        The processed pkal data.
    """
    out = pd.DataFrame()

    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    # individual status previous calendar year
    out["unemployed_m"] = object_to_int(raw_data["kal1d02"]).fillna(0)
    out["early_retirement_pension_m"] = object_to_int(raw_data["kal1e02"])
    out["unemployment_benefits_m"] = object_to_int(raw_data["kal2f02"])
    out["mutterschaftsgeld_bezug_pkal"] = object_to_bool_categorical(
        raw_data["kal2j01_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["mutterschaftsgeld_bezug_anzahl_m"] = _mutterschaftsgeld_monate(
        raw_data["kal2j02"],
        out["mutterschaftsgeld_bezug_pkal"],
    )

    # the first full time employment variables includes the timeframe 1984 until 1997,
    # the second the timeframe 1998 until 2022
    # individual employment status by month
    # Month 1 - Jan
    out["tmp_ft_employed_m_v1_1"] = object_to_str_categorical(
        series=raw_data["kal1a001_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_1"] = object_to_str_categorical(
        series=raw_data["kal1a001_v2"],
        renaming={
            "[1] Jan Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jan Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_1"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_1"], out["tmp_ft_employed_m_v2_1"], False
    )
    out["pt_employed_m_1"] = object_to_str_categorical(
        series=raw_data["kal1b001"],
        renaming={
            "[1] Jan Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Jan Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_1"] = object_to_str_categorical(
        raw_data["kal1n001"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 2 - Feb
    out["tmp_ft_employed_m_v1_2"] = object_to_str_categorical(
        series=raw_data["kal1a002_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_2"] = object_to_str_categorical(
        series=raw_data["kal1a002_v2"],
        renaming={
            "[1] Feb Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Feb Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_2"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_2"], out["tmp_ft_employed_m_v2_2"], False
    )
    out["pt_employed_m_2"] = object_to_str_categorical(
        series=raw_data["kal1b002"],
        renaming={
            "[1] Feb Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Feb Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_2"] = object_to_str_categorical(
        raw_data["kal1n002"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 3 - Mrz
    out["tmp_ft_employed_m_v1_3"] = object_to_str_categorical(
        series=raw_data["kal1a003_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_3"] = object_to_str_categorical(
        series=raw_data["kal1a003_v2"],
        renaming={
            "[1] Mrz Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Mrz Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_3"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_3"], out["tmp_ft_employed_m_v2_3"], False
    )
    out["pt_employed_m_3"] = object_to_str_categorical(
        series=raw_data["kal1b003"],
        renaming={
            "[1] Mrz Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Mrz Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_3"] = object_to_str_categorical(
        raw_data["kal1n003"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 4 - Apr
    out["tmp_ft_employed_m_v1_4"] = object_to_str_categorical(
        series=raw_data["kal1a004_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_4"] = object_to_str_categorical(
        series=raw_data["kal1a004_v2"],
        renaming={
            "[1] Apr Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Apr Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_4"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_4"], out["tmp_ft_employed_m_v2_4"], False
    )
    out["pt_employed_m_4"] = object_to_str_categorical(
        series=raw_data["kal1b004"],
        renaming={
            "[1] Apr Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Apr Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_4"] = object_to_str_categorical(
        raw_data["kal1n004"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 5 - Mai
    out["tmp_ft_employed_m_v1_5"] = object_to_str_categorical(
        series=raw_data["kal1a005_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_5"] = object_to_str_categorical(
        series=raw_data["kal1a005_v2"],
        renaming={
            "[1] Mai Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Mai Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_5"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_5"], out["tmp_ft_employed_m_v2_5"], False
    )
    out["pt_employed_m_5"] = object_to_str_categorical(
        series=raw_data["kal1b005"],
        renaming={
            "[1] Mai Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Mai Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_5"] = object_to_str_categorical(
        raw_data["kal1n005"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 6 - Jun
    out["tmp_ft_employed_m_v1_6"] = object_to_str_categorical(
        series=raw_data["kal1a006_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_6"] = object_to_str_categorical(
        series=raw_data["kal1a006_v2"],
        renaming={
            "[1] Jun Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jun Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_6"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_6"], out["tmp_ft_employed_m_v2_6"], False
    )
    out["pt_employed_m_6"] = object_to_str_categorical(
        series=raw_data["kal1b006"],
        renaming={
            "[1] Jun Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Jun Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_6"] = object_to_str_categorical(
        raw_data["kal1n006"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 7 - Jul
    out["tmp_ft_employed_m_v1_7"] = object_to_str_categorical(
        series=raw_data["kal1a007_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_7"] = object_to_str_categorical(
        series=raw_data["kal1a007_v2"],
        renaming={
            "[1] Jul Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jul Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_7"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_7"], out["tmp_ft_employed_m_v2_7"], False
    )
    out["pt_employed_m_7"] = object_to_str_categorical(
        series=raw_data["kal1b007"],
        renaming={
            "[1] Jul Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Jul Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_7"] = object_to_str_categorical(
        raw_data["kal1n007"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 8 - Aug
    out["tmp_ft_employed_m_v1_8"] = object_to_str_categorical(
        series=raw_data["kal1a008_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_8"] = object_to_str_categorical(
        series=raw_data["kal1a008_v2"],
        renaming={
            "[1] Aug Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Aug Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_8"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_8"], out["tmp_ft_employed_m_v2_8"], False
    )
    out["pt_employed_m_8"] = object_to_str_categorical(
        series=raw_data["kal1b008"],
        renaming={
            "[1] Aug Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Aug Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_8"] = object_to_str_categorical(
        raw_data["kal1n008"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 9 - Sep
    out["tmp_ft_employed_m_v1_9"] = object_to_str_categorical(
        series=raw_data["kal1a009_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_9"] = object_to_str_categorical(
        series=raw_data["kal1a009_v2"],
        renaming={
            "[1] Sep Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Sep Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_9"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_9"], out["tmp_ft_employed_m_v2_9"], False
    )
    out["pt_employed_m_9"] = object_to_str_categorical(
        series=raw_data["kal1b009"],
        renaming={
            "[1] Sep Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Sep Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_9"] = object_to_str_categorical(
        raw_data["kal1n009"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 10 - Okt
    out["tmp_ft_employed_m_v1_10"] = object_to_str_categorical(
        series=raw_data["kal1a010_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_10"] = object_to_str_categorical(
        series=raw_data["kal1a010_v2"],
        renaming={
            "[1] Okt Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Okt Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_10"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_10"], out["tmp_ft_employed_m_v2_10"], False
    )
    out["pt_employed_m_10"] = object_to_str_categorical(
        series=raw_data["kal1b010"],
        renaming={
            "[1] Okt Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Okt Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_10"] = object_to_str_categorical(
        raw_data["kal1n010"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 11 - Nov
    out["tmp_ft_employed_m_v1_11"] = object_to_str_categorical(
        series=raw_data["kal1a011_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_11"] = object_to_str_categorical(
        series=raw_data["kal1a011_v2"],
        renaming={
            "[1] Nov Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Nov Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_11"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_11"], out["tmp_ft_employed_m_v2_11"], False
    )
    out["pt_employed_m_11"] = object_to_str_categorical(
        series=raw_data["kal1b011"],
        renaming={
            "[1] Nov Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Nov Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_11"] = object_to_str_categorical(
        raw_data["kal1n011"],
        renaming={1: "Minijob erwerbstätig"},
    )

    # Month 12 - Dez
    out["tmp_ft_employed_m_v1_12"] = object_to_str_categorical(
        series=raw_data["kal1a012_v1"],
        renaming={"[1] Ja": "Vollzeit erwerbstätig"},
    )
    out["tmp_ft_employed_m_v2_12"] = object_to_str_categorical(
        series=raw_data["kal1a012_v2"],
        renaming={
            "[1] Dez Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Dez Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["ft_employed_m_12"] = combine_first_and_make_categorical(
        out["tmp_ft_employed_m_v1_12"], out["tmp_ft_employed_m_v2_12"], False
    )
    out["pt_employed_m_12"] = object_to_str_categorical(
        series=raw_data["kal1b012"],
        renaming={
            "[1] Dez Teilzeit erwerbst.": "Teilzeit erwerbstätig",
            "[8] Dez Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["minijob_employed_m_12"] = object_to_str_categorical(
        raw_data["kal1n012"],
        renaming={1: "Minijob erwerbstätig"},
    )

    out["number_of_months_employed"] = _number_of_months_employed(out)
    out["employed_in_at_least_one_month"] = out["number_of_months_employed"] > 0

    return out
