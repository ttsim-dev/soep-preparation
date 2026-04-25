"""Clean and convert SOEP pkal variables to appropriate data types."""

import numpy as np
import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    combine_first_and_make_categorical,
    object_to_bool_categorical,
    object_to_int,
    object_to_str_categorical,
    replace_not_applicable_answer,
)


def _mutterschaftsgeld_anzahl_monate(
    monate: pd.Series[pd.Categorical],
    bezug: pd.Series[pd.Categorical],
) -> pd.Series[int]:
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


def _combine_versions_employed_m(
    data_v1: pd.Series,
    renaming_v1: dict,
    data_v2: pd.Series,
    renaming_v2: dict,
) -> pd.Series:
    ft_employed_m_v1 = object_to_str_categorical(
        series=data_v1,
        renaming=renaming_v1,
    )
    ft_employed_m_v2 = object_to_str_categorical(
        series=data_v2,
        renaming=renaming_v2,
    )
    return combine_first_and_make_categorical(
        series_1=ft_employed_m_v1,
        series_2=ft_employed_m_v2,
        ordered=False,
    )


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the pkal module.

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
    out["unemployed_anzahl_monate"] = object_to_int(
        replace_not_applicable_answer(series=raw_data["kal1d02"], value=0)
    )
    out["early_retirement_pension_number_months"] = object_to_int(
        replace_not_applicable_answer(series=raw_data["kal1e02"], value=0)
    )
    out["unemployment_benefits_number_months"] = object_to_int(
        replace_not_applicable_answer(series=raw_data["kal2f02"], value=0)
    )
    # V41 removed kal2j01_h from distribution; use plain kal2j01.
    # Positive label changed from "[1] Ja" to "[1] genannt".
    out["bezog_mutterschaftsgeld_pkal"] = object_to_bool_categorical(
        series=raw_data["kal2j01"],
        renaming={"[2] Nein": False, "[1] genannt": True},
        ordered=True,
    )
    out["mutterschaftsgeld_anzahl_monate"] = _mutterschaftsgeld_anzahl_monate(
        monate=raw_data["kal2j02"],
        bezug=out["bezog_mutterschaftsgeld_pkal"],
    )

    # the first full time employment variables includes the timeframe 1984 until 1997,
    # the second the timeframe 1998 until 2022
    # individual employment status by month
    # Month 1 - Jan
    out["ft_employed_m_1"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a001_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a001_v2"],
        renaming_v2={
            "[1] Jan Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jan Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_1"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b001_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b001_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_1"] = object_to_str_categorical(
        series=raw_data["kal1n001"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 2 - Feb
    out["ft_employed_m_2"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a002_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a002_v2"],
        renaming_v2={
            "[1] Feb Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Feb Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_2"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b002_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b002_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_2"] = object_to_str_categorical(
        raw_data["kal1n002"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 3 - Mrz
    out["ft_employed_m_3"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a003_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a003_v2"],
        renaming_v2={
            "[1] Mrz Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Mrz Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_3"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b003_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b003_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_3"] = object_to_str_categorical(
        raw_data["kal1n003"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 4 - Apr
    out["ft_employed_m_4"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a004_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a004_v2"],
        renaming_v2={
            "[1] Apr Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Apr Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_4"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b004_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b004_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_4"] = object_to_str_categorical(
        raw_data["kal1n004"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 5 - Mai
    out["ft_employed_m_5"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a005_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a005_v2"],
        renaming_v2={
            "[1] Mai Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Mai Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_5"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b005_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b005_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_5"] = object_to_str_categorical(
        raw_data["kal1n005"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 6 - Jun
    out["ft_employed_m_6"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a006_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a006_v2"],
        renaming_v2={
            "[1] Jun Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jun Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_6"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b006_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b006_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_6"] = object_to_str_categorical(
        raw_data["kal1n006"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 7 - Jul
    out["ft_employed_m_7"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a007_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a007_v2"],
        renaming_v2={
            "[1] Jul Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Jul Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_7"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b007_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b007_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_7"] = object_to_str_categorical(
        raw_data["kal1n007"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 8 - Aug
    out["ft_employed_m_8"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a008_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a008_v2"],
        renaming_v2={
            "[1] Aug Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Aug Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_8"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b008_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b008_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_8"] = object_to_str_categorical(
        raw_data["kal1n008"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 9 - Sep
    out["ft_employed_m_9"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a009_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a009_v2"],
        renaming_v2={
            "[1] Sep Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Sep Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_9"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b009_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b009_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_9"] = object_to_str_categorical(
        raw_data["kal1n009"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 10 - Okt
    out["ft_employed_m_10"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a010_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a010_v2"],
        renaming_v2={
            "[1] Okt Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Okt Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_10"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b010_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b010_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_10"] = object_to_str_categorical(
        raw_data["kal1n010"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 11 - Nov
    out["ft_employed_m_11"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a011_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a011_v2"],
        renaming_v2={
            "[1] Nov Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Nov Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_11"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b011_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b011_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_11"] = object_to_str_categorical(
        raw_data["kal1n011"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    # Month 12 - Dez
    out["ft_employed_m_12"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1a012_v1"],
        renaming_v1={"[1] Ja": "Vollzeit erwerbstätig"},
        data_v2=raw_data["kal1a012_v2"],
        renaming_v2={
            "[1] Dez Vollzeit erwerbst.": "Vollzeit erwerbstätig",
            "[8] Dez Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )
    out["pt_employed_m_12"] = _combine_versions_employed_m(
        data_v1=raw_data["kal1b012_v1"],
        renaming_v1={"[1] genannt": "Teilzeit erwerbstätig"},
        data_v2=raw_data["kal1b012_v2"],
        renaming_v2={
            "[1] genannt": "Teilzeit erwerbstätig",
            "[8] Werkstatt für Behinderte": "Werkstatt für behinderte Menschen",
        },
    )
    out["minijob_employed_m_12"] = object_to_str_categorical(
        raw_data["kal1n012"],
        renaming={
            "[1] genannt": "Minijob erwerbstätig",
            "[8] Werkstatt fuer behinderte Menschen": "Werkstatt für behinderte Menschen",  # noqa: E501
        },
    )

    out["number_of_months_employed"] = _number_of_months_employed(out)
    out["employed_in_at_least_one_month"] = out["number_of_months_employed"] > 0

    return out
