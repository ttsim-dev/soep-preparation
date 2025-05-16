"""Functions to pre-process variables for a raw ppathl dataset."""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities.series_manipulator import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    object_to_int,
    object_to_int_categorical,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the ppathl dataset."""
    out = pd.DataFrame()

    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["current_east_west"] = object_to_str_categorical(
        raw_data["sampreg"],
        renaming={
            "[1] Westdeutschland, alte Bundeslaender": (
                "Westdeutschland (alte Bundeslaender)"
            ),
            "[2] Ostdeutschland, neue Bundeslaender": (
                "Ostdeutschland (neue Bundeslaender)"
            ),
        },
    )
    out["befragungsstatus"] = object_to_str_categorical(raw_data["netto"])
    out["year_immigration"] = object_to_int(
        raw_data["immiyear"].replace(
            {
                -1: "[-1] Keine Angabe",
                -2: "[-2] Trifft nicht zu",
            },
        ),
    )
    out["born_in_germany"] = object_to_str_categorical(raw_data["germborn"])
    out["country_of_birth"] = object_to_str_categorical(raw_data["corigin"])
    out["birth_month_from_ppathl"] = object_to_int_categorical(
        raw_data["gebmonat"],
        renaming=month_mapping.de,
        ordered=True,
    )
    out["east_west_1989"] = object_to_str_categorical(raw_data["loc1989"])
    out["migrationshintergrund"] = object_to_str_categorical(raw_data["migback"])
    out["sexual_orientation"] = object_to_str_categorical(raw_data["sexor"])
    out["birth_bundesland"] = object_to_str_categorical(raw_data["birthregion"])
    out["p_bleibe_wkeit"] = apply_lowest_float_dtype(raw_data["pbleib"])
    out["p_gewicht"] = apply_lowest_float_dtype(raw_data["phrf"])
    out["p_gewicht_nur_neue"] = apply_lowest_float_dtype(raw_data["phrf0"])
    out["p_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw_data["phrf1"])
    out["pointer_partner"] = object_to_int(
        raw_data["parid"].replace(
            {
                -1: "[-1] Keine Angabe",
                -2: "[-2] Trifft nicht zu",
            },
        ),
    )
    out["has_partner"] = object_to_str_categorical(raw_data["partner"])
    return out
