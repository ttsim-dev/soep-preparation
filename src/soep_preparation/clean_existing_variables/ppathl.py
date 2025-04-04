"""Functions to pre-process variables for a raw ppathl dataset.

Functions:
- clean: Coordinates the pre-processing for the dataset.

Usage:
    Import this module and call clean to pre-process variables.
"""

import pandas as pd

from soep_preparation.clean_existing_variables import month_mapping
from soep_preparation.utilities import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    categorical_to_int_categorical,
    int_categorical_to_int,
    str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the ppathl dataset."""
    out = pd.DataFrame()
    out["hh_id"] = int_categorical_to_int(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["current_east_west"] = str_categorical(
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
    out["befragungsstatus"] = str_categorical(raw_data["netto"])
    out["year_immigration"] = int_categorical_to_int(raw_data["immiyear"])
    out["born_in_germany"] = str_categorical(raw_data["germborn"])
    out["country_of_birth"] = str_categorical(raw_data["corigin"])
    out["birth_month_from_ppathl"] = categorical_to_int_categorical(
        raw_data["gebmonat"],
        ordered=False,
        renaming=month_mapping.de,
    )
    out["east_west_1989"] = str_categorical(raw_data["loc1989"])
    out["migrationshintergrund"] = str_categorical(raw_data["migback"])
    out["sexual_orientation"] = str_categorical(raw_data["sexor"])
    out["birth_bundesland"] = str_categorical(raw_data["birthregion"])
    out["p_bleibe_wkeit"] = apply_lowest_float_dtype(raw_data["pbleib"])
    out["p_gewicht"] = apply_lowest_float_dtype(raw_data["phrf"])
    out["p_gewicht_nur_neue"] = apply_lowest_float_dtype(raw_data["phrf0"])
    out["p_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw_data["phrf1"])
    out["pointer_partner"] = int_categorical_to_int(raw_data["parid"])
    out["has_partner"] = str_categorical(raw_data["partner"])

    return out
