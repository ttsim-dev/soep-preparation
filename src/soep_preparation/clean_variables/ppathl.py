"""Clean and convert SOEP ppathl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities import month_mapping
from soep_preparation.utilities.data_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
    create_dummy,
    object_to_int,
    object_to_int_categorical,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the ppathl data file.

    Args:
        raw_data: The raw ppathl data.

    Returns:
        The processed ppathl data.
    """
    out = pd.DataFrame()

    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"].replace([-3, -2], pd.NA))
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    # individual characteristics
    out["born_in_germany"] = object_to_str_categorical(raw_data["germborn"])
    out["country_of_birth"] = object_to_str_categorical(raw_data["corigin"])
    out["birth_month_ppathl"] = object_to_int_categorical(
        series=raw_data["gebmonat"],
        renaming=month_mapping.de,
        ordered=True,
    )
    out["1989_place_of_residence"] = object_to_str_categorical(raw_data["loc1989"])
    out["migration_background"] = object_to_str_categorical(raw_data["migback"])
    out["birth_bundesland"] = object_to_str_categorical(raw_data["birthregion"])

    # individual current information
    out["survey_status_current"] = object_to_str_categorical(raw_data["netto"])
    out["place_of_residence_current"] = object_to_str_categorical(
        series=raw_data["sampreg"],
        renaming={
            "[1] Westdeutschland, alte Bundeslaender": (
                "Westdeutschland (alte Bundesländer)"
            ),
            "[2] Ostdeutschland, neue Bundeslaender": (
                "Ostdeutschland (neue Bundesländer)"
            ),
        },
    )
    out["east_germany"] = create_dummy(
        series=out["place_of_residence_current"],
        value_for_comparison="Ostdeutschland (neue Bundesländer)",
        comparison_type="equal",
    )
    out["year_of_immigration"] = object_to_int(
        raw_data["immiyear"].replace(
            {
                -1: "[-1] Keine Angabe",
                -2: "[-2] Trifft nicht zu",
            },
        ),
    )
    out["sexual_orientation"] = object_to_str_categorical(raw_data["sexor"])
    out["partnership_status"] = object_to_str_categorical(raw_data["partner"])
    out["pointer_partner"] = object_to_int(
        raw_data["parid"].replace(
            {
                -1: "[-1] Keine Angabe",
                -2: "[-2] Trifft nicht zu",
            },
        ),
    )

    # individual staying probabilities and weighting factors
    out["individual_staying_probability"] = apply_smallest_float_dtype(
        raw_data["pbleib"]
    )
    out["individual_weighting_factor"] = apply_smallest_float_dtype(raw_data["phrf"])
    out["individual_weighting_factor_new_only"] = apply_smallest_float_dtype(
        raw_data["phrf0"]
    )
    out["individual_weighting_factor_without_new"] = apply_smallest_float_dtype(
        raw_data["phrf1"]
    )
    return out
