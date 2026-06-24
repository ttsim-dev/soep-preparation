"""Clean and convert SOEP hgen variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    float_to_int,
    object_to_float,
    object_to_int,
    object_to_str_categorical,
    replace_not_applicable_answer,
)


def _bruttokaltmiete_m_hh(
    miete: pd.Series[pd.Categorical],
    rented_or_owned: pd.Series[pd.Categorical],
) -> pd.Series[float]:
    out = object_to_float(miete)
    return out.where(rented_or_owned != "Owner", 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the hgen module.

    Args:
        raw_data: The raw hgen data.

    Returns:
        The processed hgen data.
    """
    out = pd.DataFrame()
    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["survey_year"] = float_to_int(
        raw_data["syear"],
        code_negative_values_as_na=False,
    )

    out["building_year_hh_max"] = object_to_int(raw_data["hgcnstyrmax"])
    out["building_year_hh_min"] = object_to_int(raw_data["hgcnstyrmin"])
    out["heating_costs_m_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hgheat"], value=0)
    )
    out["year_moved_in"] = object_to_int(raw_data["hgmoveyr"])
    out["rented_or_owned"] = object_to_str_categorical(
        series=raw_data["hgowner"],
        renaming={
            "[1] Eigentuemer": "Owner",
            "[2] Hauptmieter": "Main tenant",
            "[3] Untermieter": "Subtenant",
            "[4] Mieter": "Tenant",
            "[5] Heimbewohner oder Gemeinschaftsunterkunft": (
                "Resident of a home or communal accommodation"
            ),
        },
    )
    out["rent_minus_heating_costs_m_hh"] = _bruttokaltmiete_m_hh(
        miete=raw_data["hgrent"],
        rented_or_owned=out["rented_or_owned"],
    )
    out["living_space_hh"] = object_to_float(raw_data["hgsize"])
    out["heating_costs_reason_missing"] = object_to_str_categorical(
        series=raw_data["hgheatinfo"],
        ordered=False,
    )
    out["hh_type_one_digit"] = object_to_str_categorical(
        series=raw_data["hgtyp1hh"],
        nr_identifiers=2,
    )
    out["hh_type_two_digits"] = object_to_str_categorical(raw_data["hgtyp2hh"])
    return out
