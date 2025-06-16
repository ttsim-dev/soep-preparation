"""Clean and convert SOEP hgen variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_int_dtype,
    float_to_int,
    object_to_float,
    object_to_int,
    object_to_str_categorical,
)


def _bruttokaltmiete_m_hh(
    miete: "pd.Series[pd.Categorical]",
    rented_or_owned: "pd.Series[pd.Categorical]",
) -> "pd.Series[float]":
    out = object_to_float(miete)
    return out.where(rented_or_owned != "Eigentümer", 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the hgen file.

    Args:
        raw_data: The raw hgen data.

    Returns:
        The processed hgen data.
    """
    out = pd.DataFrame()
    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["survey_year"] = float_to_int(raw_data["syear"])

    out["building_year_hh_max"] = object_to_int(raw_data["hgcnstyrmax"])
    out["building_year_hh_min"] = object_to_int(raw_data["hgcnstyrmin"])
    out["heating_costs_m_hh"] = object_to_int(raw_data["hgheat"])
    out["year_moved_in"] = object_to_int(raw_data["hgmoveyr"])
    out["rented_or_owned"] = object_to_str_categorical(
        series=raw_data["hgowner"],
        renaming={
            "[1] Eigentuemer": "Eigentümer",
            "[2] Hauptmieter": "Hauptmieter",
            "[3] Untermieter": "Untermieter",
            "[4] Mieter": "Mieter",
            "[5] Heimbewohner oder Gewarkschaftsunterkunft": "Heimbewohner oder Gewarkschaftsunterkunft",
        },
    )
    out["rent_minus_heating_costs_m_hh"] = _bruttokaltmiete_m_hh(
        raw_data["hgrent"],
        out["rented_or_owned"],
    )
    out["living_space_hh"] = object_to_int(raw_data["hgsize"])
    out["heating_costs_reason_missing"] = object_to_str_categorical(
        raw_data["hgheatinfo"],
        ordered=False,
    )
    out["hh_typ_one_digit"] = object_to_str_categorical(
        raw_data["hgtyp1hh"],
        nr_identifiers=2,
    )
    out["hh_typ_two_digits"] = object_to_str_categorical(raw_data["hgtyp2hh"])
    return out
