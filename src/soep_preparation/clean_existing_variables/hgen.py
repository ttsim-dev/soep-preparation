"""Clean and convert SOEP hgen variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
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
    return out.where(rented_or_owned != "Eigentuemer", 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the hgen file.

    Args:
        raw_data (pd.DataFrame): The raw hgen data.

    Returns:
        pd.DataFrame: The processed hgen data.
    """
    out = pd.DataFrame()
    out["hh_id_orig"] = apply_lowest_int_dtype(raw_data["cid"])
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])

    out["survey_year"] = float_to_int(raw_data["syear"])
    out["building_year_hh_max"] = object_to_int(raw_data["hgcnstyrmax"])
    out["building_year_hh_min"] = object_to_int(raw_data["hgcnstyrmin"])
    out["heating_costs_m_hh"] = object_to_int(raw_data["hgheat"])
    out["einzugsjahr"] = object_to_int(raw_data["hgmoveyr"])
    out["rented_or_owned"] = object_to_str_categorical(raw_data["hgowner"])
    out["bruttokaltmiete_m_hh"] = _bruttokaltmiete_m_hh(
        raw_data["hgrent"],
        out["rented_or_owned"],
    )
    out["living_space_hh"] = object_to_int(raw_data["hgsize"])
    out["heizkosten_mi_reason"] = object_to_str_categorical(
        raw_data["hgheatinfo"],
        ordered=False,
    )
    out["hh_typ"] = object_to_str_categorical(
        raw_data["hgtyp1hh"],
        nr_identifiers=2,
    )
    out["hh_typ_2st"] = object_to_str_categorical(raw_data["hgtyp2hh"])
    return out
