import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    float_categorical_to_float,
    float_categorical_to_int,
    int_to_int_categorical,
    str_categorical,
)


def _bruttokaltmiete_m_hh(
    miete: "pd.Series[pd.Categorical]",
    rented_or_owned: "pd.Series[pd.Categorical]",
) -> "pd.Series[float]":
    out = float_categorical_to_float(miete)
    return out.where(rented_or_owned != "Eigentuemer", 0)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hgen dataset."""
    out = pd.DataFrame()
    out["hh_id_orig"] = apply_lowest_int_dtype(raw["cid"])
    out["hh_id"] = apply_lowest_int_dtype(raw["hid"])

    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["building_year_hh_max"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgcnstyrmax"]),
    )
    out["building_year_hh_min"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgcnstyrmin"]),
    )
    out["heating_costs_m_hh"] = float_categorical_to_float(raw["hgheat"])
    out["einzugsjahr"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgmoveyr"]),
    )
    out["rented_or_owned"] = str_categorical(raw["hgowner"])
    out["bruttokaltmiete_m_hh"] = _bruttokaltmiete_m_hh(
        raw["hgrent"],
        out["rented_or_owned"],
    )
    out["living_space_hh"] = int_to_int_categorical(
        float_categorical_to_int(raw["hgsize"]),
    )
    out["heizkosten_mi_reason"] = str_categorical(
        raw["hgheatinfo"],
        ordered=False,
    )
    out["hh_typ"] = str_categorical(
        raw["hgtyp1hh"],
        nr_identifiers=2,
    )
    out["hh_typ_2st"] = str_categorical(raw["hgtyp2hh"])
    return out
