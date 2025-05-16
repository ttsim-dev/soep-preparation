"""Functions to pre-process variables for a raw kidlong dataset."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    object_to_int,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the kidlong dataset."""
    out = pd.DataFrame()

    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["pointer_mother"] = object_to_int(raw_data["k_pmum"])
    out["betreuungskost_einrichtung"] = object_to_int(
        raw_data["kk_amtp_h"],
    )
    out["school_costs"] = object_to_int(raw_data["ks_amtp_h"])

    return out
