import pandas as pd

from soep_cleaning.initial_preprocessing.helper import (
    int_categorical,
    int_categorical_to_int,
    str_categorical,
)
from soep_cleaning.utilities import find_lowest_int_dtype


def design(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = raw_data["cid"].astype(
        find_lowest_int_dtype(raw_data["cid"]),
    )
    out["hh_random_group"] = raw_data["rgroup"].astype(
        find_lowest_int_dtype(raw_data["rgroup"]),
    )
    out["hh_strat"] = raw_data["strat"].astype(find_lowest_int_dtype(raw_data["strat"]))

    out["hh_soep_sample"] = str_categorical(
        raw_data["hsample"],
        no_identifiers=2,
        unordered=True,
    )

    return out


def kidlong(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the kidlong dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype(find_lowest_int_dtype(raw_data["hid"]))
    out["p_id"] = raw_data["pid"].astype(find_lowest_int_dtype(raw_data["pid"]))
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["pointer_mother"] = int_categorical(raw_data["k_pmum"], ordered=False)
    out["betreuungskost_einrichtung"] = int_categorical_to_int(
        raw_data["kk_amtp_h"],
    )
    out["school_costs"] = int_categorical_to_int(raw_data["ks_amtp_h"])

    return out
