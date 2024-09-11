from soep_cleaning.config import pd
from soep_cleaning.initial_cleaning.helper import (
    float_categorical_to_float,
    int_categorical_to_int,
    str_categorical,
)
from soep_cleaning.utilities import apply_lowest_int_dtype


def design(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["hh_random_group"] = apply_lowest_int_dtype(raw["rgroup"])
    out["hh_strat"] = apply_lowest_int_dtype(raw["strat"])

    out["hh_soep_sample"] = str_categorical(
        raw["hsample"],
        nr_identifiers=2,
        ordered=False,
    )

    return out


def kidlong(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the kidlong dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = int_categorical_to_int(raw["hid"])
    out["p_id"] = int_categorical_to_int(raw["pid"])
    out["year"] = int_categorical_to_int(raw["syear"])
    out["pointer_mother"] = int_categorical_to_int(raw["k_pmum"])
    out["betreuungskost_einrichtung"] = float_categorical_to_float(
        raw["kk_amtp_h"],
    )
    out["school_costs"] = float_categorical_to_float(raw["ks_amtp_h"])

    return out
