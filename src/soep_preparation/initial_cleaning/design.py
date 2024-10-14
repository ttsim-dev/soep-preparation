from soep_preparation.config import pd
from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    str_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the biol dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["hh_random_group"] = apply_lowest_int_dtype(raw["rgroup"])
    out["hh_strat"] = apply_lowest_int_dtype(raw["strat"])

    out["hh_soep_sample"] = str_categorical(
        raw["hsample"],
        nr_identifiers=2,
    )

    return out
