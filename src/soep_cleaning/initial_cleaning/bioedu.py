from soep_cleaning.config import pd
from soep_cleaning.initial_cleaning import month_mapping
from soep_cleaning.utilities import (
    apply_lowest_int_dtype,
    categorical_to_int_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the bioedu dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["birth_month_from_bioedu"] = categorical_to_int_categorical(
        raw["gebmonat"],
        ordered=False,
        renaming=month_mapping.en,
    )
    return out
