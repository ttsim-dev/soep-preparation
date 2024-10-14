from soep_preparation.config import pd
from soep_preparation.initial_cleaning import month_mapping
from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    categorical_to_int_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the bioedu dataset."""
    out = pd.DataFrame()
    out["hh_id_orig"] = apply_lowest_int_dtype(raw["cid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["birth_month_from_bioedu"] = categorical_to_int_categorical(
        raw["gebmonat"],
        ordered=False,
        renaming=month_mapping.en,
    )
    return out
