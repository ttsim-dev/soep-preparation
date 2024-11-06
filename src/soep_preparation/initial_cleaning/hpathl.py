import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    str_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hpathl dataset."""
    out = pd.DataFrame()
    out["hh_id"] = apply_lowest_int_dtype(raw["hid"])
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["hh_soep_sample_from_hpathl"] = str_categorical(
        raw["hsample"].cat.rename_categories(
            {
                "1994/5 Migration (1984-92/94 West)": "1994/5 Migration (1984-92/94, West)"
            }
        ),
        nr_identifiers=2,
    )
    out["hh_bleibe_wkeit"] = apply_lowest_float_dtype(raw["hbleib"])
    out["hh_gewicht"] = apply_lowest_float_dtype(raw["hhrf"])
    out["hh_gewicht_nur_neue"] = apply_lowest_float_dtype(raw["hhrf0"])
    out["hh_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw["hhrf0"])
    return out
