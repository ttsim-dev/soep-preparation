"""Clean and convert SOEP hpathl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the hpathl file.

    Args:
        raw_data: The raw hpathl data.

    Returns:
        The processed hpathl data.
    """
    out = pd.DataFrame()
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["hh_soep_sample_hpathl"] = object_to_str_categorical(
        raw_data["hsample"].replace(
            {
                "[4] D 1994/5 Migration (1984-92/94 West)": (
                    "[4] D 1994/5 Migration (1984-92/94, West)"
                ),
            },
        ),
        nr_identifiers=2,
    )
    out["hh_staying_probability"] = apply_smallest_float_dtype(raw_data["hbleib"])
    out["hh_weighting_factor"] = apply_smallest_float_dtype(raw_data["hhrf"])
    out["hh_weighting_factor_new_only"] = apply_smallest_float_dtype(raw_data["hhrf0"])
    out["hh_weighting_factor_without_new"] = apply_smallest_float_dtype(
        raw_data["hhrf1"]
    )
    return out
