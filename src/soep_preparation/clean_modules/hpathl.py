"""Clean and convert SOEP hpathl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities import sample_mapping
from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    convert_to_float,
    object_to_str_categorical,
    translate_categories,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the hpathl module.

    Args:
        raw_data: The raw hpathl data.

    Returns:
        The processed hpathl data.
    """
    out = pd.DataFrame()
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["hh_soep_sample_hpathl"] = translate_categories(
        object_to_str_categorical(
            series=raw_data["hsample"].replace(
                {
                    "[4] D 1994/5 Migration (1984-92/94 West)": (
                        "[4] D 1994/5 Migration (1984-92/94, West)"
                    ),
                },
            ),
            nr_identifiers=2,
        ),
        sample_mapping.to_english,
    )
    out["hh_staying_probability"] = convert_to_float(raw_data["hbleib"])
    out["hh_weighting_factor"] = convert_to_float(raw_data["hhrf"])
    out["hh_weighting_factor_new_only"] = convert_to_float(raw_data["hhrf0"])
    out["hh_weighting_factor_without_new"] = convert_to_float(raw_data["hhrf1"])
    return out
