"""Clean and convert SOEP pwealth variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the pwealth module.

    Args:
        raw_data: The raw pwealth data.

    Returns:
        The processed pwealth data. The postfixes a-e stand for different imputations.
    """
    out = pd.DataFrame()
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])

    out["property_value_primary_residence_a"] = apply_smallest_float_dtype(
        raw_data["p0100a"]
    )
    out["property_value_primary_residence_b"] = apply_smallest_float_dtype(
        raw_data["p0100b"]
    )
    out["property_value_primary_residence_c"] = apply_smallest_float_dtype(
        raw_data["p0100c"]
    )
    out["property_value_primary_residence_d"] = apply_smallest_float_dtype(
        raw_data["p0100d"]
    )
    out["property_value_primary_residence_e"] = apply_smallest_float_dtype(
        raw_data["p0100e"]
    )

    out["net_property_value_primary_residence_a"] = apply_smallest_float_dtype(
        raw_data["p0110a"]
    )
    out["net_property_value_primary_residence_b"] = apply_smallest_float_dtype(
        raw_data["p0110b"]
    )
    out["net_property_value_primary_residence_c"] = apply_smallest_float_dtype(
        raw_data["p0110c"]
    )
    out["net_property_value_primary_residence_d"] = apply_smallest_float_dtype(
        raw_data["p0110d"]
    )
    out["net_property_value_primary_residence_e"] = apply_smallest_float_dtype(
        raw_data["p0110e"]
    )

    # due to non-response and to achieve comparable market values,
    # a maximum-likelihood Heckman selection regression model is used
    # the imputation is repeated leading to postfixes a-e
    # variation between the imputation results is marginal, averaging is sensible
    out["financial_assets_value_a"] = apply_smallest_float_dtype(raw_data["f0100a"])
    out["financial_assets_value_b"] = apply_smallest_float_dtype(raw_data["f0100b"])
    out["financial_assets_value_c"] = apply_smallest_float_dtype(raw_data["f0100c"])
    out["financial_assets_value_d"] = apply_smallest_float_dtype(raw_data["f0100d"])
    out["financial_assets_value_e"] = apply_smallest_float_dtype(raw_data["f0100e"])

    out["gross_overall_wealth_a"] = apply_smallest_float_dtype(raw_data["w0101a"])
    out["gross_overall_wealth_b"] = apply_smallest_float_dtype(raw_data["w0101b"])
    out["gross_overall_wealth_c"] = apply_smallest_float_dtype(raw_data["w0101c"])
    out["gross_overall_wealth_d"] = apply_smallest_float_dtype(raw_data["w0101d"])
    out["gross_overall_wealth_e"] = apply_smallest_float_dtype(raw_data["w0101e"])

    out["net_overall_wealth_a"] = apply_smallest_float_dtype(raw_data["w0111a"])
    out["net_overall_wealth_b"] = apply_smallest_float_dtype(raw_data["w0111b"])
    out["net_overall_wealth_c"] = apply_smallest_float_dtype(raw_data["w0111c"])
    out["net_overall_wealth_d"] = apply_smallest_float_dtype(raw_data["w0111d"])
    out["net_overall_wealth_e"] = apply_smallest_float_dtype(raw_data["w0111e"])

    out["vehicles_value_a"] = apply_smallest_float_dtype(
        raw_data["v0100a"].replace({-8: pd.NA})
    )
    out["vehicles_value_b"] = apply_smallest_float_dtype(
        raw_data["v0100b"].replace({-8: pd.NA})
    )
    out["vehicles_value_c"] = apply_smallest_float_dtype(
        raw_data["v0100c"].replace({-8: pd.NA})
    )
    out["vehicles_value_d"] = apply_smallest_float_dtype(
        raw_data["v0100d"].replace({-8: pd.NA})
    )
    out["vehicles_value_e"] = apply_smallest_float_dtype(
        raw_data["v0100e"].replace({-8: pd.NA})
    )

    out["gross_overall_wealth_including_vehicles_a"] = apply_smallest_float_dtype(
        raw_data["n0101a"].replace({-8: pd.NA})
    )
    out["gross_overall_wealth_including_vehicles_b"] = apply_smallest_float_dtype(
        raw_data["n0101b"].replace({-8: pd.NA})
    )
    out["gross_overall_wealth_including_vehicles_c"] = apply_smallest_float_dtype(
        raw_data["n0101c"].replace({-8: pd.NA})
    )
    out["gross_overall_wealth_including_vehicles_d"] = apply_smallest_float_dtype(
        raw_data["n0101d"].replace({-8: pd.NA})
    )
    out["gross_overall_wealth_including_vehicles_e"] = apply_smallest_float_dtype(
        raw_data["n0101e"].replace({-8: pd.NA})
    )
    out["net_overall_wealth_including_vehicles_and_student_loans_a"] = (
        apply_smallest_float_dtype(raw_data["n0111a"].replace({-8: pd.NA}))
    )
    out["net_overall_wealth_including_vehicles_and_student_loans_b"] = (
        apply_smallest_float_dtype(raw_data["n0111b"].replace({-8: pd.NA}))
    )
    out["net_overall_wealth_including_vehicles_and_student_loans_c"] = (
        apply_smallest_float_dtype(raw_data["n0111c"].replace({-8: pd.NA}))
    )
    out["net_overall_wealth_including_vehicles_and_student_loans_d"] = (
        apply_smallest_float_dtype(raw_data["n0111d"].replace({-8: pd.NA}))
    )
    out["net_overall_wealth_including_vehicles_and_student_loans_e"] = (
        apply_smallest_float_dtype(raw_data["n0111e"].replace({-8: pd.NA}))
    )

    out["imputation_flag_net_overall_wealth_including_vehicles_and_student_loans"] = (
        object_to_str_categorical(
            series=raw_data["n02220"],
            renaming={
                "[0] No imputation": "No imputation",
                "[1] Edited": "Edited",
                "[2] Imputed": "Imputed",
            },
            ordered=True,
        )
    )
    return out
