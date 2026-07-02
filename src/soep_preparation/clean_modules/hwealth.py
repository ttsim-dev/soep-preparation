"""Clean and convert SOEP hwealth variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    convert_to_float,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the hwealth module.

    Args:
        raw_data: The raw hwealth data.

    Returns:
        The processed hwealth data. The postfixes a-e stand for different imputations.
    """
    out = pd.DataFrame()
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])

    out["hh_property_value_primary_residence_a"] = convert_to_float(raw_data["p010ha"])
    out["hh_property_value_primary_residence_b"] = convert_to_float(raw_data["p010hb"])
    out["hh_property_value_primary_residence_c"] = convert_to_float(raw_data["p010hc"])
    out["hh_property_value_primary_residence_d"] = convert_to_float(raw_data["p010hd"])
    out["hh_property_value_primary_residence_e"] = convert_to_float(raw_data["p010he"])

    out["hh_net_property_value_primary_residence_a"] = convert_to_float(
        raw_data["p011ha"]
    )
    out["hh_net_property_value_primary_residence_b"] = convert_to_float(
        raw_data["p011hb"]
    )
    out["hh_net_property_value_primary_residence_c"] = convert_to_float(
        raw_data["p011hc"]
    )
    out["hh_net_property_value_primary_residence_d"] = convert_to_float(
        raw_data["p011hd"]
    )
    out["hh_net_property_value_primary_residence_e"] = convert_to_float(
        raw_data["p011he"]
    )

    # due to non-response and to achieve comparable market values,
    # a maximum-likelihood Heckman selection regression model is used
    # the imputation is repeated leading to postfixes a-e
    # variation between the imputation results is marginal, averaging is sensible
    out["hh_financial_assets_value_a"] = convert_to_float(raw_data["f010ha"])
    out["hh_financial_assets_value_b"] = convert_to_float(raw_data["f010hb"])
    out["hh_financial_assets_value_c"] = convert_to_float(raw_data["f010hc"])
    out["hh_financial_assets_value_d"] = convert_to_float(raw_data["f010hd"])
    out["hh_financial_assets_value_e"] = convert_to_float(raw_data["f010he"])

    out["hh_gross_overall_wealth_a"] = convert_to_float(raw_data["w010ha"])
    out["hh_gross_overall_wealth_b"] = convert_to_float(raw_data["w010hb"])
    out["hh_gross_overall_wealth_c"] = convert_to_float(raw_data["w010hc"])
    out["hh_gross_overall_wealth_d"] = convert_to_float(raw_data["w010hd"])
    out["hh_gross_overall_wealth_e"] = convert_to_float(raw_data["w010he"])

    out["hh_net_overall_wealth_a"] = convert_to_float(raw_data["w011ha"])
    out["hh_net_overall_wealth_b"] = convert_to_float(raw_data["w011hb"])
    out["hh_net_overall_wealth_c"] = convert_to_float(raw_data["w011hc"])
    out["hh_net_overall_wealth_d"] = convert_to_float(raw_data["w011hd"])
    out["hh_net_overall_wealth_e"] = convert_to_float(raw_data["w011he"])

    out["hh_vehicles_value_a"] = convert_to_float(
        raw_data["v010ha"].replace({-8: pd.NA})
    )
    out["hh_vehicles_value_b"] = convert_to_float(
        raw_data["v010hb"].replace({-8: pd.NA})
    )
    out["hh_vehicles_value_c"] = convert_to_float(
        raw_data["v010hc"].replace({-8: pd.NA})
    )
    out["hh_vehicles_value_d"] = convert_to_float(
        raw_data["v010hd"].replace({-8: pd.NA})
    )
    out["hh_vehicles_value_e"] = convert_to_float(
        raw_data["v010he"].replace({-8: pd.NA})
    )

    out["hh_gross_overall_wealth_including_vehicles_a"] = convert_to_float(
        raw_data["n010ha"].replace({-8: pd.NA})
    )
    out["hh_gross_overall_wealth_including_vehicles_b"] = convert_to_float(
        raw_data["n010hb"].replace({-8: pd.NA})
    )
    out["hh_gross_overall_wealth_including_vehicles_c"] = convert_to_float(
        raw_data["n010hc"].replace({-8: pd.NA})
    )
    out["hh_gross_overall_wealth_including_vehicles_d"] = convert_to_float(
        raw_data["n010hd"].replace({-8: pd.NA})
    )
    out["hh_gross_overall_wealth_including_vehicles_e"] = convert_to_float(
        raw_data["n010he"].replace({-8: pd.NA})
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_a"] = (
        convert_to_float(raw_data["n011ha"].replace({-8: pd.NA}))
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_b"] = (
        convert_to_float(raw_data["n011hb"].replace({-8: pd.NA}))
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_c"] = (
        convert_to_float(raw_data["n011hc"].replace({-8: pd.NA}))
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_d"] = (
        convert_to_float(raw_data["n011hd"].replace({-8: pd.NA}))
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_e"] = (
        convert_to_float(raw_data["n011he"].replace({-8: pd.NA}))
    )

    out[
        "imputation_flag_hh_net_overall_wealth_including_vehicles_and_student_loans"
    ] = object_to_str_categorical(
        series=raw_data["n022h0"],
        renaming={
            "[0] No imputation": "No imputation",
            "[1] Edited": "Edited",
            "[2] Imputed": "Imputed",
        },
        ordered=True,
    )
    return out
