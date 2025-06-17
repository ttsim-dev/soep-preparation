"""Clean and convert SOEP hwealth variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_float_dtype,
    apply_smallest_int_dtype,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the hwealth file.

    Args:
        raw_data: The raw hwealth data.

    Returns:
        The processed hwealth data.
    """
    wide = pd.DataFrame()
    wide["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])
    wide["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])

    wide["tmp_hh_property_value_primary_residence_a"] = apply_smallest_float_dtype(
        raw_data["p010ha"]
    )
    wide["tmp_hh_property_value_primary_residence_b"] = apply_smallest_float_dtype(
        raw_data["p010hb"]
    )
    wide["tmp_hh_property_value_primary_residence_c"] = apply_smallest_float_dtype(
        raw_data["p010hc"]
    )
    wide["tmp_hh_property_value_primary_residence_d"] = apply_smallest_float_dtype(
        raw_data["p010hd"]
    )
    wide["tmp_hh_property_value_primary_residence_e"] = apply_smallest_float_dtype(
        raw_data["p010he"]
    )

    wide["tmp_hh_financial_assets_value_a"] = apply_smallest_float_dtype(
        raw_data["f010ha"]
    )
    wide["tmp_hh_financial_assets_value_b"] = apply_smallest_float_dtype(
        raw_data["f010hb"]
    )
    wide["tmp_hh_financial_assets_value_c"] = apply_smallest_float_dtype(
        raw_data["f010hc"]
    )
    wide["tmp_hh_financial_assets_value_d"] = apply_smallest_float_dtype(
        raw_data["f010hd"]
    )
    wide["tmp_hh_financial_assets_value_e"] = apply_smallest_float_dtype(
        raw_data["f010he"]
    )

    wide["tmp_hh_gross_overall_wealth_a"] = apply_smallest_float_dtype(
        raw_data["w010ha"]
    )
    wide["tmp_hh_gross_overall_wealth_b"] = apply_smallest_float_dtype(
        raw_data["w010hb"]
    )
    wide["tmp_hh_gross_overall_wealth_c"] = apply_smallest_float_dtype(
        raw_data["w010hc"]
    )
    wide["tmp_hh_gross_overall_wealth_d"] = apply_smallest_float_dtype(
        raw_data["w010hd"]
    )
    wide["tmp_hh_gross_overall_wealth_e"] = apply_smallest_float_dtype(
        raw_data["w010he"]
    )

    wide["tmp_hh_net_overall_wealth_a"] = apply_smallest_float_dtype(raw_data["w011ha"])
    wide["tmp_hh_net_overall_wealth_b"] = apply_smallest_float_dtype(raw_data["w011hb"])
    wide["tmp_hh_net_overall_wealth_c"] = apply_smallest_float_dtype(raw_data["w011hc"])
    wide["tmp_hh_net_overall_wealth_d"] = apply_smallest_float_dtype(raw_data["w011hd"])
    wide["tmp_hh_net_overall_wealth_e"] = apply_smallest_float_dtype(raw_data["w011he"])

    wide["tmp_hh_vehicles_value_a"] = apply_smallest_int_dtype(raw_data["v010ha"])
    wide["tmp_hh_vehicles_value_b"] = apply_smallest_int_dtype(raw_data["v010hb"])
    wide["tmp_hh_vehicles_value_c"] = apply_smallest_int_dtype(raw_data["v010hc"])
    wide["tmp_hh_vehicles_value_d"] = apply_smallest_int_dtype(raw_data["v010hd"])
    wide["tmp_hh_vehicles_value_e"] = apply_smallest_int_dtype(raw_data["v010he"])

    wide["tmp_hh_gross_overall_wealth_including_vehicles_a"] = (
        apply_smallest_float_dtype(raw_data["n010ha"])
    )
    wide["tmp_hh_gross_overall_wealth_including_vehicles_b"] = (
        apply_smallest_float_dtype(raw_data["n010hb"])
    )
    wide["tmp_hh_gross_overall_wealth_including_vehicles_c"] = (
        apply_smallest_float_dtype(raw_data["n010hc"])
    )
    wide["tmp_hh_gross_overall_wealth_including_vehicles_d"] = (
        apply_smallest_float_dtype(raw_data["n010hd"])
    )
    wide["tmp_hh_gross_overall_wealth_including_vehicles_e"] = (
        apply_smallest_float_dtype(raw_data["n010he"])
    )

    wide["tmp_hh_net_overall_wealth_including_vehicles_and_student_loans_a"] = (
        apply_smallest_float_dtype(raw_data["n011ha"])
    )
    wide["tmp_hh_net_overall_wealth_including_vehicles_and_student_loans_b"] = (
        apply_smallest_float_dtype(raw_data["n011hb"])
    )
    wide["tmp_hh_net_overall_wealth_including_vehicles_and_student_loans_c"] = (
        apply_smallest_float_dtype(raw_data["n011hc"])
    )
    wide["tmp_hh_net_overall_wealth_including_vehicles_and_student_loans_d"] = (
        apply_smallest_float_dtype(raw_data["n011hd"])
    )
    wide["tmp_hh_net_overall_wealth_including_vehicles_and_student_loans_e"] = (
        apply_smallest_float_dtype(raw_data["n011he"])
    )

    wide["tmp_imputation_flag_netwealth"] = object_to_str_categorical(
        raw_data["n022h0"],
        renaming={
            "[0] No imputation": "No imputation",
            "[1] Edited": "Edited",
            "[2] Imputed": "Imputed",
        },
        ordered=True,
    )

    # Transforming the wide data to long format
    prev_wide_variables = [
        "tmp_hh_property_value_primary_residence",
        "tmp_hh_financial_assets_value",
        "tmp_hh_gross_overall_wealth",
        "tmp_hh_net_overall_wealth",
        "tmp_hh_vehicles_value",
        "tmp_hh_gross_overall_wealth_including_vehicles",
        "tmp_hh_net_overall_wealth_including_vehicles_and_student_loans",
    ]
    # We remove rows that are completely empty that is, all non-existent
    # assets in each class of the potential enumerations are dropped
    # (households with 2 assets per class only take two rows in the final data)
    tmp_long = (
        pd.wide_to_long(
            wide,
            stubnames=prev_wide_variables,
            i=["survey_year", "hh_id"],
            j="tmp_asset_group",
            sep="_",
            suffix=r"\w+",
        )
        .dropna(subset=prev_wide_variables, how="all")
        .reset_index()
    )

    long = pd.DataFrame()

    long["survey_year"] = tmp_long["survey_year"]
    long["hh_id"] = tmp_long["hh_id"]
    long["imputation_flag_netwealth"] = tmp_long["tmp_imputation_flag_netwealth"]
    long["asset_class"] = tmp_long["tmp_asset_group"]
    long["hh_property_value_primary_residence"] = tmp_long[
        "tmp_hh_property_value_primary_residence"
    ]
    long["hh_financial_assets_value"] = tmp_long["tmp_hh_financial_assets_value"]
    long["hh_gross_overall_wealth"] = tmp_long["tmp_hh_gross_overall_wealth"]
    long["hh_net_overall_wealth"] = tmp_long["tmp_hh_net_overall_wealth"]
    long["hh_vehicles_value"] = tmp_long["tmp_hh_vehicles_value"]
    long["hh_gross_overall_wealth_including_vehicles"] = tmp_long[
        "tmp_hh_gross_overall_wealth_including_vehicles"
    ]
    long["hh_net_overall_wealth_including_vehicles_and_student_loans"] = tmp_long[
        "tmp_hh_net_overall_wealth_including_vehicles_and_student_loans"
    ]

    return long
