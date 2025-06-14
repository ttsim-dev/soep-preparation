"""Clean and convert SOEP hwealth variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    fail_if_invalid_input,
    find_lowest_int_dtype,
    object_to_str_categorical,
)


def _hwealth_wide_to_long(data_wide: pd.DataFrame) -> pd.DataFrame:
    fail_if_invalid_input(data_wide, "pandas.core.frame.DataFrame")
    prev_wide_cols = [
        "hh_property_value_primary_residence",
        "hh_financial_assets_value",
        "hh_gross_overall_wealth",
        "hh_net_overall_wealth",
        "hh_vehicles_value",
        "hh_gross_overall_wealth_including_vehicles",
        "hh_net_overall_wealth_including_vehicles_and_student_loans",
    ]
    data_long = pd.wide_to_long(
        data_wide,
        stubnames=prev_wide_cols,
        i=["survey_year", "hh_id"],
        j="var",
        sep="_",
        suffix=r"\w+",
    ).reset_index()
    data_long_no_missings = data_long.dropna(subset=prev_wide_cols, how="all")
    return data_long_no_missings.astype(
        {
            "hh_property_value_primary_residence": "float64[pyarrow]",
            "hh_financial_assets_value": "float64[pyarrow]",
            "hh_gross_overall_wealth": "float64[pyarrow]",
            "hh_net_overall_wealth": "float64[pyarrow]",
            "hh_vehicles_value": (
                find_lowest_int_dtype(data_long_no_missings["hh_vehicles_value"])
            ),
            "hh_gross_overall_wealth_including_vehicles": "float64[pyarrow]",
            "hh_net_overall_wealth_including_vehicles_and_student_loans": "float64[pyarrow]",
        },
    )


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the hwealth file.

    Args:
        raw_data: The raw hwealth data.

    Returns:
        The processed hwealth data.
    """
    out = pd.DataFrame()
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])

    out["hh_property_value_primary_residence_a"] = apply_lowest_float_dtype(
        raw_data["p010ha"]
    )
    out["hh_property_value_primary_residence_b"] = apply_lowest_float_dtype(
        raw_data["p010hb"]
    )
    out["hh_property_value_primary_residence_c"] = apply_lowest_float_dtype(
        raw_data["p010hc"]
    )
    out["hh_property_value_primary_residence_d"] = apply_lowest_float_dtype(
        raw_data["p010hd"]
    )
    out["hh_property_value_primary_residence_e"] = apply_lowest_float_dtype(
        raw_data["p010he"]
    )

    out["hh_financial_assets_value_a"] = apply_lowest_float_dtype(raw_data["f010ha"])
    out["hh_financial_assets_value_b"] = apply_lowest_float_dtype(raw_data["f010hb"])
    out["hh_financial_assets_value_c"] = apply_lowest_float_dtype(raw_data["f010hc"])
    out["hh_financial_assets_value_d"] = apply_lowest_float_dtype(raw_data["f010hd"])
    out["hh_financial_assets_value_e"] = apply_lowest_float_dtype(raw_data["f010he"])

    out["hh_gross_overall_wealth_a"] = apply_lowest_float_dtype(raw_data["w010ha"])
    out["hh_gross_overall_wealth_b"] = apply_lowest_float_dtype(raw_data["w010hb"])
    out["hh_gross_overall_wealth_c"] = apply_lowest_float_dtype(raw_data["w010hc"])
    out["hh_gross_overall_wealth_d"] = apply_lowest_float_dtype(raw_data["w010hd"])
    out["hh_gross_overall_wealth_e"] = apply_lowest_float_dtype(raw_data["w010he"])

    out["hh_net_overall_wealth_a"] = apply_lowest_float_dtype(raw_data["w011ha"])
    out["hh_net_overall_wealth_b"] = apply_lowest_float_dtype(raw_data["w011hb"])
    out["hh_net_overall_wealth_c"] = apply_lowest_float_dtype(raw_data["w011hc"])
    out["hh_net_overall_wealth_d"] = apply_lowest_float_dtype(raw_data["w011hd"])
    out["hh_net_overall_wealth_e"] = apply_lowest_float_dtype(raw_data["w011he"])

    out["hh_vehicles_value_a"] = apply_lowest_int_dtype(raw_data["v010ha"])
    out["hh_vehicles_value_b"] = apply_lowest_int_dtype(raw_data["v010hb"])
    out["hh_vehicles_value_c"] = apply_lowest_int_dtype(raw_data["v010hc"])
    out["hh_vehicles_value_d"] = apply_lowest_int_dtype(raw_data["v010hd"])
    out["hh_vehicles_value_e"] = apply_lowest_int_dtype(raw_data["v010he"])

    out["hh_gross_overall_wealth_including_vehicles_a"] = apply_lowest_float_dtype(
        raw_data["n010ha"]
    )
    out["hh_gross_overall_wealth_including_vehicles_b"] = apply_lowest_float_dtype(
        raw_data["n010hb"]
    )
    out["hh_gross_overall_wealth_including_vehicles_c"] = apply_lowest_float_dtype(
        raw_data["n010hc"]
    )
    out["hh_gross_overall_wealth_including_vehicles_d"] = apply_lowest_float_dtype(
        raw_data["n010hd"]
    )
    out["hh_gross_overall_wealth_including_vehicles_e"] = apply_lowest_float_dtype(
        raw_data["n010he"]
    )

    out["hh_net_overall_wealth_including_vehicles_and_student_loans_a"] = (
        apply_lowest_float_dtype(raw_data["n011ha"])
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_b"] = (
        apply_lowest_float_dtype(raw_data["n011hb"])
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_c"] = (
        apply_lowest_float_dtype(raw_data["n011hc"])
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_d"] = (
        apply_lowest_float_dtype(raw_data["n011hd"])
    )
    out["hh_net_overall_wealth_including_vehicles_and_student_loans_e"] = (
        apply_lowest_float_dtype(raw_data["n011he"])
    )

    out["flag_netwealth"] = object_to_str_categorical(
        raw_data["n022h0"],
        renaming={
            "[0] No imputation": "No imputation",
            "[1] Edited": "Edited",
            "[2] Imputed": "Imputed",
        },
        ordered=True,
    )

    return _hwealth_wide_to_long(out)
