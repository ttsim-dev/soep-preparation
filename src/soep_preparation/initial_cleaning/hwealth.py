from soep_preparation.config import pd
from soep_preparation.utilities import (
    _fail_if_invalid_input,
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    find_lowest_int_dtype,
    str_categorical,
)


def _hwealth_wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    _fail_if_invalid_input(df, "pandas.core.frame.DataFrame")
    prev_wide_cols = [
        "wohnsitz_immobilienverm_hh",
        "finanzverm_hh",
        "bruttoverm_hh",
        "nettoverm_hh",
        "wert_fahrzeuge",
        "bruttoverm_inkl_fahrz_hh",
        "nettoverm_fahrz_kredit_hh",
    ]
    df = pd.wide_to_long(
        df,
        stubnames=prev_wide_cols,
        i=["year", "soep_hh_id"],
        j="var",
        sep="_",
        suffix=r"\w+",
    ).reset_index()
    df = df.dropna(subset=prev_wide_cols, how="all")
    return df.astype(
        {
            "wohnsitz_immobilienverm_hh": "float64[pyarrow]",
            "finanzverm_hh": "float64[pyarrow]",
            "bruttoverm_hh": "float64[pyarrow]",
            "nettoverm_hh": "float64[pyarrow]",
            "wert_fahrzeuge": find_lowest_int_dtype(df["wert_fahrzeuge"]),
            "bruttoverm_inkl_fahrz_hh": "float64[pyarrow]",
            "nettoverm_fahrz_kredit_hh": "float64[pyarrow]",
        },
    )


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the hwealth dataset."""
    out = pd.DataFrame()
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])

    out["wohnsitz_immobilienverm_hh_a"] = apply_lowest_float_dtype(raw["p010ha"])
    out["wohnsitz_immobilienverm_hh_b"] = apply_lowest_float_dtype(raw["p010hb"])
    out["wohnsitz_immobilienverm_hh_c"] = apply_lowest_float_dtype(raw["p010hc"])
    out["wohnsitz_immobilienverm_hh_d"] = apply_lowest_float_dtype(raw["p010hd"])
    out["wohnsitz_immobilienverm_hh_e"] = apply_lowest_float_dtype(raw["p010he"])

    out["finanzverm_hh_a"] = apply_lowest_float_dtype(raw["f010ha"])
    out["finanzverm_hh_b"] = apply_lowest_float_dtype(raw["f010hb"])
    out["finanzverm_hh_c"] = apply_lowest_float_dtype(raw["f010hc"])
    out["finanzverm_hh_d"] = apply_lowest_float_dtype(raw["f010hd"])
    out["finanzverm_hh_e"] = apply_lowest_float_dtype(raw["f010he"])

    out["bruttoverm_hh_a"] = apply_lowest_float_dtype(raw["w010ha"])
    out["bruttoverm_hh_b"] = apply_lowest_float_dtype(raw["w010hb"])
    out["bruttoverm_hh_c"] = apply_lowest_float_dtype(raw["w010hc"])
    out["bruttoverm_hh_d"] = apply_lowest_float_dtype(raw["w010hd"])
    out["bruttoverm_hh_e"] = apply_lowest_float_dtype(raw["w010he"])

    out["nettoverm_hh_a"] = apply_lowest_float_dtype(raw["w011ha"])
    out["nettoverm_hh_b"] = apply_lowest_float_dtype(raw["w011hb"])
    out["nettoverm_hh_c"] = apply_lowest_float_dtype(raw["w011hc"])
    out["nettoverm_hh_d"] = apply_lowest_float_dtype(raw["w011hd"])
    out["nettoverm_hh_e"] = apply_lowest_float_dtype(raw["w011he"])

    out["wert_fahrzeuge_a"] = apply_lowest_int_dtype(raw["v010ha"])
    out["wert_fahrzeuge_b"] = apply_lowest_int_dtype(raw["v010hb"])
    out["wert_fahrzeuge_c"] = apply_lowest_int_dtype(raw["v010hc"])
    out["wert_fahrzeuge_d"] = apply_lowest_int_dtype(raw["v010hd"])
    out["wert_fahrzeuge_e"] = apply_lowest_int_dtype(raw["v010he"])

    out["bruttoverm_inkl_fahrz_hh_a"] = apply_lowest_float_dtype(raw["n010ha"])
    out["bruttoverm_inkl_fahrz_hh_b"] = apply_lowest_float_dtype(raw["n010hb"])
    out["bruttoverm_inkl_fahrz_hh_c"] = apply_lowest_float_dtype(raw["n010hc"])
    out["bruttoverm_inkl_fahrz_hh_d"] = apply_lowest_float_dtype(raw["n010hd"])
    out["bruttoverm_inkl_fahrz_hh_e"] = apply_lowest_float_dtype(raw["n010he"])

    out["nettoverm_fahrz_kredit_hh_a"] = apply_lowest_float_dtype(raw["n011ha"])
    out["nettoverm_fahrz_kredit_hh_b"] = apply_lowest_float_dtype(raw["n011hb"])
    out["nettoverm_fahrz_kredit_hh_c"] = apply_lowest_float_dtype(raw["n011hc"])
    out["nettoverm_fahrz_kredit_hh_d"] = apply_lowest_float_dtype(raw["n011hd"])
    out["nettoverm_fahrz_kredit_hh_e"] = apply_lowest_float_dtype(raw["n011he"])

    out["flag_netwealth"] = str_categorical(raw["n022h0"])
    return _hwealth_wide_to_long(out)
