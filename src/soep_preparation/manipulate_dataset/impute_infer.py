import pandas as pd


def _determine_age_hh_head(data: pd.DataFrame) -> pd.Series:
    out = data.copy()
    out["age_hh_head"] = out.mask(out["hh_position"] == "Household head", "age")
    return out.groupby(["hh_id", "year"])["age_hh_head"].transform("first")


def ffill_and_bfill_by_group(
    data: pd.DataFrame,
    target_col: str,
    group_cols: list = ["p_id"],
) -> pd.Series:
    out = data.copy()
    return out.groupby(group_cols)[target_col].apply(lambda x: x.ffill().bfill())


def infer_birth_year(data: pd.DataFrame) -> pd.Series:
    out = data.copy()
    out["birth_year"] = out["birth_year"].fillna(data["survey_year"] - data["age"])
    out["birth_year"] = out.drop(out[out["birth_year"] > out["survey_year"]].index)
    return ffill_and_bfill_by_group(out, "birth_year")


def impute_hh_position(data: pd.DataFrame) -> pd.Series:
    out = data.copy()
    for var in ["hh_position"]:
        out[f"{var}_na_in_raw"] = out[var].isna()
        data[f"{var}_imp"] = ffill_and_bfill_by_group(data, var, ["p_id", "hh_id"])

    # Make sure that through this forward and backwordfilling, a household never has
    # more than one household head
    for var_label, label in [("hh_head", "Household head"), ("spouse", "Spouse")]:
        out[f"is_{var_label}"] = out["hh_position_imp"] == label
        out[f"anz_{var_label}"] = out.groupby(["hh_id", "year"])[
            f"is_{var_label}"
        ].transform("sum")

    out["hh_position_imp"] = out["hh_position_imp"].where(
        ~(
            (out["anz_hh_head"] == 2)
            & (out["anz_spouse"] == 0)
            & out["hh_position_na_in_raw"]
            & out["is_hh_head"]
        ),
        "Spouse",
    )

    out["hh_position_imp"] = out["hh_position_imp"].where(
        ~(
            (out["anz_hh_head"] == 2)
            & (out["anz_spouse"] > 0)
            & out["hh_position_na_in_raw"]
            & out["is_hh_head"]
        ),
        pd.NA,
    )

    out["hh_position_imp"] = out["hh_position_imp"].where(
        ~(
            (out["anz_spouse"] == 2)
            & (out["anz_hh_head"] == 0)
            & out["hh_position_na_in_raw"]
            & out["is_spouse"]
        ),
        "Household head",
    )

    out["hh_position_imp"] = out["hh_position_imp"].where(
        ~(
            (out["anz_spouse"] == 2)
            & (out["anz_hh_head"] > 0)
            & out["hh_position_na_in_raw"]
            & out["is_spouse"]
        ),
        pd.NA,
    )
    return out["hh_position_imp"]


def infer_age_from_relation_to_hh_head(data: pd.DataFrame) -> pd.Series:
    out = data.copy()
    out["age_hh_head"] = _determine_age_hh_head(out)
    out["age"] = out["age"].fillna(
        out.loc[out["hh_position"] == "Spouse", "age_hh_head"],
    )
    out["age"] = out["age"].fillna(
        (out.loc[out["hh_position"] == "Child", "age_hh_head"] - 30).clip(lower=0),
    )
    out["age"] = out["age"].fillna(
        out.loc[out["hh_position"] == "Parent", "age_hh_head"] + 30,
    )
    return out["age"]
