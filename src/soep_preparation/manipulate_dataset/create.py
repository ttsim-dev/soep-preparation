import pandas as pd

from soep_preparation.manipulate_dataset.impute_infer import (
    ffill_and_bfill_by_group,
    impute_hh_position,
    infer_age_from_relation_to_hh_head,
    infer_birth_year,
)


def create_personal_variables(data: pd.DataFrame) -> pd.DataFrame:
    """Create variables for the long dataset.

    Args:
        data: The merged dataset.

    Returns:
        The dataset with newly added variables.
    """
    out = data.copy()
    out["birth_year"] = infer_birth_year(
        out[["p_id", "survey_year", "age", "birth_year"]],
    )
    out["age_preliminary"] = out["age"].fillna(out["survey_year"] - out["birth_year"])
    out["gender"] = ffill_and_bfill_by_group(out, "gender")
    out["hh_position"] = out["hh_position"].fillna(impute_hh_position(out))
    out["age"] = infer_age_from_relation_to_hh_head(out)

    # Drop households with zero weight and missings in age, hh_position, gender
    out_hhs_with_weight = out.query("hh_gewicht > 0")
    out_complete = out_hhs_with_weight.dropna(subset=["age", "hh_position", "gender"])

    out_complete["m_elterngeld"] = 0
    return out_complete


def fill_pgen_for_children(data: pd.DataFrame) -> pd.DataFrame:
    """Fill missing values for children of pgen variables.

    Args:
        data: Dataset with missing values of children.

    Returns:
        Dataset with filled values.
    """
    out = data.copy()
    children_default = {
        "marital_status": "Ledig",
        "employment_status": "Nicht erwerbstätig",
        "laborforce_status": "NW-in education-training",
        "education_isced": "in school",
        "education_isced_alt": "in school",
        "occupation_status": "NE: in Ausbildung, \
            inkl. Weiterbildung, Berufsausbildung, Lehre",
        "public_service": 0,
        "exp_full_time": 0,
        "exp_part_time": 0,
        "exp_unempl": 0,
        "weekly_working_hours_actual": 0,
        "weekly_working_hours_contract": 0,
        "labor_earnings_prev": 0,
        "self_empl_earnings_prev": 0,
        "net_wage_m": 0,
        # That's an assumption
        "disability_degree": 0,
        "unempl_months_prev": 0,
        "in_education": True,
        "self_employed": False,
        "retired": False,
        "m_elterngeld": 0,
        "education": "primary_and_lower_secondary",
    }
    selection = out["age"] < 18
    for col, value in children_default.items():
        try:
            out.loc[selection, col] = out.loc[selection, col].fillna(value)
        except:
            out[col] = out[col].cat.add_categories(value)
            out.loc[selection, col] = out.loc[selection, col].fillna(value)

    return out
