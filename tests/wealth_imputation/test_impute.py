"""End-to-end behavior of the 2022 wealth imputation on synthetic modules."""

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.impute import run_imputation

_TRAIN_IDS = list(range(1, 9))  # eight 2017 households (one person each)
_RECIPIENT_IDS = [101, 102, 103]  # three 2022 households


def _person_rows(p_ids: list[int], year: int) -> dict[str, list]:
    return {
        "p_id": p_ids,
        "hh_id": p_ids,
        "survey_year": [year] * len(p_ids),
    }


def _pequiv() -> pd.DataFrame:
    rows = {
        key: train + rec
        for (key, train), (_, rec) in zip(
            _person_rows(_TRAIN_IDS, 2017).items(),
            _person_rows(_RECIPIENT_IDS, 2022).items(),
            strict=True,
        )
    }
    frame = pd.DataFrame(rows)
    n = len(frame)
    frame["age"] = np.linspace(35, 70, n)
    frame["gender"] = ["m", "f"] * (n // 2) + ["m"] * (n % 2)
    frame["number_of_persons_hh"] = 2
    frame["number_of_children_living_in_hh"] = 1
    frame["federal_state_of_residence"] = "BY"
    frame["einkommen_nach_steuern_y_hh"] = np.linspace(20000, 90000, n)
    return frame


def _pgen() -> pd.DataFrame:
    frame = _pequiv()[["p_id", "hh_id", "survey_year"]].copy()
    n = len(frame)
    frame["education_isced"] = "tertiary"
    frame["employment_status"] = "employed"
    frame["marital_status"] = "married"
    frame["occupation_status"] = "white_collar"
    frame["self_employed"] = "no"
    frame["retired"] = "no"
    frame["gross_labor_income_previous_month_m"] = np.linspace(1500, 6000, n)
    frame["tenure"] = np.linspace(1, 30, n)
    return frame


def _ppathl() -> pd.DataFrame:
    frame = _pequiv()[["p_id", "hh_id", "survey_year"]].copy()
    frame["migration_background"] = "none"
    return frame


def _hgen() -> pd.DataFrame:
    frame = _pequiv()[["hh_id", "survey_year"]].copy()
    n = len(frame)
    frame["rented_or_owned"] = "owned"
    frame["living_space_hh"] = np.linspace(60, 160, n)
    frame["building_year_hh_min"] = np.linspace(1950, 2010, n)
    return frame


def _pwealth() -> pd.DataFrame:
    """Eight 2017 heads with a mix of owners and non-owners per component."""
    frame = pd.DataFrame(_person_rows(_TRAIN_IDS, 2017))
    owns_first = lambda count, value: [value] * count + [0.0] * (8 - count)  # noqa: E731
    frame["property_value_primary_residence_a"] = owns_first(4, 250000.0)
    frame["financial_assets_value_a"] = owns_first(6, 40000.0)
    frame["private_insurances_value_a"] = owns_first(5, 20000.0)
    frame["vehicles_value_a"] = owns_first(7, 12000.0)
    frame["consumer_debt_value_a"] = owns_first(3, 8000.0)
    # Official total exceeds the modelled net by a fixed residual (other RE/business).
    modelled = (
        frame["property_value_primary_residence_a"]
        + frame["financial_assets_value_a"]
        + frame["private_insurances_value_a"]
        + frame["vehicles_value_a"]
        - frame["consumer_debt_value_a"]
    )
    frame["net_overall_wealth_including_vehicles_and_student_loans_a"] = (
        modelled + 10000.0
    )
    return frame


def _synthetic_modules() -> dict[str, pd.DataFrame]:
    return {
        "pequiv": _pequiv(),
        "pgen": _pgen(),
        "ppathl": _ppathl(),
        "hgen": _hgen(),
        "pwealth": _pwealth(),
    }


def test_run_imputation_produces_one_interval_per_recipient_household():
    """Each 2022 household gets a finite, ordered net-wealth interval."""
    result = run_imputation(_synthetic_modules(), n_draws=50, seed=0, k=3)
    intervals = result.intervals
    assert set(intervals["hh_id"]) == set(_RECIPIENT_IDS)
    assert np.all(np.isfinite(intervals["point_estimate"].to_numpy()))
    assert np.all(intervals["lower"].to_numpy() <= intervals["upper"].to_numpy())


def test_run_imputation_summary_reports_used_components():
    """The run summary lists the components actually fit."""
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    assert result.summary["components_used"]
    assert result.summary["n_recipients"] == len(_RECIPIENT_IDS)


def test_run_imputation_raises_without_recipients():
    """Imputation fails closed when no 2022 households are present."""
    modules = _synthetic_modules()
    for name in ("pequiv", "pgen", "ppathl"):
        modules[name] = modules[name].query("survey_year != 2022")
    modules["hgen"] = modules["hgen"].query("survey_year != 2022")
    with pytest.raises(ValueError, match="2022 recipients"):
        run_imputation(modules, n_draws=10, seed=0, k=3)
