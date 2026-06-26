"""End-to-end behavior of the 2022 wealth imputation on synthetic modules."""

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation import impute
from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.impute import (
    _HW_FINANCIAL,
    _HW_MORTGAGE,
    _HW_PROPERTY_GROSS,
    _HW_VEHICLES,
    _OFFICIAL_TOTAL_COLUMN,
    _household_person_direct,
    _training_residual,
    observed_component_total,
    run_imputation,
)
from soep_preparation.wealth_imputation.market_indices import RESIDUAL_INDEX

_TRAIN_IDS = list(range(1, 11))  # ten 2017 training households (one person each)
_RECIPIENT_IDS = [101, 102, 103]  # three 2022 recipient households
_TRAIN_WAVE = 2017


def _feature_frame(p_ids: list[int], year: int) -> pd.DataFrame:
    n = len(p_ids)
    return pd.DataFrame(
        {
            "p_id": p_ids,
            "hh_id": p_ids,
            "survey_year": [year] * n,
            "age": np.linspace(35, 70, n),
            "gender": ["m", "f"] * (n // 2) + ["m"] * (n % 2),
            "number_of_persons_hh": [2] * n,
            "number_of_children_living_in_hh": [1] * n,
            "federal_state_of_residence": ["BY"] * n,
            "income_after_tax_y_hh": np.linspace(20000, 90000, n),
        }
    )


def _pequiv() -> pd.DataFrame:
    return pd.concat(
        [_feature_frame(_TRAIN_IDS, 2017), _feature_frame(_RECIPIENT_IDS, 2022)],
        ignore_index=True,
    )


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


def _owns_first(count: int, value: float, total: int) -> list[float]:
    return [value] * count + [0.0] * (total - count)


def _pwealth() -> pd.DataFrame:
    """Person wealth: lag sources plus the summed person-direct components.

    Rows at 2012 lag into the 2017 training features; 2017 rows for the training
    households supply the insurances/consumer-debt component targets (summed to the
    household); 2017 rows for the recipients lag into 2022.
    """
    blocks = [
        pd.DataFrame(  # lag source for the 2017 training features
            {"p_id": _TRAIN_IDS, "hh_id": _TRAIN_IDS, "survey_year": [2012] * 10}
        ),
        pd.DataFrame(  # 2017 person-direct component source for training households
            {"p_id": _TRAIN_IDS, "hh_id": _TRAIN_IDS, "survey_year": [2017] * 10}
        ),
        pd.DataFrame(  # lag source for the 2022 recipients
            {
                "p_id": _RECIPIENT_IDS,
                "hh_id": _RECIPIENT_IDS,
                "survey_year": [2017] * len(_RECIPIENT_IDS),
            }
        ),
    ]
    frame = pd.concat(blocks, ignore_index=True)
    n = len(frame)
    frame["property_value_primary_residence_a"] = np.linspace(0, 300000, n)
    frame["financial_assets_value_a"] = np.linspace(0, 60000, n)
    frame["vehicles_value_a"] = np.linspace(0, 15000, n)
    frame["private_insurances_value_a"] = np.linspace(0, 30000, n)
    frame["consumer_debt_value_a"] = np.linspace(0, 9000, n)
    # Give the 2017 training households a clear owner/non-owner split per component.
    is_train_2017 = (frame["survey_year"] == _TRAIN_WAVE) & frame["hh_id"].isin(
        _TRAIN_IDS
    )
    frame.loc[is_train_2017, "private_insurances_value_a"] = _owns_first(6, 20000.0, 10)
    frame.loc[is_train_2017, "consumer_debt_value_a"] = _owns_first(5, 8000.0, 10)
    return frame


def _hwealth() -> pd.DataFrame:
    """Household targets for the ten 2017 training households (owners + non-owners)."""
    frame = pd.DataFrame({"hh_id": _TRAIN_IDS, "survey_year": [2017] * 10})
    frame["hh_net_property_value_primary_residence_a"] = _owns_first(6, 250000.0, 10)
    # Gross property exceeds net for owners, so mortgage = gross - net = 50000.
    frame["hh_property_value_primary_residence_a"] = _owns_first(6, 300000.0, 10)
    frame["hh_financial_assets_value_a"] = _owns_first(7, 40000.0, 10)
    frame["hh_vehicles_value_a"] = _owns_first(8, 12000.0, 10)
    total = (
        frame["hh_net_property_value_primary_residence_a"]
        + frame["hh_financial_assets_value_a"]
        + frame["hh_vehicles_value_a"]
    )
    frame["hh_net_overall_wealth_including_vehicles_and_student_loans_a"] = (
        total + 10000.0  # residual = other RE/business/insurance net of debts
    )
    return frame


def _synthetic_modules() -> dict[str, pd.DataFrame]:
    return {
        "pequiv": _pequiv(),
        "pgen": _pgen(),
        "ppathl": _ppathl(),
        "hgen": _hgen(),
        "pwealth": _pwealth(),
        "hwealth": _hwealth(),
    }


def test_run_imputation_produces_one_interval_per_recipient_household():
    """Each 2022 household gets a finite, ordered net-wealth interval."""
    result = run_imputation(_synthetic_modules(), n_draws=50, seed=0, k=3)
    intervals = result.intervals
    assert set(intervals["hh_id"]) == set(_RECIPIENT_IDS)
    assert np.all(np.isfinite(intervals["point_estimate"].to_numpy()))
    assert np.all(intervals["lower"].to_numpy() <= intervals["upper"].to_numpy())


def test_run_imputation_fits_all_six_components():
    """Gross property, mortgage, financial, vehicles, pension, debt are all fit."""
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    expected = {
        "owner_occupied_property_gross",
        "owner_occupied_mortgage",
        "financial_assets",
        "vehicles",
        "private_pension",
        "consumer_debt",
    }
    assert set(result.summary["components_used"]) == expected
    assert result.summary["components_skipped"] == []
    assert result.summary["n_recipients"] == len(_RECIPIENT_IDS)


def test_run_imputation_summary_carries_the_draw_level_distribution():
    """The summary exposes the predictive distribution across draws, with bands."""
    result = run_imputation(_synthetic_modules(), n_draws=50, seed=0, k=3)
    distribution = result.summary["distribution_across_draws"]
    assert {"gini", "zero_share", "negative_share", "p50"} <= set(distribution)
    assert set(distribution["gini"]) == {"mean", "lower", "upper"}


def test_run_imputation_draws_the_residual_with_a_signed_pmm_model():
    """The residual to the official total is drawn by the signed PMM model."""
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    assert result.summary["residual_model"] == "signed_pmm"


def test_run_imputation_reports_the_donor_pool_mean_residual():
    """The summary reports the donor-pool mean residual, not a recipient contribution.

    The residual is drawn by recipient-score matching, which reweights donors, so the
    realised mean contribution differs from the donor-pool mean. The summary exposes the
    donor-pool mean under an honest name and no longer claims it is the contribution.
    """
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    assert "donor_pool_mean_residual" in result.summary
    assert "mean_residual" not in result.summary


def test_training_residual_drops_rows_with_a_missing_modelled_component():
    """A row missing any modelled component is dropped, not zero-filled, from training.

    Zero-filling a missing modelled component would push that component's full mass into
    the residual outcome, biasing the residual donor pool. Such rows are excluded so the
    residual is trained only on complete component vectors.
    """
    training = pd.DataFrame(
        {
            "survey_year": [2012, 2012],
            "hh_vehicles_value_a": [10000.0, np.nan],
            _OFFICIAL_TOTAL_COLUMN: [30000.0, 5000.0],
        }
    )
    have_total, residual = _training_residual(
        training, [CanonicalComponent.VEHICLES], target_year=2022
    )
    assert len(have_total) == 1
    factor = RESIDUAL_INDEX[2022] / RESIDUAL_INDEX[2012]
    np.testing.assert_allclose(residual, np.array([20000.0]) * factor)


def test_training_residual_deflates_to_the_target_wave():
    """The signed residual to the official total is brought into target-wave terms."""
    training = pd.DataFrame(
        {
            "survey_year": [2012, 2012],
            "hh_vehicles_value_a": [10000.0, 0.0],
            _OFFICIAL_TOTAL_COLUMN: [30000.0, 5000.0],
        }
    )
    _, residual = _training_residual(
        training, [CanonicalComponent.VEHICLES], target_year=2022
    )
    factor = RESIDUAL_INDEX[2022] / RESIDUAL_INDEX[2012]
    np.testing.assert_allclose(residual, np.array([20000.0, 5000.0]) * factor)


def test_observed_component_total_signs_the_modelled_components():
    """The observed total nets gross property and debts across the six components."""
    observed = observed_component_total(_synthetic_modules(), _TRAIN_WAVE)
    value = observed.loc[observed["hh_id"] == 1, "observed_total"].iloc[0]
    # 300000 gross - 50000 mortgage + 40000 financial + 12000 vehicles
    # + 20000 insurances - 8000 consumer debt.
    expected_total = 300000.0 - 50000.0 + 40000.0 + 12000.0 + 20000.0 - 8000.0
    np.testing.assert_allclose(value, expected_total)


def test_all_missing_target_is_excluded_from_observed_truth(
    monkeypatch: pytest.MonkeyPatch,
):
    """A household whose every modelled component is missing is not scored as zero.

    The backtest truth roster must hold only households with a complete observed
    component vector. A household with all components missing carries no observed
    wealth, so recoding it to zero would inflate the zero mass and the household count
    of the comparison target.
    """
    heads = pd.DataFrame(
        {
            "p_id": [1],
            "hh_id": [20],
            "survey_year": [2017],
            _HW_PROPERTY_GROSS: [np.nan],
            _HW_MORTGAGE: [np.nan],
            _HW_FINANCIAL: [np.nan],
            _HW_VEHICLES: [np.nan],
            "hh_private_insurances_value_a": [np.nan],
            "hh_consumer_debt_value_a": [np.nan],
        }
    )
    monkeypatch.setattr(impute, "_household_heads", lambda *_: heads)
    out = observed_component_total({}, 2017)
    assert 20 not in out["hh_id"].tolist()


def test_partially_observed_household_is_excluded_from_observed_truth(
    monkeypatch: pytest.MonkeyPatch,
):
    """A household missing any modelled component is dropped from the truth roster.

    The backtest truth must compare like with like: a partial component vector
    zero-filled to a "total" is a completed-component sum, not an observed total, so a
    household lacking even one component carries no comparable observed wealth and is
    excluded.
    """
    heads = pd.DataFrame(
        {
            "p_id": [1],
            "hh_id": [21],
            "survey_year": [2017],
            _HW_PROPERTY_GROSS: [100.0],
            _HW_MORTGAGE: [np.nan],
            _HW_FINANCIAL: [np.nan],
            _HW_VEHICLES: [np.nan],
            "hh_private_insurances_value_a": [np.nan],
            "hh_consumer_debt_value_a": [np.nan],
        }
    )
    monkeypatch.setattr(impute, "_household_heads", lambda *_: heads)
    out = observed_component_total({}, 2017)
    assert out.empty


def test_fully_observed_household_enters_observed_truth(
    monkeypatch: pytest.MonkeyPatch,
):
    """A household with all modelled components observed is kept with the signed sum."""
    heads = pd.DataFrame(
        {
            "p_id": [1],
            "hh_id": [22],
            "survey_year": [2017],
            _HW_PROPERTY_GROSS: [300000.0],
            _HW_MORTGAGE: [50000.0],
            _HW_FINANCIAL: [40000.0],
            _HW_VEHICLES: [12000.0],
            "hh_private_insurances_value_a": [20000.0],
            "hh_consumer_debt_value_a": [8000.0],
        }
    )
    monkeypatch.setattr(impute, "_household_heads", lambda *_: heads)
    out = observed_component_total({}, 2017)
    value = out.loc[out["hh_id"] == 22, "observed_total"].iloc[0]
    expected = 300000.0 - 50000.0 + 40000.0 + 12000.0 + 20000.0 - 8000.0
    np.testing.assert_allclose(value, expected)


def test_person_direct_all_missing_group_is_not_summed_to_zero():
    """A household whose members all lack a person-direct value yields NA, not 0.

    Pandas sums an all-missing group to zero, which would confound nonresponse with a
    genuine structural zero. With `min_count=1`, an all-missing group is `NA`.
    """
    pwealth = pd.DataFrame(
        {
            "p_id": [1, 2],
            "hh_id": [50, 50],
            "survey_year": [2017, 2017],
            "private_insurances_value_a": [np.nan, np.nan],
            "consumer_debt_value_a": [np.nan, np.nan],
        }
    )
    out = _household_person_direct(pwealth)
    assert pd.isna(out.loc[out["hh_id"] == 50, "hh_private_insurances_value_a"].iloc[0])


def test_run_imputation_raises_without_recipients():
    """Imputation fails closed when no 2022 households are present."""
    modules = _synthetic_modules()
    for name in ("pequiv", "pgen", "ppathl", "hgen"):
        modules[name] = modules[name].query("survey_year != 2022")
    with pytest.raises(ValueError, match="recipients in prediction wave"):
        run_imputation(modules, n_draws=10, seed=0, k=3)
