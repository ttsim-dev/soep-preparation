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
    _mortgage_without_property_share,
    _out_of_support_summary,
    _training_residual,
    observed_component_total,
    run_imputation,
)
from soep_preparation.wealth_imputation.market_indices import RESIDUAL_INDEX
from soep_preparation.wealth_imputation.simulate import ComponentDrawConfig

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
    """Household targets for the ten 2017 training households (owners + non-owners).

    Six households own gross property (300000). Four of them carry a mortgage (net
    250000, so mortgage = gross - net = 50000); the other two own outright (net 300000,
    mortgage 0). This gives the mortgage ownership model, fit on property owners only,
    both an owner and a non-owner class.
    """
    frame = pd.DataFrame({"hh_id": _TRAIN_IDS, "survey_year": [2017] * 10})
    frame["hh_net_property_value_primary_residence_a"] = (
        [250000.0] * 4 + [300000.0] * 2 + [0.0] * 4
    )
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


def test_mortgage_without_property_share_is_structurally_zero():
    """With coupled draws a mortgage can never land on a non-property-owner.

    The property/mortgage coupling zeros the mortgage for every non-property-owner, so
    the expected share of incoherent (mortgage, no property) balance sheets is exactly
    zero by construction, whatever the per-recipient probabilities.
    """
    mortgage_prob = np.array([0.5, 0.5])
    property_prob = np.array([1.0, 0.0])
    assert _mortgage_without_property_share(mortgage_prob, property_prob) == 0.0


def test_run_imputation_reports_zero_mortgage_without_property():
    """The coupled draw makes a mortgage without property structurally impossible."""
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    assert result.summary["mortgage_without_property_expected_share"] == 0.0


def test_mortgage_donor_pool_contains_only_property_owners():
    """The mortgage amount model draws only from donors who own owner-occupied property.

    A donor with a mortgage but no gross property would let a property owner inherit an
    incoherent mortgage amount; restricting the pool to property-owning donors keeps the
    coupled mortgage on the population that can hold one. The synthetic households with
    positive gross property and a positive mortgage are exactly the donor pool, so no
    non-property-owner donor enters it.
    """
    modules = _synthetic_modules()
    n_property_owning_mortgage_donors = _count_property_owning_mortgage_donors(modules)
    result = run_imputation(modules, n_draws=5, seed=0, k=3)
    assert result.summary["mortgage_donor_pool_size"] == (
        n_property_owning_mortgage_donors
    )


def _count_property_owning_mortgage_donors(
    modules: dict[str, pd.DataFrame],
) -> int:
    hwealth = modules["hwealth"]
    gross = pd.to_numeric(hwealth[_HW_PROPERTY_GROSS], errors="coerce")
    net = pd.to_numeric(
        hwealth["hh_net_property_value_primary_residence_a"], errors="coerce"
    )
    mortgage = (gross - net).clip(lower=0.0)
    return int(((gross > 0.0) & (mortgage > 0.0)).sum())


def test_run_imputation_marks_component_only_as_the_primary_total():
    """The headline `intervals` are the component-only total, not residual-inclusive."""
    result = run_imputation(_synthetic_modules(), n_draws=50, seed=0, k=3)
    assert result.summary["primary_total"] == "component_only"


def test_run_imputation_publishes_a_residual_inclusive_scenario():
    """The residual-inclusive total is a separate scenario, one row per household."""
    result = run_imputation(_synthetic_modules(), n_draws=50, seed=0, k=3)
    scenario = result.residual_inclusive_intervals
    assert scenario is not None
    assert len(scenario) == len(result.intervals)


def test_residual_inclusive_total_exceeds_the_component_only_total():
    """The positive synthetic residual lifts the scenario above the primary total."""
    result = run_imputation(_synthetic_modules(), n_draws=200, seed=0, k=3)
    scenario_intervals = result.residual_inclusive_intervals
    assert scenario_intervals is not None
    primary = result.intervals["point_estimate"].mean()
    scenario = scenario_intervals["point_estimate"].mean()
    assert scenario > primary


def test_run_imputation_summary_carries_the_residual_inclusive_distribution():
    """The summary exposes a separate across-draw distribution for the scenario."""
    result = run_imputation(_synthetic_modules(), n_draws=50, seed=0, k=3)
    distribution = result.summary["residual_inclusive_distribution_across_draws"]
    assert {"gini", "zero_share", "negative_share", "p50"} <= set(distribution)


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


def test_training_residual_restricts_to_the_given_household_subset():
    """A subset mask keeps only the selected rows before building the residual.

    The residual cross-fit needs to restrict the donor pool to one wave; passing a
    boolean subset over the training rows yields the residual on exactly those rows.
    """
    training = pd.DataFrame(
        {
            "survey_year": [2012, 2017],
            "hh_vehicles_value_a": [10000.0, 8000.0],
            _OFFICIAL_TOTAL_COLUMN: [30000.0, 18000.0],
        }
    )
    have_total, residual = _training_residual(
        training,
        [CanonicalComponent.VEHICLES],
        target_year=2022,
        subset=np.array([False, True]),
    )
    factor = RESIDUAL_INDEX[2022] / RESIDUAL_INDEX[2017]
    assert have_total["survey_year"].tolist() == [2017]
    np.testing.assert_allclose(residual, np.array([10000.0]) * factor)


def test_training_residual_subset_defaults_to_the_full_set():
    """Omitting the subset keeps every training row (the existing callers' behavior)."""
    training = pd.DataFrame(
        {
            "survey_year": [2012, 2012],
            "hh_vehicles_value_a": [10000.0, 8000.0],
            _OFFICIAL_TOTAL_COLUMN: [30000.0, 18000.0],
        }
    )
    have_total, _ = _training_residual(
        training, [CanonicalComponent.VEHICLES], target_year=2022
    )
    assert len(have_total) == 2


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


def _support_probe_config() -> ComponentDrawConfig:
    """Four recipients at scores 0, 1, 2, 10 against donors at 0 and 1."""
    return ComponentDrawConfig(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        ownership_prob=np.ones(4),
        ownership_share=np.ones(4),
        recipient_predicted=np.array([0.0, 1.0, 2.0, 10.0]),
        donor_predicted=np.array([0.0, 1.0]),
        donor_observed=np.array([100.0, 200.0]),
        scale=1.0,
        k=1,
    )


def test_out_of_support_summary_share_counts_recipients_beyond_the_caliper():
    """The out-of-support share is the fraction of recipients beyond the caliper.

    The four recipients (scores 0, 1, 2, 10) have nearest-donor distances 0, 0, 1, 9.
    With `caliper = 1.5` only the recipient at distance 9 exceeds it, so the share is
    1 / 4 = 0.25.
    """
    summary = _out_of_support_summary([_support_probe_config()], caliper=1.5)
    assert summary["financial_assets"]["out_of_support_share"] == 0.25


def test_out_of_support_summary_share_is_none_without_a_caliper():
    """With no caliper there is no threshold, so the share is None."""
    summary = _out_of_support_summary([_support_probe_config()], caliper=None)
    assert summary["financial_assets"]["out_of_support_share"] is None


def test_run_imputation_reports_out_of_support_distance_quantiles_per_component():
    """The summary reports nearest-donor distance quantiles for each used component."""
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    out_of_support = result.summary["out_of_support"]
    assert set(out_of_support) == set(result.summary["components_used"])
    financial = out_of_support["financial_assets"]
    assert {"median", "p90", "p99", "out_of_support_share"} <= set(financial)
    assert financial["median"] <= financial["p90"] <= financial["p99"]


def test_run_imputation_out_of_support_share_is_none_when_caliper_off():
    """Without a caliper there is no threshold, so the out-of-support share is None."""
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    financial = result.summary["out_of_support"]["financial_assets"]
    assert financial["out_of_support_share"] is None
    assert result.summary["uses_support_gate"] is False


def test_run_imputation_out_of_support_share_zero_when_caliper_admits_all_donors():
    """A caliper above every nearest-donor distance leaves nobody out of support."""
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3, caliper=1e9)
    financial = result.summary["out_of_support"]["financial_assets"]
    assert financial["out_of_support_share"] == 0.0
    assert result.summary["uses_support_gate"] is True


def test_run_imputation_caliper_off_leaves_draws_byte_for_byte_unchanged():
    """The default (caliper off) point estimates match a run that names caliper=None."""
    modules = _synthetic_modules()
    default_run = run_imputation(modules, n_draws=30, seed=0, k=3)
    explicit_none = run_imputation(modules, n_draws=30, seed=0, k=3, caliper=None)
    np.testing.assert_array_equal(
        default_run.intervals["point_estimate"].to_numpy(),
        explicit_none.intervals["point_estimate"].to_numpy(),
    )


def test_run_imputation_caliper_set_keeps_every_recipient_household():
    """A caliper flags out-of-support recipients but never drops a household."""
    modules = _synthetic_modules()
    gated = run_imputation(modules, n_draws=20, seed=0, k=3, caliper=1e9)
    assert set(gated.intervals["hh_id"]) == set(_RECIPIENT_IDS)


def test_run_imputation_reports_donor_wave_composition_per_component():
    """The summary reports drawn-donor wave shares from the training waves only.

    All synthetic training donors are from 2017, so any drawn donor is attributed to
    2017 and the per-component shares sum to one (an empty dict when no recipient was
    drawn as an owner in this fixture).
    """
    result = run_imputation(_synthetic_modules(), n_draws=20, seed=0, k=3)
    composition = result.summary["donor_wave_composition"]
    assert set(composition) == set(result.summary["components_used"])
    for shares in composition.values():
        assert set(shares) <= {2017}
        if shares:
            assert np.isclose(sum(shares.values()), 1.0, atol=1e-9)


def test_run_imputation_raises_without_recipients():
    """Imputation fails closed when no 2022 households are present."""
    modules = _synthetic_modules()
    for name in ("pequiv", "pgen", "ppathl", "hgen"):
        modules[name] = modules[name].query("survey_year != 2022")
    with pytest.raises(ValueError, match="recipients in prediction wave"):
        run_imputation(modules, n_draws=10, seed=0, k=3)
