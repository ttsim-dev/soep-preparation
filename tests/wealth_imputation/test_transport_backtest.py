"""Leave-one-wave-out temporal-transport backtest behaviour."""

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.transport_backtest import (
    _transport_stability,
    official_rank_comparison,
    official_total_truth,
    run_transport_backtest,
)


def _hwealth_two_waves() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hh_id": [1, 2, 3, 10, 20],
            "survey_year": [2017, 2017, 2017, 2012, 2012],
            "hh_net_overall_wealth_a": [100.0, 200.0, np.nan, 50.0, 70.0],
        }
    )


def test_official_total_truth_keeps_one_finite_w011h_row_per_household_in_the_wave():
    """The truth roster is the finite official `w011h` totals for the requested wave."""
    truth = official_total_truth({"hwealth": _hwealth_two_waves()}, 2017)
    assert truth["official_total"].tolist() == [100.0, 200.0]


def test_official_total_truth_excludes_other_waves():
    """Households from a different wave do not enter the wave's truth roster."""
    truth = official_total_truth({"hwealth": _hwealth_two_waves()}, 2017)
    assert truth["survey_year"].unique().tolist() == [2017]


def _intervals(hh_ids: list[int], point_estimates: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hh_id": hh_ids,
            "survey_year": [2017] * len(hh_ids),
            "point_estimate": point_estimates,
        }
    )


def _truth(hh_ids: list[int], totals: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "hh_id": hh_ids,
            "survey_year": [2017] * len(hh_ids),
            "official_total": totals,
        }
    )


def test_official_rank_comparison_perfect_rank_has_unit_correlation():
    """When the imputed order matches the official order, rank correlation is 1."""
    comparison = official_rank_comparison(
        _intervals([1, 2, 3, 4], [10.0, 20.0, 30.0, 40.0]),
        _truth([1, 2, 3, 4], [1.0, 2.0, 3.0, 4.0]),
        n_groups=2,
    )
    assert comparison["rank_correlation"] == pytest.approx(1.0)


def test_official_rank_comparison_reversed_rank_is_minus_one():
    """When the imputed order reverses the official order, rank correlation is -1."""
    comparison = official_rank_comparison(
        _intervals([1, 2, 3, 4], [40.0, 30.0, 20.0, 10.0]),
        _truth([1, 2, 3, 4], [1.0, 2.0, 3.0, 4.0]),
        n_groups=2,
    )
    assert comparison["rank_correlation"] == pytest.approx(-1.0)


def test_official_rank_comparison_is_empty_safe():
    """No overlapping households yields n = 0 and NaN metrics, not a crash."""
    comparison = official_rank_comparison(
        _intervals([1, 2], [10.0, 20.0]), _truth([7, 8], [1.0, 2.0]), n_groups=2
    )
    assert comparison["n"] == 0
    assert np.isnan(comparison["rank_correlation"])


def _stability_input() -> dict:
    return {
        "2012": {
            "vs_official_w011h": {
                "rank_correlation": 0.6,
                "exact_quintile_accuracy": 0.4,
                "mean_abs_quintile_error": 0.9,
            }
        },
        "2017": {
            "vs_official_w011h": {
                "rank_correlation": 0.8,
                "exact_quintile_accuracy": 0.5,
                "mean_abs_quintile_error": 0.7,
            }
        },
    }


def test_transport_stability_reports_the_spread_of_a_rank_metric_across_waves():
    """Stability spread is the max-minus-min of a rank metric over held-out waves."""
    stability = _transport_stability(_stability_input())
    assert stability["rank_correlation"]["spread"] == pytest.approx(0.2)


def test_transport_stability_counts_the_held_out_waves():
    """The stability summary records how many waves were held out and scored."""
    stability = _transport_stability(_stability_input())
    assert stability["n_waves"] == 2


_HH_IDS = list(range(1, 13))
_WEALTH_WAVES = (2012, 2017)
_VEHICLE_WAVE = 2017  # vehicles are observed only from 2017, as in the real data


def _owns_first(count: int, value: float, total: int) -> list[float]:
    return [value] * count + [0.0] * (total - count)


def _features(year: int) -> pd.DataFrame:
    n = len(_HH_IDS)
    return pd.DataFrame(
        {
            "p_id": _HH_IDS,
            "hh_id": _HH_IDS,
            "survey_year": [year] * n,
            "age": np.linspace(35, 70, n),
            "gender": ["m", "f"] * (n // 2),
            "number_of_persons_hh": [2] * n,
            "number_of_children_living_in_hh": [1] * n,
            "federal_state_of_residence": ["BY"] * n,
            "income_after_tax_y_hh": np.linspace(20000, 90000, n),
        }
    )


def _two_wave_pequiv() -> pd.DataFrame:
    return pd.concat([_features(year) for year in _WEALTH_WAVES], ignore_index=True)


def _two_wave_pwealth() -> pd.DataFrame:
    # Component-target rows at each wealth wave plus prior-wave lag sources (year - 5).
    years = sorted({*_WEALTH_WAVES, *(year - 5 for year in _WEALTH_WAVES)})
    frame = pd.concat(
        [
            pd.DataFrame(
                {"p_id": _HH_IDS, "hh_id": _HH_IDS, "survey_year": [year] * 12}
            )
            for year in years
        ],
        ignore_index=True,
    )
    n = len(frame)
    frame["property_value_primary_residence_a"] = np.linspace(0, 300000, n)
    frame["financial_assets_value_a"] = np.linspace(0, 60000, n)
    frame["vehicles_value_a"] = np.linspace(0, 15000, n)
    frame["private_insurances_value_a"] = np.linspace(0, 30000, n)
    frame["consumer_debt_value_a"] = np.linspace(0, 9000, n)
    for wave in _WEALTH_WAVES:
        in_wave = frame["survey_year"] == wave
        frame.loc[in_wave, "private_insurances_value_a"] = _owns_first(7, 20000.0, 12)
        frame.loc[in_wave, "consumer_debt_value_a"] = _owns_first(6, 8000.0, 12)
    return frame


def _two_wave_hwealth() -> pd.DataFrame:
    blocks = []
    for wave in _WEALTH_WAVES:
        frame = pd.DataFrame({"hh_id": _HH_IDS, "survey_year": [wave] * 12})
        frame["hh_net_property_value_primary_residence_a"] = (
            [250000.0] * 5 + [300000.0] * 2 + [0.0] * 5
        )
        frame["hh_property_value_primary_residence_a"] = _owns_first(7, 300000.0, 12)
        frame["hh_financial_assets_value_a"] = _owns_first(8, 40000.0, 12)
        # Vehicles are observed only from 2017: earlier waves carry no vehicle column,
        # so a completed-component truth cannot exist there -- this is why the transport
        # backtest scores against the all-wave official total instead.
        vehicles = (
            _owns_first(9, 12000.0, 12) if wave == _VEHICLE_WAVE else [np.nan] * 12
        )
        frame["hh_vehicles_value_a"] = vehicles
        net = (
            frame["hh_net_property_value_primary_residence_a"]
            + frame["hh_financial_assets_value_a"]
        )
        frame["hh_net_overall_wealth_a"] = net + 10000.0
        frame["hh_net_overall_wealth_including_vehicles_and_student_loans_a"] = (
            net + 10000.0 + (net * 0.0 + 12000.0 if wave == _VEHICLE_WAVE else 0.0)
        )
        blocks.append(frame)
    return pd.concat(blocks, ignore_index=True)


def _two_wave_modules() -> dict[str, pd.DataFrame]:
    pequiv = _two_wave_pequiv()
    heads = pequiv[["p_id", "hh_id", "survey_year"]].copy()
    pgen = heads.copy()
    pgen["education_isced"] = "tertiary"
    pgen["employment_status"] = "employed"
    pgen["marital_status"] = "married"
    pgen["occupation_status"] = "white_collar"
    pgen["self_employed"] = "no"
    pgen["retired"] = "no"
    pgen["gross_labor_income_previous_month_m"] = np.linspace(1500, 6000, len(pgen))
    pgen["tenure"] = np.linspace(1, 30, len(pgen))
    ppathl = heads.copy()
    ppathl["migration_background"] = "none"
    hgen = pequiv[["hh_id", "survey_year"]].copy()
    hgen["rented_or_owned"] = "owned"
    hgen["living_space_hh"] = np.linspace(60, 160, len(hgen))
    hgen["building_year_hh_min"] = np.linspace(1950, 2010, len(hgen))
    return {
        "pequiv": pequiv,
        "pgen": pgen,
        "ppathl": ppathl,
        "hgen": hgen,
        "pwealth": _two_wave_pwealth(),
        "hwealth": _two_wave_hwealth(),
    }


def _run_two_wave() -> dict:
    return run_transport_backtest(
        _two_wave_modules(),
        holdout_waves=_WEALTH_WAVES,
        all_waves=_WEALTH_WAVES,
        n_draws=20,
        seed=0,
        k=3,
        n_groups=2,
    )


def test_run_transport_backtest_scores_each_held_out_wave():
    """Each held-out wave gets its own scorecard, even one lacking a component."""
    report = _run_two_wave()
    assert set(report["per_holdout_wave"]) == {"2012", "2017"}


def test_run_transport_backtest_trains_on_the_other_waves():
    """Holding out 2017 trains on the remaining wealth wave (2012), and vice versa."""
    report = _run_two_wave()
    assert report["per_holdout_wave"]["2017"]["training_waves"] == [2012]


def test_run_transport_backtest_scores_against_the_official_total_on_rank():
    """Each held-out wave carries a rank comparison to the official `w011h` total."""
    report = _run_two_wave()
    assert "rank_correlation" in report["per_holdout_wave"]["2012"]["vs_official_w011h"]


def test_run_transport_backtest_summarises_stability_over_the_held_out_waves():
    """The stability summary spans exactly the held-out waves scored."""
    report = _run_two_wave()
    assert report["transport_stability"]["n_waves"] == 2
