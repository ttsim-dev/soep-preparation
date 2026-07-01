"""Tests for building a GETTSIM-ready dataset from a SOEP final dataset."""

import pandas as pd
import pytest

from soep_preparation.gettsim_inputs.build import (
    build_gettsim_inputs,
    create_mapping_report,
    fail_if_required_inputs_missing,
    resolve_available_mapping,
)
from soep_preparation.gettsim_inputs.mapping import SOEP_TO_GETTSIM


def _synthetic_final_dataset() -> pd.DataFrame:
    """A SOEP final dataset with a handful of mappable variables."""
    return pd.DataFrame(
        {
            "p_id": pd.Series([1, 2], dtype="int32[pyarrow]"),
            "hh_id": pd.Series([10, 10], dtype="int32[pyarrow]"),
            "survey_year": pd.Series([2023, 2023], dtype="int16[pyarrow]"),
            "living_space_hh": pd.Series([80.0, 80.0], dtype="float[pyarrow]"),
            "heating_costs_m_hh": pd.Series([120.0, 120.0], dtype="float[pyarrow]"),
            "first_pension_receipt_year": pd.Series(
                [2020, pd.NA], dtype="int16[pyarrow]"
            ),
            "actual_working_hours_w": pd.Series([40.0, 0.0], dtype="float[pyarrow]"),
            "life_satisfaction_low_to_high": pd.Series([7, 8], dtype="int8[pyarrow]"),
        }
    )


def test_build_renames_living_space_to_gettsim_qname() -> None:
    """The living-space column is renamed to `wohnen__wohnfläche_hh`."""
    out = build_gettsim_inputs(_synthetic_final_dataset())
    assert "wohnen__wohnfläche_hh" in out.columns


def test_build_keeps_index_variables() -> None:
    """Index variables are carried through verbatim."""
    out = build_gettsim_inputs(_synthetic_final_dataset())
    assert list(out[["p_id", "hh_id", "survey_year"]].columns) == [
        "p_id",
        "hh_id",
        "survey_year",
    ]


def test_build_preserves_values_under_renamed_column() -> None:
    """Renaming preserves the underlying values."""
    out = build_gettsim_inputs(_synthetic_final_dataset())
    expected = pd.Series([80.0, 80.0], dtype="float[pyarrow]")
    pd.testing.assert_series_equal(
        out["wohnen__wohnfläche_hh"].reset_index(drop=True),
        expected,
        check_names=False,
    )


def test_build_drops_unmapped_soep_columns() -> None:
    """SOEP columns with no GETTSIM target are not carried into the output."""
    out = build_gettsim_inputs(_synthetic_final_dataset())
    assert "life_satisfaction_low_to_high" not in out.columns


def test_build_skips_inputs_whose_soep_column_is_absent() -> None:
    """A mapped input is omitted when its SOEP column is absent from the data."""
    minimal = pd.DataFrame(
        {
            "p_id": pd.Series([1], dtype="int32[pyarrow]"),
            "living_space_hh": pd.Series([80.0], dtype="float[pyarrow]"),
        }
    )
    out = build_gettsim_inputs(minimal)
    assert "wohnen__heizkosten_m_hh" not in out.columns


def test_resolve_available_mapping_matches_present_columns() -> None:
    """Resolution keeps only mapped inputs whose SOEP column is present."""
    resolved = resolve_available_mapping(
        mapping=SOEP_TO_GETTSIM,
        available_columns=pd.Index(["living_space_hh", "heating_costs_m_hh"]),
    )
    assert resolved == {
        "wohnen__wohnfläche_hh": "living_space_hh",
        "wohnen__heizkosten_m_hh": "heating_costs_m_hh",
    }


def test_validator_flags_unmapped_required_input() -> None:
    """An unmapped required input (`lohnsteuer__steuerklasse`) is flagged."""
    with pytest.raises(ValueError, match="lohnsteuer__steuerklasse"):
        fail_if_required_inputs_missing(
            required_inputs=frozenset({"lohnsteuer__steuerklasse"}),
            available_columns=pd.Index(["living_space_hh"]),
        )


def test_validator_flags_mapped_input_with_absent_column() -> None:
    """A mapped input whose SOEP column is absent is flagged."""
    with pytest.raises(ValueError, match="wohnen__wohnfläche_hh"):
        fail_if_required_inputs_missing(
            required_inputs=frozenset({"wohnen__wohnfläche_hh"}),
            available_columns=pd.Index(["heating_costs_m_hh"]),
        )


def test_validator_passes_for_supplied_input() -> None:
    """A required input with a present mapped column does not raise."""
    fail_if_required_inputs_missing(
        required_inputs=frozenset({"wohnen__wohnfläche_hh"}),
        available_columns=pd.Index(["living_space_hh"]),
    )


def test_mapping_report_counts_sum_to_total() -> None:
    """Mapped plus unmapped counts equal the total number of inputs."""
    report = create_mapping_report(SOEP_TO_GETTSIM)
    assert (
        report["n_inputs_mapped"] + report["n_inputs_unmapped"]
        == (report["n_inputs_total"])
    )


def test_mapping_report_lists_mapped_pairs() -> None:
    """The report's `mapped` table contains a known mapped pair."""
    report = create_mapping_report(SOEP_TO_GETTSIM)
    assert report["mapped"]["wohnen__wohnfläche_hh"] == "living_space_hh"
