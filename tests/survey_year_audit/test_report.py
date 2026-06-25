"""Survey-year alignment probe: per-variable, per-year distribution summaries.

The report helps resolve whether each variable's recorded values are aligned to the
survey year or to the previous year (issue #44): an analyst compares the year-over-year
movement against the questionnaire and external benchmarks. The summaries are
disclosure-safe — only year-cells with enough observations are described.
"""

import pandas as pd

from soep_preparation.survey_year_audit.report import build_alignment_report


def test_numeric_variable_reports_mean_per_survey_year():
    """A numeric variable's per-year mean is reported for sufficiently large cells."""
    data = {
        "income": pd.DataFrame(
            {
                "survey_year": [2019] * 40 + [2020] * 35,
                "p_id": range(75),
                "labour_income_y": [100.0] * 40 + [200.0] * 35,
            }
        )
    }
    report = build_alignment_report(modules=data, min_cell=30)
    assert report["labour_income_y"]["by_survey_year"][2019]["mean"] == 100.0
    assert report["labour_income_y"]["by_survey_year"][2020]["mean"] == 200.0


def test_small_year_cell_is_suppressed():
    """A year-cell below the minimum count is suppressed, not summarized."""
    data = {
        "income": pd.DataFrame(
            {
                "survey_year": [2019] * 10,
                "p_id": range(10),
                "labour_income_y": [50.0] * 10,
            }
        )
    }
    report = build_alignment_report(modules=data, min_cell=30)
    assert report["labour_income_y"]["by_survey_year"][2019] == {
        "n": 10,
        "suppressed": True,
    }


def test_categorical_variable_reports_category_shares():
    """Shares are reported for categories that each clear the minimum count."""
    data = {
        "status": pd.DataFrame(
            {
                "survey_year": [2019] * 80,
                "p_id": range(80),
                "employment_status": pd.Series(
                    ["employed"] * 50 + ["unemployed"] * 30, dtype="category"
                ),
            }
        )
    }
    report = build_alignment_report(modules=data, min_cell=30)
    shares = report["employment_status"]["by_survey_year"][2019]["shares"]
    assert shares["employed"] == 0.625
    assert shares["unemployed"] == 0.375


def test_rare_category_within_large_cell_is_suppressed():
    """A category below the minimum count is not disclosed even inside a large cell.

    `rare` has one observation; suppressing it alone would let an attacker recover it
    from the total and the published shares, so the smallest safe category is folded
    in with it (complementary suppression).
    """
    data = {
        "status": pd.DataFrame(
            {
                "survey_year": [2019] * 91,
                "p_id": range(91),
                "employment_status": pd.Series(
                    ["a"] * 50 + ["b"] * 40 + ["rare"], dtype="category"
                ),
            }
        )
    }
    report = build_alignment_report(modules=data, min_cell=30)
    cell = report["employment_status"]["by_survey_year"][2019]
    assert "rare" not in cell["shares"]
    assert "b" not in cell["shares"]
    assert cell["shares"]["a"] == round(50 / 91, 4)
    assert cell["n_categories_suppressed"] == 2


def test_index_variables_are_excluded():
    """Index variables carry no value information and are not summarized."""
    data = {
        "income": pd.DataFrame(
            {
                "survey_year": [2019] * 40,
                "p_id": range(40),
                "labour_income_y": [100.0] * 40,
            }
        )
    }
    report = build_alignment_report(modules=data, min_cell=30)
    assert "p_id" not in report
    assert "survey_year" not in report


def test_module_without_survey_year_is_skipped():
    """A module that lacks survey_year cannot be aligned and is skipped."""
    data = {
        "static": pd.DataFrame({"p_id": range(40), "birthplace": ["DE"] * 40}),
    }
    report = build_alignment_report(modules=data, min_cell=30)
    assert report == {}
