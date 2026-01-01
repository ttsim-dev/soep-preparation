"""Test _fail_if_mapping_changed function."""

from pathlib import Path

import pytest

from soep_preparation.create_metadata.task import _fail_if_mapping_changed


def test_new_variable():
    """Test error when new variable is added."""
    new_mapping = {
        "var1": {"module": "test_module", "dtype": "int64", "survey_years": [2020]}
    }
    existing_mapping = {}

    with pytest.raises(ValueError, match="added"):
        _fail_if_mapping_changed(
            new_mapping=new_mapping,
            existing_mapping=existing_mapping,
            new_mapping_path=Path("/tmp/test.yaml"),
        )


def test_removed_variable():
    """Test error when variable is removed."""
    new_mapping = {}
    existing_mapping = {
        "var1": {"module": "test_module", "dtype": "int64", "survey_years": [2020]}
    }

    with pytest.raises(ValueError, match="removed"):
        _fail_if_mapping_changed(
            new_mapping=new_mapping,
            existing_mapping=existing_mapping,
            new_mapping_path=Path("/tmp/test.yaml"),
        )


def test_dtype_changed():
    """Test error when dtype changes."""
    new_mapping = {
        "var1": {"module": "test_module", "dtype": "float64", "survey_years": [2020]}
    }
    existing_mapping = {
        "var1": {"module": "test_module", "dtype": "int64", "survey_years": [2020]}
    }

    with pytest.raises(ValueError, match="dtype changed"):
        _fail_if_mapping_changed(
            new_mapping=new_mapping,
            existing_mapping=existing_mapping,
            new_mapping_path=Path("/tmp/test.yaml"),
        )


def test_new_survey_years():
    """Test error when new survey years are added."""
    new_mapping = {
        "var1": {
            "module": "test_module",
            "dtype": "int64",
            "survey_years": [2020, 2021],
        }
    }
    existing_mapping = {
        "var1": {"module": "test_module", "dtype": "int64", "survey_years": [2020]}
    }

    with pytest.raises(ValueError, match="new survey years"):
        _fail_if_mapping_changed(
            new_mapping=new_mapping,
            existing_mapping=existing_mapping,
            new_mapping_path=Path("/tmp/test.yaml"),
        )


def test_removed_survey_years():
    """Test error when survey years are removed."""
    new_mapping = {
        "var1": {"module": "test_module", "dtype": "int64", "survey_years": [2020]}
    }
    existing_mapping = {
        "var1": {
            "module": "test_module",
            "dtype": "int64",
            "survey_years": [2020, 2021],
        }
    }

    with pytest.raises(ValueError, match="removed survey years"):
        _fail_if_mapping_changed(
            new_mapping=new_mapping,
            existing_mapping=existing_mapping,
            new_mapping_path=Path("/tmp/test.yaml"),
        )


def test_all_errors_together():
    """Test all three non-mutually-exclusive errors together."""
    new_mapping = {
        "var1": {
            "module": "test_module",
            "dtype": "float64",
            "survey_years": [2020, 2022],
        }
    }
    existing_mapping = {
        "var1": {
            "module": "test_module",
            "dtype": "int64",
            "survey_years": [2020, 2021],
        }
    }

    with pytest.raises(ValueError) as exc_info:  # noqa: PT011
        _fail_if_mapping_changed(
            new_mapping=new_mapping,
            existing_mapping=existing_mapping,
            new_mapping_path=Path("/tmp/test.yaml"),
        )
    error_msg = str(exc_info.value)
    assert "dtype changed" in error_msg
    assert "new survey years" in error_msg
    assert "removed survey years" in error_msg
