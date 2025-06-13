import inspect
from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from soep_preparation.convert_stata_to_pandas.task import _get_relevant_column_names


@pytest.mark.skip(reason="Test skipped since loading real script required.")
def test_get_relevant_column_names_with_raw_data_in_docstring():
    function_content = '''
    def clean():
        """
        Some documentation containing raw_data["not_a_real_column"]
        """
        value = raw_data["real_column"]
    '''

    with (
        patch.object(SourceFileLoader, "load_module") as mock_loader,
        patch.object(
            inspect,
            "getsource",
            return_value=function_content,
        ),
    ):
        mock_module = MagicMock()
        mock_loader.return_value = mock_module

        script_path = Path("dummy/path")
        actual = _get_relevant_column_names(script_path)
        expected = ["real_column"]
        assert actual == expected


@pytest.mark.skip(reason="Test skipped since loading real script required.")
def test_get_relevant_column_names_with_empty_string():
    function_content = """
    def clean():
        value = raw_data[""]
    """

    with (
        patch.object(SourceFileLoader, "load_module") as mock_loader,
        patch.object(
            inspect,
            "getsource",
            return_value=function_content,
        ),
    ):
        mock_module = MagicMock()
        mock_loader.return_value = mock_module

        script_path = Path("dummy/path")
        actual = _get_relevant_column_names(script_path)
        expected = []
        assert actual == expected


@pytest.mark.skip(reason="Test skipped since loading real script required.")
def test_get_relevant_column_names_valid_cases():
    function_content = """
    def clean():
        value = raw_data["column_name"]
        another = raw_data['another_column']
        something_else = raw_data["third_column"]
    """

    with (
        patch.object(SourceFileLoader, "load_module") as mock_loader,
        patch.object(
            inspect,
            "getsource",
            return_value=function_content,
        ),
    ):
        mock_module = MagicMock()
        mock_loader.return_value = mock_module

        script_path = Path("dummy/path")
        actual = _get_relevant_column_names(script_path)
        expected = ["column_name", "another_column", "third_column"]
        assert actual == expected


@pytest.mark.skip(reason="Test skipped since loading real script required.")
def test_get_relevant_column_names_mixed_cases():
    function_content = """
    def clean():
        valid_case = raw_data["valid_column"]
        invalid_case = raw_data[column_name]  # no quotes
        another_valid = raw_data['another_valid_column']
    """

    with (
        patch.object(SourceFileLoader, "load_module") as mock_loader,
        patch.object(
            inspect,
            "getsource",
            return_value=function_content,
        ),
    ):
        mock_module = MagicMock()
        mock_loader.return_value = mock_module

        script_path = Path("dummy/path")
        actual = _get_relevant_column_names(script_path)
        expected = ["valid_column", "another_valid_column"]
        assert actual == expected
