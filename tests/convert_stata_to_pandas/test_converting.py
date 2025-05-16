import inspect
from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest.mock import MagicMock, patch

from soep_preparation.convert_stata_to_pandas.task import _get_relevant_column_names


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

        dataset = Path("dummy/path")
        actual = _get_relevant_column_names(dataset)
        expected = ["real_column"]
        assert actual == expected


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

        dataset = Path("dummy/path")
        actual = _get_relevant_column_names(dataset)
        expected = []
        assert actual == expected


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

        dataset = Path("dummy/path")
        actual = _get_relevant_column_names(dataset)
        expected = ["column_name", "another_column", "third_column"]
        assert actual == expected


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

        dataset = Path("dummy/path")
        actual = _get_relevant_column_names(dataset)
        expected = ["valid_column", "another_valid_column"]
        assert actual == expected
