import inspect
from importlib.machinery import SourceFileLoader
from pathlib import Path
from unittest.mock import MagicMock, patch

from soep_cleaning.dataset_pickling.task import _columns_for_dataset


def test_columns_for_dataset_with_raw_in_docstring():
    function_content = '''
    def clean():
        """
        Some documentation containing raw["not_a_real_column"]
        """
        value = raw["real_column"]
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
        actual = _columns_for_dataset(dataset)
        expected = ["real_column"]
        assert actual == expected


def test_columns_for_dataset_with_empty_string():
    function_content = """
    def clean():
        value = raw[""]
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
        actual = _columns_for_dataset(dataset)
        expected = []
        assert actual == expected


def test_columns_for_dataset_valid_cases():
    function_content = """
    def clean():
        value = raw["column_name"]
        another = raw['another_column']
        something_else = raw["third_column"]
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
        actual = _columns_for_dataset(dataset)
        expected = ["column_name", "another_column", "third_column"]
        assert actual == expected


def test_columns_for_dataset_mixed_cases():
    function_content = """
    def clean():
        valid_case = raw["valid_column"]
        invalid_case = raw[column_name]  # no quotes
        another_valid = raw['another_valid_column']
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
        actual = _columns_for_dataset(dataset)
        expected = ["valid_column", "another_valid_column"]
        assert actual == expected
