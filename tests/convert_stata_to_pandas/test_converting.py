from pathlib import Path
from unittest.mock import MagicMock, patch

from soep_preparation.utilities.general import get_relevant_column_names


@patch("soep_preparation.utilities.general.load_module")
@patch("inspect.getsource")
def testget_relevant_column_names_with_raw_data_in_docstring(
    mock_getsource: MagicMock,
    mock_load_module: MagicMock,
) -> None:
    function_content = '''
    def clean():
        """
        Some documentation containing raw_data["not_a_real_column"]
        """
        value = raw_data["real_column"]
    '''
    mock_getsource.return_value = function_content

    mock_module = MagicMock()
    mock_module.clean = lambda: None
    mock_load_module.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = ["real_column"]
    assert actual == expected


@patch("soep_preparation.utilities.general.load_module")
@patch("inspect.getsource")
def testget_relevant_column_names_with_empty_string(
    mock_getsource: MagicMock,
    mock_load_module: MagicMock,
) -> None:
    function_content = """
    def clean():
        value = raw_data[""]
    """
    mock_getsource.return_value = function_content

    mock_module = MagicMock()
    mock_module.clean = lambda: None
    mock_load_module.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = []
    assert actual == expected


@patch("soep_preparation.utilities.general.load_module")
@patch("inspect.getsource")
def testget_relevant_column_names_valid_cases(
    mock_getsource: MagicMock,
    mock_load_module: MagicMock,
) -> None:
    function_content = """
    def clean():
        value = raw_data["column_name"]
        another = raw_data['another_column']
        something_else = raw_data["third_column"]
    """
    mock_getsource.return_value = function_content

    mock_module = MagicMock()
    mock_module.clean = lambda: None
    mock_load_module.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = ["column_name", "another_column", "third_column"]
    assert actual == expected


@patch("soep_preparation.utilities.general.load_module")
@patch("inspect.getsource")
def testget_relevant_column_names_mixed_cases(
    mock_getsource: MagicMock,
    mock_load_module: MagicMock,
) -> None:
    function_content = """
    def clean():
        valid_case = raw_data["valid_column"]
        invalid_case = raw_data[column_name]  # no quotes
        another_valid = raw_data['another_valid_column']
    """
    mock_getsource.return_value = function_content

    mock_module = MagicMock()
    mock_module.clean = lambda: None
    mock_load_module.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = ["valid_column", "another_valid_column"]
    assert actual == expected
