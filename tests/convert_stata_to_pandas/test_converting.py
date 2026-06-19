from pathlib import Path
from unittest.mock import MagicMock, patch

from soep_preparation.utilities.general import get_relevant_column_names


@patch("soep_preparation.utilities.general.load_script")
@patch("inspect.getsource")
def test_get_relevant_column_names_with_raw_data_in_docstring(
    mock_getsource: MagicMock,
    mock_load_script: MagicMock,
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
    mock_load_script.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = ["real_column"]
    assert actual == expected


@patch("soep_preparation.utilities.general.load_script")
@patch("inspect.getsource")
def test_get_relevant_column_names_ignores_raw_data_in_comment(
    mock_getsource: MagicMock,
    mock_load_script: MagicMock,
) -> None:
    function_content = """
    def clean():
        # historical note: raw_data["phantom_column"] once lived here
        value = raw_data["real_column"]
    """
    mock_getsource.return_value = function_content

    mock_module = MagicMock()
    mock_module.clean = lambda: None
    mock_load_script.return_value = mock_module

    actual = get_relevant_column_names(Path("dummy/path"))
    assert actual == ["real_column"]


@patch("soep_preparation.utilities.general.load_script")
@patch("inspect.getsource")
def test_get_relevant_column_names_ignores_dynamic_fstring_subscript(
    mock_getsource: MagicMock,
    mock_load_script: MagicMock,
) -> None:
    function_content = """
    def clean():
        for month in range(1, 13):
            out[f"m_{month}"] = raw_data[f"kal1e{month:03d}"]
        value = raw_data["real_column"]
    """
    mock_getsource.return_value = function_content

    mock_module = MagicMock()
    mock_module.clean = lambda: None
    mock_load_script.return_value = mock_module

    actual = get_relevant_column_names(Path("dummy/path"))
    assert actual == ["real_column"]


@patch("soep_preparation.utilities.general.load_script")
@patch("inspect.getsource")
def test_get_relevant_column_names_with_empty_string(
    mock_getsource: MagicMock,
    mock_load_script: MagicMock,
) -> None:
    function_content = """
    def clean():
        value = raw_data[""]
    """
    mock_getsource.return_value = function_content

    mock_module = MagicMock()
    mock_module.clean = lambda: None
    mock_load_script.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = []
    assert actual == expected


@patch("soep_preparation.utilities.general.load_script")
@patch("inspect.getsource")
def test_get_relevant_column_names_valid_cases(
    mock_getsource: MagicMock,
    mock_load_script: MagicMock,
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
    mock_load_script.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = ["column_name", "another_column", "third_column"]
    assert actual == expected


@patch("soep_preparation.utilities.general.load_script")
@patch("inspect.getsource")
def test_get_relevant_column_names_mixed_cases(
    mock_getsource: MagicMock,
    mock_load_script: MagicMock,
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
    mock_load_script.return_value = mock_module

    script_path = Path("dummy/path")
    actual = get_relevant_column_names(script_path)
    expected = ["valid_column", "another_valid_column"]
    assert actual == expected
