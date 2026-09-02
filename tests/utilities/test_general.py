"""Test discovery of raw data files and combine modules."""

from pathlib import Path

import pytest

from soep_preparation.utilities.general import (
    get_combine_module_names,
    get_raw_data_file_names,
)


def _make_clean_modules(directory: Path, module_names: list[str]) -> Path:
    clean_modules = directory / "clean_modules"
    clean_modules.mkdir(parents=True)
    for module_name in module_names:
        (clean_modules / f"{module_name}.py").write_text("def clean(raw_data): ...")
    return clean_modules


def _make_raw_data(directory: Path, module_names: list[str]) -> Path:
    raw_data_dir = directory / "V1"
    raw_data_dir.mkdir(parents=True)
    for module_name in module_names:
        (raw_data_dir / f"{module_name}.dta").touch()
    return directory


def _make_combine_modules(directory: Path, script_names: list[str]) -> Path:
    combine_modules = directory / "combine_modules"
    combine_modules.mkdir(parents=True)
    for script_name in script_names:
        (combine_modules / f"{script_name}.py").write_text("def combine(**kwargs): ...")
    return combine_modules


def test_get_raw_data_file_names_raises_for_missing_required_file(
    tmp_path: Path,
) -> None:
    """A required module without its raw data file aborts the run."""
    clean_modules = _make_clean_modules(tmp_path / "src", ["pgen", "pl"])
    data_root = _make_raw_data(tmp_path / "data", ["pgen"])

    with pytest.raises(FileNotFoundError, match=r"pl\.dta"):
        get_raw_data_file_names(
            directory=clean_modules,
            data_root=data_root,
            soep_version="V1",
        )


def test_get_raw_data_file_names_skips_missing_optional_file(tmp_path: Path) -> None:
    """An optional module without its raw data file drops out of the pipeline."""
    clean_modules = _make_clean_modules(tmp_path / "src", ["pgen", "cirdef"])
    data_root = _make_raw_data(tmp_path / "data", ["pgen"])

    with pytest.warns(UserWarning, match="cirdef"):
        names = get_raw_data_file_names(
            directory=clean_modules,
            data_root=data_root,
            soep_version="V1",
            optional_modules=frozenset({"cirdef"}),
        )

    assert names == ["pgen"]


def test_get_raw_data_file_names_includes_present_optional_file(tmp_path: Path) -> None:
    """An optional module with its raw data file present is processed as usual."""
    clean_modules = _make_clean_modules(tmp_path / "src", ["pgen", "cirdef"])
    data_root = _make_raw_data(tmp_path / "data", ["pgen", "cirdef"])

    names = get_raw_data_file_names(
        directory=clean_modules,
        data_root=data_root,
        soep_version="V1",
        optional_modules=frozenset({"cirdef"}),
    )

    assert sorted(names) == ["cirdef", "pgen"]


def test_get_combine_module_names_skips_scripts_with_unavailable_module(
    tmp_path: Path,
) -> None:
    """A combine script drops out when one of its modules was not processed."""
    _make_clean_modules(tmp_path / "src", ["pequiv", "pkal", "cirdef"])
    combine_modules = _make_combine_modules(
        tmp_path / "src", ["pequiv_pkal", "cirdef_pequiv"]
    )

    with pytest.warns(UserWarning, match="cirdef_pequiv"):
        names = get_combine_module_names(
            directory=combine_modules,
            available_modules=["pequiv", "pkal"],
        )

    assert names == ["pequiv_pkal"]


def test_get_combine_module_names_raises_without_cleaning_script(
    tmp_path: Path,
) -> None:
    """A combine script naming a module with no cleaning script is a bug."""
    _make_clean_modules(tmp_path / "src", ["pequiv"])
    combine_modules = _make_combine_modules(tmp_path / "src", ["pequiv_typo"])

    with pytest.raises(ValueError, match="typo"):
        get_combine_module_names(
            directory=combine_modules,
            available_modules=["pequiv"],
        )
