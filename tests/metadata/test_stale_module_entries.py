"""Test the staleness guard for deleted-module catalog entries."""

import hashlib

import pytest

from soep_preparation.config import MODULES
from soep_preparation.create_metadata.task import (
    _METADATA_CATALOG,
    _fail_if_stale_module_entries,
)

_ABSENT_MODULE = "module_that_does_not_exist_xyz"


def test_real_module_does_not_raise() -> None:
    """A module with a `clean_modules/` source file is not flagged as stale."""
    _fail_if_stale_module_entries(["pl"])


def test_stale_module_raises_file_not_found() -> None:
    """A persisted entry with no source file aborts loudly."""
    with pytest.raises(FileNotFoundError):
        _fail_if_stale_module_entries([_ABSENT_MODULE])


def test_error_names_the_stale_module() -> None:
    """The error message names the stale module so it can be located."""
    with pytest.raises(FileNotFoundError, match=_ABSENT_MODULE):
        _fail_if_stale_module_entries([_ABSENT_MODULE])


def test_error_includes_rm_command_with_entry_hash() -> None:
    """The fix is a copy-pasteable `rm` targeting the entry's SHA-256-named files."""
    digest = hashlib.sha256(_ABSENT_MODULE.encode()).hexdigest()
    with pytest.raises(FileNotFoundError) as exc_info:
        _fail_if_stale_module_entries([_ABSENT_MODULE])
    error_msg = str(exc_info.value)
    assert "rm -f" in error_msg
    assert f"{digest}.pkl" in error_msg
    assert f"{digest}-node.pkl" in error_msg


def test_error_targets_both_modules_and_metadata_catalogs() -> None:
    """The command drops the stale entry from the modules and metadata catalogs."""
    with pytest.raises(FileNotFoundError) as exc_info:
        _fail_if_stale_module_entries([_ABSENT_MODULE])
    error_msg = str(exc_info.value)
    assert str(MODULES.path) in error_msg
    assert str(_METADATA_CATALOG.path) in error_msg
