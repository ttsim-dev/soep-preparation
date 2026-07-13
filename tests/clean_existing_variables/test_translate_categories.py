"""Tests for `translate_categories`: relabel categorical categories German->English."""

import pandas as pd
import pytest

from soep_preparation.utilities.data_manipulator import translate_categories

_TRANSLATIONS = {
    "Voll erwerbstätig": "Full-time employed",
    "Nicht erwerbstätig": "Not employed",
}


def _german_series() -> pd.Series:
    return pd.Series(
        ["Voll erwerbstätig", "Nicht erwerbstätig", "Voll erwerbstätig"],
        dtype="string[pyarrow]",
    ).astype("category")


def test_translate_relabels_values():
    """Each value is relabelled to its English translation."""
    result = translate_categories(_german_series(), _TRANSLATIONS)
    assert result.tolist() == [
        "Full-time employed",
        "Not employed",
        "Full-time employed",
    ]


def test_translate_sorts_categories_alphabetically():
    """The result's categories are English-alphabetical, independent of input order."""
    result = translate_categories(_german_series(), _TRANSLATIONS)
    assert list(result.cat.categories) == ["Full-time employed", "Not employed"]


def test_translate_keeps_string_category_dtype():
    """The categories keep the pyarrow-backed string dtype."""
    result = translate_categories(_german_series(), _TRANSLATIONS)
    assert result.cat.categories.dtype == "string[pyarrow]"


def test_translate_fails_on_uncovered_category():
    """A category missing from the map fails loudly rather than staying German."""
    with pytest.raises(ValueError, match="Voll erwerbstätig"):
        translate_categories(_german_series(), {"Nicht erwerbstätig": "Not employed"})


def test_translate_extra_map_keys_are_ignored():
    """Keys not present in the data are allowed (a shared map may over-cover)."""
    result = translate_categories(
        _german_series(),
        {**_TRANSLATIONS, "Teilzeitbeschäftigung": "Part-time employed"},
    )
    assert "Part-time employed" not in list(result.cat.categories)
