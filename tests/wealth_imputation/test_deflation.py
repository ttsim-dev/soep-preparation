"""Behavior of cross-wave donor deflation."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.deflation import (
    deflate_donor_values,
    deflation_factor,
)

_INDEX = {2012: 100.0, 2017: 150.0, 2022: 200.0}


def test_deflation_factor_scales_an_earlier_wave_to_the_target():
    """A 2012 donor is scaled by target/origin index to reach 2022 terms."""
    np.testing.assert_allclose(
        deflation_factor(_INDEX, from_year=2012, target_year=2022), 2.0, atol=1e-9
    )


def test_deflation_factor_is_one_for_the_target_year():
    """A donor already in the target year is unchanged."""
    np.testing.assert_allclose(
        deflation_factor(_INDEX, from_year=2022, target_year=2022), 1.0, atol=1e-9
    )


def test_deflate_donor_values_applies_per_donor_origin_year():
    """Each donor value is scaled by its own origin wave's factor."""
    values = np.array([100.0, 100.0])
    result = deflate_donor_values(
        values, from_years=[2012, 2017], index_by_year=_INDEX, target_year=2022
    )
    # 100 * 200/100 = 200; 100 * 200/150 = 133.33...
    np.testing.assert_allclose(result, [200.0, 400.0 / 3.0], atol=1e-6)


def test_deflate_donor_values_raises_on_unknown_year():
    """A donor wave missing from the index fails closed rather than guessing."""
    with pytest.raises(ValueError, match="2005"):
        deflate_donor_values(
            np.array([100.0]),
            from_years=[2005],
            index_by_year=_INDEX,
            target_year=2022,
        )
