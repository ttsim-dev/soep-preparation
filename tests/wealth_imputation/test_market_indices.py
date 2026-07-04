"""The residual blend index combines property and equity, rebased to a common base."""

import pytest

from soep_preparation.wealth_imputation.market_indices import (
    HOUSE_PRICE_INDEX,
    MSCI_WORLD_INDEX,
    RESIDUAL_INDEX,
)

_BASE_YEAR = 2000


def test_residual_index_is_one_hundred_in_the_base_year():
    """Both legs are rebased to 100 in the base year, so their blend is too."""
    assert RESIDUAL_INDEX[_BASE_YEAR] == pytest.approx(100.0)


def test_residual_index_is_the_equal_weight_rebased_blend():
    """Each level is the mean of the two legs rebased to the common base year."""
    year = 2022
    house = HOUSE_PRICE_INDEX[year] / HOUSE_PRICE_INDEX[_BASE_YEAR] * 100.0
    equity = MSCI_WORLD_INDEX[year] / MSCI_WORLD_INDEX[_BASE_YEAR] * 100.0
    assert RESIDUAL_INDEX[year] == pytest.approx(0.5 * house + 0.5 * equity)


def test_residual_index_covers_all_wealth_waves_and_2022():
    """The index must span every donor wave plus the 2022 target."""
    for year in (2002, 2007, 2012, 2017, 2022):
        assert year in RESIDUAL_INDEX
