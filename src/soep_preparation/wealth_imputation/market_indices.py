"""Annual asset-class index levels for cross-wave donor scaling.

Donors observed in an earlier wealth wave understate 2022 values when asset prices ran
ahead of consumer prices, so the donor pool is scaled to 2022 terms with
component-appropriate indices rather than a uniform CPI. Index levels are base 100 in
2000.

These mappings (financial -> MSCI World, pension -> REX bonds, property -> BIS house
prices, residual -> a property/equity blend) are **modelling assumptions, not deflation
identities**: a household's component need not track its proxy index, so each is a
sensitivity choice that should be compared against CPI / no-scaling alternatives, not
treated as a known transformation.

Sources:
- MSCI World and REX (5-year German government bonds): annual series provided by
  MImmesberger on PR #88 (`benchmark_index_annual.csv`), base 100 in 2000.
- House prices: BIS nominal residential property prices for Germany, FRED series
  `QDEN628BIS`, annual averages of the quarterly index (base 100 in 2010); used to
  deflate the owner-occupied and other-real-estate components.
"""

from types import MappingProxyType

MSCI_WORLD_INDEX: MappingProxyType[int, float] = MappingProxyType(
    {
        2000: 100.00,
        2001: 88.98,
        2002: 61.16,
        2003: 68.67,
        2004: 73.79,
        2005: 94.10,
        2006: 102.61,
        2007: 104.12,
        2008: 63.20,
        2009: 83.55,
        2010: 101.23,
        2011: 97.43,
        2012: 111.66,
        2013: 132.30,
        2014: 157.40,
        2015: 172.10,
        2016: 192.87,
        2017: 210.64,
        2018: 200.65,
        2019: 261.32,
        2020: 280.21,
        2021: 358.35,
        2022: 312.28,
        2023: 371.90,
        2024: 467.85,
    }
)

REX_BOND_INDEX: MappingProxyType[int, float] = MappingProxyType(
    {
        2000: 100.00,
        2001: 105.91,
        2002: 115.84,
        2003: 120.54,
        2004: 128.43,
        2005: 132.88,
        2006: 133.22,
        2007: 137.09,
        2008: 151.50,
        2009: 159.68,
        2010: 167.11,
        2011: 180.33,
        2012: 188.09,
        2013: 187.07,
        2014: 197.10,
        2015: 198.16,
        2016: 201.99,
        2017: 199.54,
        2018: 201.92,
        2019: 202.68,
        2020: 204.15,
        2021: 201.03,
        2022: 178.85,
        2023: 186.26,
        2024: 188.37,
    }
)

_BASE_YEAR = 2000
_REBASE_LEVEL = 100.0
_RESIDUAL_PROPERTY_WEIGHT = 0.5


def _rebased(index: MappingProxyType[int, float]) -> dict[int, float]:
    """Rescale an index so its base-year level is `_REBASE_LEVEL`."""
    base = index[_BASE_YEAR]
    return {year: level / base * _REBASE_LEVEL for year, level in index.items()}


HOUSE_PRICE_INDEX: MappingProxyType[int, float] = MappingProxyType(
    {
        2000: 101.37,
        2001: 101.27,
        2002: 100.58,
        2003: 99.18,
        2004: 97.88,
        2005: 96.98,
        2006: 97.07,
        2007: 96.88,
        2008: 100.05,
        2009: 99.42,
        2010: 100.00,
        2011: 102.42,
        2012: 105.47,
        2013: 108.67,
        2014: 111.85,
        2015: 117.12,
        2016: 125.93,
        2017: 133.62,
        2018: 142.53,
        2019: 150.75,
        2020: 162.47,
        2021: 181.28,
        2022: 192.22,
        2023: 176.03,
        2024: 173.35,
    }
)

# The reconciliation residual to the official total is dominated by business assets and
# other real estate (it also absorbs omitted liabilities and editing discrepancies).
# Lacking a separate series for either, deflate it by an equal-weight blend of the
# property and equity indices (other real estate ~ house prices; business ~ equities),
# each rebased to a common base year so the 50/50 weighting is economic, not an artefact
# of the legs' different base years. The weight is a rough default, easily adjusted.
_HOUSE_REBASED = _rebased(HOUSE_PRICE_INDEX)
_MSCI_REBASED = _rebased(MSCI_WORLD_INDEX)
RESIDUAL_INDEX: MappingProxyType[int, float] = MappingProxyType(
    {
        year: _RESIDUAL_PROPERTY_WEIGHT * _HOUSE_REBASED[year]
        + (1.0 - _RESIDUAL_PROPERTY_WEIGHT) * _MSCI_REBASED[year]
        for year in sorted(set(_HOUSE_REBASED) & set(_MSCI_REBASED))
    }
)
