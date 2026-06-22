"""Annual asset-class index levels for cross-wave donor deflation.

Donors observed in an earlier wealth wave understate 2022 values when asset prices
ran ahead of consumer prices, so the donor pool is brought to 2022 terms with
component-appropriate indices rather than a uniform CPI. Index levels are base 100 in
2000.

Source: MSCI World and REX (5-year German government bonds) annual series provided by
MImmesberger on PR #88 (`benchmark_index_annual.csv`). A house-price index for the
property components is not yet on file; until a verified series is added, property
donors are deflated by a pass-through factor of 1.0 (documented downward bias).
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
