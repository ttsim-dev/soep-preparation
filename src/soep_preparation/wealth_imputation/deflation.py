"""Bring earlier-wave donor values into target-wave terms via asset-class indices.

A donor observed in wave `w` is scaled by `index[target] / index[w]` so the pre-2022
donor pool reflects 2022 price levels for its component (equities, bonds, ...). This
reduces the downward bias a uniform CPI leaves when asset prices outpaced consumer
prices. The per-component index is a **sensitivity assumption, not an identity** (a
household's holding need not track the proxy index); see `market_indices`. Fails closed
on a donor wave absent from the index.
"""

from collections.abc import Mapping, Sequence

import numpy as np


def deflation_factor(
    index_by_year: Mapping[int, float], *, from_year: int, target_year: int
) -> float:
    """Return the factor scaling a `from_year` value into `target_year` terms.

    Args:
        index_by_year: Annual index levels for the asset class.
        from_year: Wave the donor value was observed in.
        target_year: Wave to express the value in.

    Returns:
        `index[target_year] / index[from_year]`.

    Raises:
        ValueError: If either year is absent from the index.
    """
    for year in (from_year, target_year):
        if year not in index_by_year:
            msg = f"Year {year} is absent from the index."
            raise ValueError(msg)
    return index_by_year[target_year] / index_by_year[from_year]


def deflate_donor_values(
    values: np.ndarray,
    from_years: Sequence[int],
    *,
    index_by_year: Mapping[int, float],
    target_year: int,
) -> np.ndarray:
    """Scale each donor value from its origin wave into target-wave terms.

    Args:
        values: Donor amounts, shape `(n_donors,)`.
        from_years: Origin wave per donor, same length as `values`.
        index_by_year: Annual index levels for the component's asset class.
        target_year: Wave to express the values in.

    Returns:
        The deflated donor values as float64, shape `(n_donors,)`.

    Raises:
        ValueError: If `from_years` length differs, or a year is absent from the index.
    """
    amounts = np.asarray(values, dtype="float64")
    if len(from_years) != amounts.shape[0]:
        msg = f"from_years has {len(from_years)} entries for {amounts.shape[0]} values."
        raise ValueError(msg)
    factors = np.array(
        [
            deflation_factor(
                index_by_year, from_year=int(year), target_year=target_year
            )
            for year in from_years
        ],
        dtype="float64",
    )
    return amounts * factors
