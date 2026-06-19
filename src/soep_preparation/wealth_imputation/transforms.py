"""Signed log-like transforms for monetary amounts.

`asinh(y / s)` behaves like `log` for large `|y|` but is finite and smooth at
zero and for negative values (net debt), so it suits wealth amounts that mix a
zero mass with positive assets and negative net positions.
"""

import numpy as np
import pandas as pd


def asinh_scaled(values: pd.Series, scale: float) -> pd.Series:
    """Apply the scaled inverse-hyperbolic-sine transform `asinh(values / scale)`.

    Args:
        values: Monetary amounts (may be zero or negative).
        scale: Positive component scale `s` controlling the linear-to-log knee.

    Returns:
        The transformed series, same index.
    """
    if scale <= 0:
        msg = f"scale must be positive, got {scale}"
        raise ValueError(msg)
    return pd.Series(np.arcsinh(values.to_numpy() / scale), index=values.index)


def inverse_asinh_scaled(transformed: pd.Series, scale: float) -> pd.Series:
    """Invert `asinh_scaled`: `scale * sinh(transformed)`.

    Args:
        transformed: Values on the `asinh`-scaled axis.
        scale: The same positive scale used in the forward transform.

    Returns:
        The back-transformed monetary amounts, same index.
    """
    if scale <= 0:
        msg = f"scale must be positive, got {scale}"
        raise ValueError(msg)
    return pd.Series(np.sinh(transformed.to_numpy()) * scale, index=transformed.index)
