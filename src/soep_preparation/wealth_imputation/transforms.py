"""Signed log-like transforms for monetary amounts.

`asinh(y / s)` behaves like `log` for large `|y|` but is finite and smooth at
zero and for negative values (net debt), so it suits wealth amounts that mix a
zero mass with positive assets and negative net positions.
"""

import numpy as np
import pandas as pd


def _fail_if_scale_invalid(scale: float) -> None:
    if not np.isfinite(scale) or scale <= 0:
        msg = f"scale must be positive and finite, got {scale}"
        raise ValueError(msg)


def asinh_scaled(values: pd.Series, scale: float) -> pd.Series:
    """Apply the scaled inverse-hyperbolic-sine transform `asinh(values / scale)`.

    Args:
        values: Monetary amounts (may be zero or negative).
        scale: Positive, finite component scale `s` controlling the linear-to-log knee.

    Returns:
        The transformed series as float64, same index.
    """
    _fail_if_scale_invalid(scale)
    numeric = values.to_numpy(dtype="float64", na_value=np.nan)
    return pd.Series(np.arcsinh(numeric / scale), index=values.index)


def inverse_asinh_scaled(transformed: pd.Series, scale: float) -> pd.Series:
    """Invert `asinh_scaled`: `scale * sinh(transformed)`.

    Args:
        transformed: Values on the `asinh`-scaled axis.
        scale: The same positive, finite scale used in the forward transform.

    Returns:
        The back-transformed monetary amounts as float64, same index.
    """
    _fail_if_scale_invalid(scale)
    numeric = transformed.to_numpy(dtype="float64", na_value=np.nan)
    return pd.Series(np.sinh(numeric) * scale, index=transformed.index)
