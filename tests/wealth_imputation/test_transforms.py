import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.transforms import (
    asinh_scaled,
    inverse_asinh_scaled,
)


def test_asinh_scaled_round_trip_recovers_values_including_zero_and_negatives():
    """Round-trip transform recovers original values with high precision."""
    values = pd.Series([-50000.0, 0.0, 1234.5, 1_000_000.0])
    restored = inverse_asinh_scaled(
        asinh_scaled(values, scale=10_000.0), scale=10_000.0
    )
    np.testing.assert_allclose(restored.to_numpy(), values.to_numpy(), atol=1e-4)


def test_asinh_scaled_rejects_nonpositive_scale():
    """Scale validation rejects zero and negative values."""
    with pytest.raises(ValueError, match="scale must be positive"):
        asinh_scaled(pd.Series([1.0]), scale=0.0)
