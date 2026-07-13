"""Behavior of the per-component sequential value generator."""

import numpy as np
import pytest

from soep_preparation.wealth_imputation.value_generator import (
    ComponentDraw,
    draw_component,
)


def _draw(seed: int = 0) -> ComponentDraw:
    return draw_component(
        ownership_prob=np.array([1.0, 0.0]),
        ownership_share=np.array([0.5, 0.5]),
        recipient_predicted=np.array([100.0, 100.0]),
        donor_predicted=np.array([100.0]),
        donor_observed=np.array([80.0]),
        scale=100.0,
        k=1,
        rng=np.random.default_rng(seed=seed),
    )


def test_draw_component_owner_takes_share_of_drawn_amount_nonowner_zero():
    """An owner's value is share x drawn amount; a non-owner's value is zero."""
    result = _draw()
    np.testing.assert_allclose(result.person_value, [40.0, 0.0], atol=1e-6)


def test_draw_component_probabilities_0_and_1_act_as_a_deterministic_filter():
    """Probability 1 always owns and 0 never owns, regardless of seed."""
    result = _draw()
    np.testing.assert_array_equal(result.owns, [True, False])
    np.testing.assert_allclose(result.gross_amount, [80.0, 0.0], atol=1e-6)


def test_draw_component_is_deterministic_for_a_fixed_seed():
    """Same seed reproduces ownership, gross amount, and person value."""
    first = _draw(seed=11)
    second = _draw(seed=11)
    np.testing.assert_array_equal(first.owns, second.owns)
    np.testing.assert_array_equal(first.person_value, second.person_value)


@pytest.mark.parametrize("bad_prob", [-0.1, 1.5, np.nan])
def test_draw_component_rejects_probabilities_outside_unit_interval(bad_prob: float):
    """An ownership probability outside [0, 1] fails closed."""
    with pytest.raises(ValueError, match="ownership_prob"):
        draw_component(
            ownership_prob=np.array([bad_prob]),
            ownership_share=np.array([0.5]),
            recipient_predicted=np.array([100.0]),
            donor_predicted=np.array([100.0]),
            donor_observed=np.array([80.0]),
            scale=100.0,
            k=1,
            rng=np.random.default_rng(seed=0),
        )


def test_draw_component_surfaces_owner_nearest_donor_distance():
    """An owner's nearest-donor score distance is surfaced; a non-owner's is NaN."""
    result = draw_component(
        ownership_prob=np.array([1.0, 0.0]),
        ownership_share=np.array([1.0, 1.0]),
        recipient_predicted=np.array([100.0, 100.0]),
        donor_predicted=np.array([90.0]),  # nearest donor is 10 away in score space
        donor_observed=np.array([80.0]),
        scale=100.0,
        k=1,
        rng=np.random.default_rng(seed=0),
    )
    np.testing.assert_allclose(result.distances[0], 10.0, atol=1e-6)
    assert np.isnan(result.distances[1])


def test_draw_component_surfaces_owner_donor_index_nonowner_negative_one():
    """An owner records its drawn donor index; a non-owner records -1."""
    result = draw_component(
        ownership_prob=np.array([1.0, 0.0]),
        ownership_share=np.array([1.0, 1.0]),
        recipient_predicted=np.array([100.0, 100.0]),
        donor_predicted=np.array([100.0]),
        donor_observed=np.array([80.0]),
        scale=100.0,
        k=1,
        rng=np.random.default_rng(seed=0),
    )
    assert result.donor_indices[0] == 0
    assert result.donor_indices[1] == -1


def test_draw_component_rejects_mismatched_recipient_lengths():
    """Recipient arrays of differing length fail closed."""
    with pytest.raises(ValueError, match="one length"):
        draw_component(
            ownership_prob=np.array([1.0, 0.0]),
            ownership_share=np.array([0.5]),
            recipient_predicted=np.array([100.0, 100.0]),
            donor_predicted=np.array([100.0]),
            donor_observed=np.array([80.0]),
            scale=100.0,
            k=1,
            rng=np.random.default_rng(seed=0),
        )
