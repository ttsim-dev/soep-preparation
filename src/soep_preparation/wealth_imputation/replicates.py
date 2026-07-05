"""Component-only 2022 wealth as a set of projection replicates.

SOEP-Core V41 ships no 2022 wealth wave, so every 2022 cell is a forward projection
from the 2002-2017 donor waves. This module builds several complete replicates of that
projection -- the six-component net-wealth total, *not* the residual-inclusive total --
and releases them lettered `a`-`e`. The letters are projection replicates built from DIW
donor implicates, not DIW's implicates verbatim, and the release is deliberately **not**
Rubin-valid: the metadata block records that so an analyst cannot silently treat 2022 as
an ordinary observed-and-imputed wealth wave. The projection spread *mixes* the
uncertainty layers that the single-value proxy in `impute` collapses -- with a handful
of replicates it is not decomposable into them (`layer_decomposition_available=false`):

- parameter uncertainty, via an approximate-Bayesian-bootstrap reweighting of the
  training units before each refit (`bayesian_bootstrap_weights`). The same weights also
  weight the predictive-mean-matching donor selection, so donor composition varies with
  the parameter draw; it is an approximate bootstrap predictive draw, not a full
  posterior predictive;
- donor-draw uncertainty, via the single predictive-mean-matching draw each replicate
  performs (the replicates *are* the draws, so this is carried across `a`-`e`);
- donor-implicate uncertainty, via cycling a distinct DIW donor implicate `a`-`e` into
  each replicate (`select_implicate_modules`).

The transport shock is reported *separately* as a labelled macro-sensitivity axis
(`ProjectionReplicates.transport_scenario`), not folded into the projection spread. Its
scale is calibrated from the official aggregate's cross-wave log-growth dispersion
(`transport_scale_from_official_aggregates`); the 2017-to-2022 direction cannot be
observed, so it is a scenario prior, not a validated posterior, and the shock is
mean-zero on the asinh axis but shifts euro-scale means, so it moves the level.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TypedDict

import numpy as np
import pandas as pd


class OfficialWealthAggregates(TypedDict):
    """Disclosure-safe official-wealth aggregates that calibrate the transport layer."""

    wave_aggregates: dict[int, float]
    """Summed official net-wealth total per wealth wave."""
    median_absolute_total: float
    """Median absolute total -- the asinh knee for the transport shock."""


class ImplicatesMetadata(TypedDict):
    """Provenance and validity guards stored with the released projection draws."""

    method: str
    n_internal_replicates: int
    n_released_draws: int
    transport_log_scale: float
    total_scale: float
    uses_observed_2022_wealth: bool
    transport_is_scenario_axis: bool
    transport_posterior_validated: bool
    euro_scale_mean_neutral: bool
    weighted_pmm: bool
    layer_decomposition_available: bool
    component_only: bool
    residual_inclusive: bool
    donor_implicates_propagated: bool
    rubin_valid: bool
    distribution_calibrated: bool


_IMPLICATE_MODULES = ("hwealth", "pwealth")

# Offset added to a replicate's bootstrap seed so the parameter-draw RNG (the Dirichlet
# reweighting) and the donor-draw RNG (seeded by `seed`) start from different
# bitstreams. Seeding both from the same integer would draw the two uncertainty layers
# from shared entropy; a large offset keeps them independent while staying reproducible.
_BOOTSTRAP_SEED_OFFSET = 1_000_000

# The `_a` component bases `run_imputation` consumes from each wealth module. Every one
# of these carries DIW implicates `a`-`e` in the cleaned data, so requesting implicate
# `b`-`e` must find each base's sibling; a missing sibling means schema drift and fails
# closed rather than silently imputing from implicate `a`.
_REQUIRED_IMPLICATE_BASES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "hwealth": (
            "hh_property_value_primary_residence",
            "hh_net_property_value_primary_residence",
            "hh_financial_assets_value",
            "hh_vehicles_value",
            "hh_net_overall_wealth_including_vehicles_and_student_loans",
        ),
        "pwealth": (
            "property_value_primary_residence",
            "financial_assets_value",
            "private_insurances_value",
            "vehicles_value",
            "consumer_debt_value",
        ),
    }
)


def select_implicate_modules(
    modules: Mapping[str, pd.DataFrame], implicate: str
) -> Mapping[str, pd.DataFrame]:
    """Move DIW donor implicate `implicate` into the `_a` slots of the wealth modules.

    `run_imputation` reads the `_a` implicate columns. To impute from a different DIW
    donor implicate without changing `run_imputation`, overwrite each `{base}_a` column
    with its `{base}_{implicate}` sibling in the wealth modules, so each replicate can
    draw from a different DIW implicate and the between-replicate spread prices DIW's
    own imputation uncertainty.

    Fails closed: for `implicate != "a"`, every consumed component base present in `_a`
    form (`_REQUIRED_IMPLICATE_BASES`) must have its `{base}_{implicate}` sibling, so a
    schema drift cannot silently leave the run on implicate `a` while the metadata
    claims propagation. Non-consumed `_a` columns are swapped when their sibling exists.

    Args:
        modules: Cleaned SOEP modules.
        implicate: DIW implicate letter to move into the `_a` slots (`"a"` is identity).

    Returns:
        A shallow copy of `modules` with the wealth frames rewritten; the input is not
        mutated. For `implicate == "a"` the input mapping is returned unchanged.

    Raises:
        ValueError: If a consumed component's requested implicate sibling is absent.

    """
    if implicate == "a":
        return modules
    _fail_if_implicate_siblings_missing(modules, implicate)
    updated = dict(modules)
    for name in _IMPLICATE_MODULES:
        if name not in modules:
            continue
        frame = modules[name]
        replacements = {
            str(column): frame[f"{str(column)[:-2]}_{implicate}"]
            for column in frame.columns
            if str(column).endswith("_a")
            and f"{str(column)[:-2]}_{implicate}" in frame.columns
        }
        if replacements:
            updated[name] = frame.assign(**replacements)
    return updated


def _fail_if_implicate_siblings_missing(
    modules: Mapping[str, pd.DataFrame], implicate: str
) -> None:
    for name, bases in _REQUIRED_IMPLICATE_BASES.items():
        if name not in modules:
            continue
        columns = set(modules[name].columns)
        for base in bases:
            if f"{base}_a" in columns and f"{base}_{implicate}" not in columns:
                msg = (
                    f"module {name!r} has {base}_a but no implicate sibling "
                    f"{base}_{implicate}; cannot propagate implicate {implicate!r}"
                )
                raise ValueError(msg)


@dataclass(frozen=True)
class ProjectionReplicates:
    """Component-only 2022 projection replicates with transport as a separate axis."""

    projection: pd.DataFrame
    """`hh_id` plus one no-transport `draw_i` total per replicate. The interpretable
    object: its between-replicate spread *mixes* the bootstrap, donor-implicate, and
    donor-draw layers -- not decomposable into them with a handful of replicates -- and
    excludes the macro transport prior."""
    transport_scenario: pd.DataFrame
    """`hh_id` plus one `draw_i` total per replicate after the systematic transport
    shock at the primary scale. A labelled macro-sensitivity axis reported beside
    `projection`, not merged into it. `apply_transport_scenario` rebuilds it at any
    other scale from `projection`."""
    donor_implicate_swaps: Mapping[str, int]
    """Realised count of columns swapped into the `_a` slots per distinct DIW donor
    implicate used (`0` for `a`). Records that propagation actually happened, rather
    than inferring it from the requested implicate tuple."""


def impute_replicates(  # noqa: PLR0913 -- keyword-only run + replicate settings
    modules: Mapping[str, pd.DataFrame],
    *,
    n_replicates: int,
    base_seed: int,
    transport_log_scale: float,
    total_scale: float,
    n_draws: int,
    k: int,
    donor_implicates: Sequence[str] = ("a",),
    use_bootstrap: bool = True,
) -> ProjectionReplicates:
    """Build `n_replicates` component-only projection replicates of 2022 net wealth.

    Each replicate is one bootstrap refit (parameter uncertainty) carrying one donor
    draw; `n_draws` must be 1, because the replicate *is* the draw, so donor-draw
    uncertainty is carried across replicates rather than averaged into a per-replicate
    median. The transport shock is applied as a *separate* frame
    (`ProjectionReplicates.transport_scenario`), not folded into the projection, so the
    projection spread stays an interpretable between-replicate uncertainty and transport
    is a labelled macro-sensitivity axis. The columns are generic `draw_i` totals;
    lettering a released subset is a separate assembly step.

    Args:
        modules: Cleaned SOEP modules passed through to `run_imputation`.
        n_replicates: Number of replicates to draw (must be positive).
        base_seed: Seed anchoring the per-replicate bootstrap/draw seeds and the
            transport shocks.
        transport_log_scale: Transport-shock scale on the asinh axis; `0.0` makes the
            transport scenario coincide with the projection.
        total_scale: Positive asinh knee (euros) for the transport shock.
        n_draws: Draws per replicate, forwarded to `run_imputation`; must be 1.
        k: Nearest-donor count, forwarded to `run_imputation`.
        donor_implicates: DIW donor implicate letters cycled across replicates
            (`select_implicate_modules`), so each replicate draws from a different DIW
            implicate and their spread prices DIW's own imputation uncertainty. The
            default `("a",)` uses only implicate `a` (no propagation).
        use_bootstrap: When `True`, each replicate refits under its own
            approximate-Bayesian-bootstrap weights (which also weight the PMM donor
            draw). When `False`, every replicate uses the fixed unweighted fit
            (`bootstrap_seed=None`), so the replicate spread carries only the donor-draw
            layer -- used by `layer_ablation_summary`.

    Returns:
        A `ProjectionReplicates` with the no-transport `projection` frame, the shocked
        `transport_scenario` frame at the primary scale, and the realised
        donor-implicate swap counts.

    Raises:
        ValueError: If `n_replicates` is not positive, `n_draws` is not 1, or
            `donor_implicates` is empty.

    """
    # Lazy import: `impute` imports `bayesian_bootstrap_weights` from this module, so a
    # top-level import here would form a cycle.
    from soep_preparation.wealth_imputation import impute  # noqa: PLC0415

    if n_replicates <= 0:
        msg = f"n_replicates must be positive, got {n_replicates}"
        raise ValueError(msg)
    if n_draws != 1:
        # Each implicate is one predictive draw; `run_imputation` collapses multiple
        # draws to their median, which would average away the donor-draw uncertainty
        # the implicates are meant to carry.
        msg = f"each implicate is a single draw, so n_draws must be 1, got {n_draws}"
        raise ValueError(msg)
    if len(donor_implicates) == 0:
        msg = "donor_implicates must contain at least one implicate letter"
        raise ValueError(msg)
    results = [
        impute.run_imputation(
            select_implicate_modules(
                modules, donor_implicates[index % len(donor_implicates)]
            ),
            n_draws=n_draws,
            seed=base_seed + index + 1,
            k=k,
            bootstrap_seed=(
                base_seed + index + 1 + _BOOTSTRAP_SEED_OFFSET
                if use_bootstrap
                else None
            ),
        )
        for index in range(n_replicates)
    ]
    hh_id = results[0].intervals[["hh_id"]].reset_index(drop=True)
    projection = hh_id.copy()
    for index, result in enumerate(results):
        projection[f"draw_{index}"] = result.intervals["point_estimate"].to_numpy()
    transport_scenario = apply_transport_scenario(
        projection,
        transport_log_scale=transport_log_scale,
        total_scale=total_scale,
        base_seed=base_seed,
    )
    donor_implicate_swaps = {
        letter: count_implicate_swaps(modules, letter)
        for letter in dict.fromkeys(donor_implicates)
    }
    return ProjectionReplicates(
        projection=projection,
        transport_scenario=transport_scenario,
        donor_implicate_swaps=donor_implicate_swaps,
    )


def apply_transport_scenario(
    projection: pd.DataFrame,
    *,
    transport_log_scale: float,
    total_scale: float,
    base_seed: int,
) -> pd.DataFrame:
    """Apply one per-replicate asinh transport shock to a projection frame at one scale.

    Draws one systematic shock per `draw_i` column (seeded by `base_seed`, so the shocks
    are reproducible and comparable across scales) and shifts every household in that
    replicate by it. Reusable so the same projection can be shocked at several named
    scales (`transport_scale_from_official_aggregates`, a bounded scale, ...) without
    re-imputing.

    Args:
        projection: The no-transport frame with `hh_id` and one `draw_i` total column
            per replicate.
        transport_log_scale: Transport-shock scale on the asinh axis; `0.0` returns a
            copy of `projection`.
        total_scale: Positive asinh knee (euros) for the shock.
        base_seed: Seed for the per-replicate shocks.

    Returns:
        A copy of `projection` with each `draw_i` column shifted by its own shock.

    """
    draw_columns = [
        column for column in projection.columns if str(column).startswith("draw_")
    ]
    deltas = draw_transport_shocks(
        len(draw_columns), log_scale=transport_log_scale, seed=base_seed
    )
    scenario = projection.copy()
    for index, column in enumerate(draw_columns):
        scenario[column] = apply_transport_shock(
            projection[column].to_numpy(), float(deltas[index]), scale=total_scale
        )
    return scenario


def layer_ablation_summary(
    modules: Mapping[str, pd.DataFrame],
    *,
    n_replicates: int,
    base_seed: int,
    k: int,
    donor_implicates: Sequence[str] = ("a", "b", "c", "d", "e"),
) -> dict[str, dict[str, float]]:
    """Attribute the projection spread to its layers by toggling them one at a time.

    The production projection spread mixes the donor-draw, bootstrap, and
    donor-implicate layers and cannot be decomposed from a handful of replicates
    (`layer_decomposition_available=false`). This runs the no-transport projection under
    four configurations and reports each one's between-replicate spread, so a reader can
    see which layer dominates instead of reading the mixed spread as if it were a
    decomposition. Expensive -- one full set of refits per configuration -- so it is an
    opt-in diagnostic, not part of the default run.

    - `donor_draw_only`: fixed unweighted fit, single implicate -- the PMM donor draw
      alone.
    - `plus_bootstrap`: adds the approximate-Bayesian-bootstrap refit (which also
      weights the PMM donor draw).
    - `plus_donor_implicate`: fixed fit, cycling the DIW donor implicates.
    - `all_layers`: bootstrap and implicate cycling together (production, no transport).

    Args:
        modules: Cleaned SOEP modules passed through to `run_imputation`.
        n_replicates: Replicates per configuration.
        base_seed: Seed shared across configurations, so their spreads are comparable.
        k: Nearest-donor count.
        donor_implicates: DIW implicate letters for the cycling configurations.

    Returns:
        A mapping from configuration name to its `replicate_mc_summary`.

    """
    configurations = {
        "donor_draw_only": (False, ("a",)),
        "plus_bootstrap": (True, ("a",)),
        "plus_donor_implicate": (False, tuple(donor_implicates)),
        "all_layers": (True, tuple(donor_implicates)),
    }
    ablation: dict[str, dict[str, float]] = {}
    for name, (use_bootstrap, implicates) in configurations.items():
        replicates = impute_replicates(
            modules,
            n_replicates=n_replicates,
            base_seed=base_seed,
            transport_log_scale=0.0,
            total_scale=1.0,
            n_draws=1,
            k=k,
            donor_implicates=implicates,
            use_bootstrap=use_bootstrap,
        )
        ablation[name] = replicate_mc_summary(replicates.projection)
    return ablation


def count_implicate_swaps(modules: Mapping[str, pd.DataFrame], implicate: str) -> int:
    """Count the columns `select_implicate_modules` would swap for `implicate`.

    The realised propagation count: how many `_a` columns in the wealth modules have the
    requested implicate sibling. `0` for implicate `a` (identity). Reported so the run
    records that donor-implicate propagation actually happened.

    Args:
        modules: Cleaned SOEP modules.
        implicate: DIW implicate letter.

    Returns:
        The number of columns that carry the requested implicate.

    """
    if implicate == "a":
        return 0
    total = 0
    for name in _IMPLICATE_MODULES:
        if name not in modules:
            continue
        columns = set(modules[name].columns)
        total += sum(
            1
            for column in columns
            if str(column).endswith("_a")
            and f"{str(column)[:-2]}_{implicate}" in columns
        )
    return total


def transport_scenario_summary(
    projection: pd.DataFrame, scenario: pd.DataFrame
) -> dict[str, float | int]:
    """Isolate a transport shock's euro-scale effect, holding the base projection fixed.

    `projection` and `scenario` share the same base draws, so the difference in their
    aggregate means isolates the level shift the (convex) transport shock induces for
    the realised shocks -- the controlled before/after that raw production means cannot
    give.
    It is one realisation of `n_shocks` draws, not the expectation over the shock
    distribution.

    Args:
        projection: The no-transport frame.
        scenario: The same frame after `apply_transport_scenario` at some scale.

    Returns:
        The shock count, the projection and transport-scenario aggregate means, their
        difference, and the relative shift (`nan` when the base mean is ~zero).

    """
    base = _aggregate_mean(projection)
    shocked = _aggregate_mean(scenario)
    shift = shocked - base
    n_shocks = sum(1 for column in scenario.columns if str(column).startswith("draw_"))
    return {
        "n_shocks": n_shocks,
        "projection_aggregate_mean": base,
        "transport_scenario_aggregate_mean": shocked,
        "aggregate_mean_shift": shift,
        "relative_aggregate_mean_shift": (
            float("nan") if np.isclose(base, 0.0) else shift / abs(base)
        ),
    }


def _aggregate_mean(frame: pd.DataFrame) -> float:
    # Unweighted mean across recipients of the per-replicate means. It is a reference
    # level for the between-replicate spread and the transport shift, not a design-
    # weighted population aggregate (the transport *scale* is calibrated from weighted
    # population totals, but this projection mean deliberately is not).
    draw_columns = [
        column for column in frame.columns if str(column).startswith("draw_")
    ]
    per_replicate_mean = [frame[column].to_numpy().mean() for column in draw_columns]
    return float(np.mean(per_replicate_mean))


_IMPLICATE_LETTERS = "abcdefghijklmnopqrstuvwxyz"


def select_released_implicates(
    frame: pd.DataFrame,
    *,
    n_released: int = 5,
    name: str = "component_only_net_wealth_2022",
) -> pd.DataFrame:
    """Select the released component-only projection draws from a replicate frame.

    One column per released draw, keyed by `hh_id`. The columns are lettered `a`-`e`
    for a five-draw release, but they are projection replicates built from DIW donor
    implicates, not DIW's implicates verbatim (see `build_implicates_metadata`). More
    replicates than released columns can be run to characterise the between-replicate
    spread (`replicate_mc_summary`); the released set is drawn evenly across the
    replicate index so it spans the full run rather than an arbitrary prefix.

    Args:
        frame: Engine output with `hh_id` and one `draw_i` total column per replicate.
        n_released: Number of implicates to release (five mirrors DIW).
        name: Column-name stem; each released column is `{name}_{letter}`.

    Returns:
        A frame with `hh_id` and `n_released` lettered net-wealth implicate columns.

    Raises:
        ValueError: If fewer than `n_released` replicates are available.

    """
    draw_columns = [
        column for column in frame.columns if str(column).startswith("draw_")
    ]
    if len(draw_columns) < n_released:
        msg = (
            f"n_released={n_released} exceeds the {len(draw_columns)} "
            "replicates available"
        )
        raise ValueError(msg)
    positions = np.linspace(0, len(draw_columns) - 1, n_released).round().astype(int)
    released = frame[["hh_id"]].reset_index(drop=True)
    for letter, position in zip(_IMPLICATE_LETTERS, positions, strict=False):
        released[f"{name}_{letter}"] = frame[draw_columns[position]].to_numpy()
    return released


def replicate_mc_summary(frame: pd.DataFrame) -> dict[str, float | int]:
    """Summarise the between-replicate spread of the aggregate mean net wealth.

    The `aggregate_mean` is the unweighted mean across recipients (a reference level for
    the relative spreads), not a design-weighted population estimate.

    Separates three distinct quantities the release must not conflate:

    - `aggregate_between_replicate_sd`: the sample SD (`ddof=1`) of the per-replicate
      aggregate means -- the uncertainty the replicates represent, a mixture of the
      bootstrap, donor-implicate, and donor-draw layers (not decomposable into them),
      *not* simulation noise.
    - `mc_se_mean`: the Monte-Carlo standard error of the replicate mean,
      `sd / sqrt(n_replicates)` -- how tightly a finite replicate count pins the central
      aggregate.
    - the relative forms of each, divided by `|aggregate_mean|` (`nan` when the mean is
      ~zero).

    Estimated over every replicate the engine produced, not just the released subset.

    Args:
        frame: Engine output with `hh_id` and one `draw_i` total column per replicate.

    Returns:
        `n_replicates`, the mean aggregate, its between-replicate SD, the Monte-Carlo SE
        of the mean, and the relative forms of both.

    """
    draw_columns = [
        column for column in frame.columns if str(column).startswith("draw_")
    ]
    per_replicate_mean = np.array(
        [frame[column].to_numpy().mean() for column in draw_columns], dtype=float
    )
    n_replicates = len(draw_columns)
    aggregate_mean = float(per_replicate_mean.mean())
    between_replicate_sd = (
        float(per_replicate_mean.std(ddof=1)) if n_replicates > 1 else float("nan")
    )
    mc_se_mean = between_replicate_sd / np.sqrt(n_replicates)
    near_zero = np.isclose(aggregate_mean, 0.0)
    return {
        "n_replicates": n_replicates,
        "aggregate_mean": aggregate_mean,
        "aggregate_between_replicate_sd": between_replicate_sd,
        "relative_between_replicate_sd": (
            float("nan") if near_zero else between_replicate_sd / abs(aggregate_mean)
        ),
        "mc_se_mean": mc_se_mean,
        "relative_mc_se_mean": (
            float("nan") if near_zero else mc_se_mean / abs(aggregate_mean)
        ),
    }


def transport_scale_from_official_aggregates(
    wave_aggregates: Mapping[int, float],
) -> float:
    """Calibrate the transport log-scale from official cross-wave aggregate levels.

    The size of a five-year forward wealth-level projection error is unobservable for
    the 2017-to-2022 step, because SOEP-Core V41 ships no 2022 wealth wave. As an
    empirical prior, use the dispersion of the official aggregate's own five-year
    log-growth across the observed wealth waves: how much the population wealth level
    has historically moved per step bounds how far the 2022 projection can
    systematically drift. The wealth waves are equally spaced (five years), so the
    consecutive log-growth steps are comparable.

    Args:
        wave_aggregates: Official population wealth aggregate per wealth-wave year (e.g.
            summed `w011h`), for at least three waves (two growth steps).

    Returns:
        The sample standard deviation of the consecutive log-growth steps -- the
        transport log-scale for `draw_transport_shocks`.

    Raises:
        ValueError: If fewer than three waves, or a non-positive aggregate.

    """
    minimum_waves = 3
    if len(wave_aggregates) < minimum_waves:
        msg = f"need at least {minimum_waves} waves to estimate a dispersion"
        raise ValueError(msg)
    return float(_official_log_growth_steps(wave_aggregates).std(ddof=1))


def transport_log_scale_excluding_largest_step(
    wave_aggregates: Mapping[int, float],
) -> float:
    """A bounded transport scale: log-growth dispersion after dropping the largest step.

    The full growth-dispersion scale is dominated by the single most extreme five-year
    step (a crisis or a boom), which overstates how far an ordinary five-year projection
    drifts. Dropping the largest-magnitude step is a leave-one-out lower bound -- a
    conservative, explicitly labelled transport *sensitivity*, not a calibrated
    forecast-error scale.

    Args:
        wave_aggregates: Official population wealth aggregate per wealth-wave year, for
            at least four waves (three steps, so two remain after the drop).

    Returns:
        The sample SD of the log-growth steps excluding the largest-magnitude one.

    Raises:
        ValueError: If fewer than four waves, or a non-positive aggregate.

    """
    minimum_waves = 4
    if len(wave_aggregates) < minimum_waves:
        msg = (
            f"need at least {minimum_waves} waves to drop a step and keep a dispersion"
        )
        raise ValueError(msg)
    steps = _official_log_growth_steps(wave_aggregates)
    kept = np.delete(steps, int(np.argmax(np.abs(steps))))
    return float(kept.std(ddof=1))


def _official_log_growth_steps(wave_aggregates: Mapping[int, float]) -> np.ndarray:
    years = sorted(wave_aggregates)
    # A dropped wave (no observed rows) would leave unequal year gaps, so a 10-year
    # step would be compared against a 5-year one as if equal. Fail closed rather than
    # mislabel the dispersion; the wealth waves are meant to be equally spaced.
    gaps = np.diff(years)
    if len(set(gaps.tolist())) > 1:
        msg = (
            "wealth waves must be equally spaced to compare log-growth steps, "
            f"got years {years}"
        )
        raise ValueError(msg)
    levels = np.array([wave_aggregates[year] for year in years], dtype=float)
    if np.any(levels <= 0.0):
        msg = "official aggregates must be positive to take a logarithm"
        raise ValueError(msg)
    return np.diff(np.log(levels))


def robust_total_scale(totals: np.ndarray) -> float:
    """Return the median absolute net-wealth total, or 1.0 if that is not positive.

    Sets the asinh knee for the transport shock so it tracks the magnitude of the wealth
    distribution rather than a hard-coded euro figure.

    Args:
        totals: Signed household net-wealth totals.

    Returns:
        A positive, finite asinh scale.

    """
    magnitude = np.abs(np.asarray(totals, dtype="float64"))
    median = float(np.median(magnitude)) if magnitude.size else 0.0
    return median if median > 0.0 else 1.0


def official_wealth_aggregates(
    household_wealth: pd.DataFrame,
    *,
    total_column: str,
    waves: Sequence[int],
    wave_column: str = "survey_year",
    weight_column: str | None = None,
) -> OfficialWealthAggregates:
    """Aggregate the official household net-wealth total per wealth wave.

    With `weight_column`, each wave aggregate is the design-weighted total
    `Σ wᵢ · totalᵢ` (a population total, invariant to sample size and composition), and
    the knee is the weighted median absolute total. Without it, the aggregates are raw
    unweighted row sums -- sensitive to how many households each wave sampled -- so the
    resulting transport scale should be read as a sample-total-growth prior.

    Args:
        household_wealth: Cleaned household-wealth frame across waves.
        total_column: Column holding the official net-wealth total (e.g. `w011h`).
        waves: The wealth-wave years to aggregate.
        wave_column: Column identifying each row's survey year.
        weight_column: Optional household design-weight column; rows with a missing
            weight are dropped. As the calibration gate it fails closed: a negative or
            non-finite observed weight, or a requested wave whose observed households
            carry zero total weight, raises.

    Returns:
        `wave_aggregates` (weighted or raw total per wave, waves with no data omitted)
        and `median_absolute_total`.

    Raises:
        ValueError: If `weight_column` holds a negative or non-finite observed weight,
            or a requested wave has zero total observed weight.

    """
    totals = pd.to_numeric(household_wealth[total_column], errors="coerce")
    years = household_wealth[wave_column]
    weights = (
        pd.to_numeric(household_wealth[weight_column], errors="coerce")
        if weight_column is not None
        else None
    )
    if weights is not None:
        _fail_if_weight_column_invalid(weights, totals, years, waves)
    wave_aggregates: dict[int, float] = {}
    for wave in waves:
        in_wave = years == wave
        if weights is None:
            wave_totals = totals[in_wave].dropna()
            if not wave_totals.empty:
                wave_aggregates[wave] = float(wave_totals.sum())
        else:
            valid = in_wave & totals.notna() & weights.notna()
            if valid.any():
                wave_aggregates[wave] = float((totals[valid] * weights[valid]).sum())
    in_waves = years.isin(waves)
    if weights is None:
        knee = robust_total_scale(totals[in_waves].dropna().to_numpy())
    else:
        valid = in_waves & totals.notna() & weights.notna()
        knee = _weighted_median_absolute(
            totals[valid].to_numpy(), weights[valid].to_numpy()
        )
    return {"wave_aggregates": wave_aggregates, "median_absolute_total": knee}


def _fail_if_weight_column_invalid(
    weights: pd.Series,
    totals: pd.Series,
    years: pd.Series,
    waves: Sequence[int],
) -> None:
    """Fail closed on design weights that would corrupt the transport calibration.

    Only weights on rows with an observed total in a requested wave feed the aggregates,
    so those are the rows validated.
    """
    in_waves = years.isin(waves)
    observed = in_waves & totals.notna() & weights.notna()
    observed_weights = weights[observed].to_numpy(dtype="float64")
    if not np.all(np.isfinite(observed_weights)):
        msg = "weight_column must be finite (no NaN/inf) where wealth is observed"
        raise ValueError(msg)
    if np.any(observed_weights < 0.0):
        msg = "weight_column must be non-negative"
        raise ValueError(msg)
    for wave in waves:
        in_wave = (years == wave) & totals.notna() & weights.notna()
        if bool(in_wave.any()) and weights[in_wave].sum() <= 0.0:
            msg = f"weight_column has zero total weight for wave {wave}"
            raise ValueError(msg)


def _weighted_median_absolute(values: np.ndarray, weights: np.ndarray) -> float:
    """Return the weighted median of `|values|`, or 1.0 if no positive weight remains.

    The weighted median is the smallest absolute total at which the cumulative weight
    reaches half the total weight -- the robust, sample-composition-invariant analog of
    `robust_total_scale`'s asinh knee.
    """
    magnitude = np.abs(np.asarray(values, dtype="float64"))
    weight = np.asarray(weights, dtype="float64")
    total_weight = weight.sum()
    if magnitude.size == 0 or total_weight <= 0.0:
        return 1.0
    order = np.argsort(magnitude)
    cumulative = np.cumsum(weight[order])
    crossing = np.searchsorted(cumulative, total_weight / 2.0)
    median = float(magnitude[order][min(int(crossing), magnitude.size - 1)])
    return median if median > 0.0 else 1.0


def build_implicates_metadata(
    *,
    n_replicates: int,
    n_released: int,
    transport_log_scale: float,
    total_scale: float,
    donor_implicates_propagated: bool = False,
) -> ImplicatesMetadata:
    """Assemble the metadata guards recorded with the released projection draws.

    The guards keep an analyst from treating the release as ordinary
    observed-and-imputed wealth. Each flag records a way the object falls short of
    Rubin-valid SOEP wealth implicates:

    - `component_only` / `residual_inclusive`: the release is the six-component total,
      omitting the reconciliation residual (business, other real estate).
    - `transport_is_scenario_axis`: the transport shock is reported as a separate,
      labelled macro-sensitivity axis, not folded into the released projection spread;
      its scale is a calibrated prior, not a validated posterior
      (`transport_posterior_validated=false`, the 2017-to-2022 drift is unobservable).
    - `euro_scale_mean_neutral`: the asinh-axis shock is mean-zero on its own axis but
      shifts euro-scale means, so it moves the level, not just its uncertainty.
    - `weighted_pmm`: the PMM donor draw is weighted by the same bootstrap weights as
      the model fits (an approximate Bayesian bootstrap), so donor composition varies
      with the parameter draw.
    - `layer_decomposition_available`: false -- five replicates confound the bootstrap,
      donor-implicate, and donor-draw layers, so the spread cannot be attributed to one.
    - `donor_implicates_propagated`: whether replicates drew from more than one DIW
      donor implicate, so DIW's own imputation uncertainty is priced. Even when true the
      released letters are projection replicates, each built from a DIW donor implicate,
      not DIW's implicate `a`-`e` verbatim.
    - `rubin_valid`: false while any of the above hold.

    Args:
        n_replicates: Internal replicates the engine ran.
        n_released: Released projection draws.
        transport_log_scale: Calibrated transport shock scale on the asinh axis.
        total_scale: Asinh knee used for the transport shock.
        donor_implicates_propagated: Whether replicates drew from more than one DIW
            donor implicate; still leaves `rubin_valid` false because 2022 stays a
            component-only projection with an unvalidated transport prior.

    Returns:
        A JSON-serialisable metadata block.

    """
    return {
        "method": "component_only_projection_replicates",
        "n_internal_replicates": n_replicates,
        "n_released_draws": n_released,
        "transport_log_scale": float(transport_log_scale),
        "total_scale": float(total_scale),
        "uses_observed_2022_wealth": False,
        "transport_is_scenario_axis": True,
        "transport_posterior_validated": False,
        "euro_scale_mean_neutral": False,
        "weighted_pmm": True,
        "layer_decomposition_available": False,
        "component_only": True,
        "residual_inclusive": False,
        "donor_implicates_propagated": donor_implicates_propagated,
        "rubin_valid": False,
        "distribution_calibrated": False,
    }


def bayesian_bootstrap_weights(n_units: int, *, seed: int) -> np.ndarray:
    """Draw approximate-Bayesian-bootstrap weights for one replicate.

    Samples a `Dirichlet(1, ..., 1)` weight vector over the training units and scales it
    so the weights average one. Used as `sample_weight` in a replicate's model refit, so
    that each replicate reflects a different plausible parameter draw (the approximate
    Bayesian bootstrap), rather than the single fixed fit the point-estimate proxy uses.
    The same weights also weight the predictive-mean-matching donor selection
    (`pmm_draw(donor_weights=...)`), so donor composition varies with the parameter draw
    -- one weighted empirical distribution drives both the fit and the donor draw. This
    is an approximate bootstrap predictive draw, not a full posterior predictive.

    Args:
        n_units: Number of training units to reweight (must be positive).
        seed: Replicate seed; the same seed reproduces the same weights.

    Returns:
        A length-`n_units` array of non-negative weights summing to `n_units`.

    """
    if n_units <= 0:
        msg = f"n_units must be positive, got {n_units}"
        raise ValueError(msg)
    rng = np.random.default_rng(seed=seed)
    weights = rng.dirichlet(np.ones(n_units))
    return weights * n_units


def transport_log_scale_from_fold_errors(fold_log_errors: Sequence[float]) -> float:
    """Calibrate the transport-shock scale from rolling-origin forward-prediction error.

    Each rolling-origin fold predicts a wealth wave from strictly earlier waves and
    yields a systematic aggregate log error `log(predicted / observed)` for that forward
    step. The transport scale is their root mean square -- the typical magnitude by
    which a forward projection misses, absorbing both a systematic bias and between-fold
    variation. With only a few wealth waves this is a thin, scenario-grade estimate, not
    a validated posterior standard deviation.

    Args:
        fold_log_errors: One systematic log error per rolling-origin fold.

    Returns:
        The root-mean-square forward-prediction log error (non-negative).

    """
    errors = np.asarray(fold_log_errors, dtype=float)
    if errors.size == 0:
        msg = "fold_log_errors must contain at least one fold"
        raise ValueError(msg)
    return float(np.sqrt(np.mean(errors**2)))


def draw_transport_shocks(
    n_replicates: int, *, log_scale: float, seed: int
) -> np.ndarray:
    """Draw one systematic transport shock per replicate.

    Each replicate receives a single multiplicative log shift
    `delta ~ Normal(0, log_scale)` applied to every household's projected total, so the
    shock is *systematic* within a replicate. Sharing it across households is
    deliberate: only a common shift makes the between-replicate variance price the
    2017-to-2022 transport uncertainty; a per-household iid shock would average out of
    any aggregate estimand and leave that uncertainty unpriced. The mean is zero -- the
    layer adds uncertainty without asserting a known bias-correction direction.

    Args:
        n_replicates: Number of replicates to shock.
        log_scale: Transport-shock standard deviation on the log scale (>= 0); a zero
            scale disables the layer.
        seed: Seed for the shock draws.

    Returns:
        A length-`n_replicates` array of log-scale shifts.

    """
    if log_scale < 0:
        msg = f"log_scale must be non-negative, got {log_scale}"
        raise ValueError(msg)
    rng = np.random.default_rng(seed=seed)
    return rng.normal(0.0, log_scale, size=n_replicates)


def apply_transport_shock(
    totals: np.ndarray, delta: float, *, scale: float
) -> np.ndarray:
    """Shift signed household totals by `delta` on the `asinh(total / scale)` axis.

    Net wealth is signed, so a multiplicative `exp(delta)` shock is undefined on a
    negative total. Shifting on the asinh axis instead -- the same variance-stabilising
    axis the amount model fits on -- gives a monotone, signed analog of a proportional
    *level* shock: approximately multiplicative for wealth well outside
    `[-scale, scale]` and linear near zero. A positive `delta` shifts every total up
    (a richer 2022 than projected), a negative one down. The shocks are mean-zero *on
    the asinh axis*, but the transform is convex in `delta`, so a symmetric shock is not
    mean-neutral on the euro scale: `E[sinh(x + delta)] = sinh(x) E[cosh(delta)]` with
    `E[cosh(delta)] > 1`, so positive totals drift up and negative totals down in
    expectation. The layer therefore moves the euro-scale level, not only its
    uncertainty. A household near zero net wealth can cross sign under a large shock --
    an intended consequence of a level shift, not a magnitude scaler.

    Args:
        totals: Signed household net-wealth totals for one replicate.
        delta: The replicate's transport shock on the asinh axis.
        scale: Positive total scale setting the asinh knee (euros).

    Returns:
        The shocked totals, same shape as `totals`.

    """
    if scale <= 0:
        msg = f"scale must be positive, got {scale}"
        raise ValueError(msg)
    axis = np.arcsinh(np.asarray(totals, dtype=float) / scale)
    return scale * np.sinh(axis + delta)
