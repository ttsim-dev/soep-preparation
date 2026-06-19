# Code-review bundle (RE-REVIEW) — wealth-imputation harness FOUNDATION

**For:** a confirming re-review of the foundation after the first Pro review's
"with fixes" verdict was actioned. Self-contained — the actual *fixed* committed source +
tests are embedded in Section 8. No repo access needed.

## 1. How to use this bundle

This is the **second pass**. The first review returned "build with changes" and six
blocking findings; all were addressed (Section 6). Your job: (a) confirm each fix actually
landed and is correct, (b) catch anything the fixes introduced or missed. The model/draw
stage, data-wired registry population, PUNR completion layer, validation harness, and the
intentionally-deferred items in Section 7 are **out of scope — do not flag their absence.**
End with the Section 7a verdict (numbered, file:line).

## 2. What this is

The code-only foundation of a SOEP wealth-imputation harness (a provisional 2022
household-wealth proxy): eight modules + a Milestone-0 probe, unit-tested on synthetic
fixtures with **no access to real survey data** (a hard project constraint). 40 tests pass;
`prek run --all-files` (ruff + ty + format + codespell) is fully clean.

## 3. Architecture invariants the code must honour

From the design spec (`2026-06-19-soep-wealth-imputation-harness-design.md`):

- **Person-level construction, then a plain household sum** over members.
- **Ownership share applied exactly once** (`shares.resolve_person_amount`); never
  re-multiplied at aggregation (`aggregate.household_component_sum` is a plain groupby-sum).
- **PUNR residual** = `household_total − member_sum` (`aggregate.punr_residual`), positive
  when the official total exceeds the represented-member sum.
- **Net-wealth sign** comes from the canonical component (`components.component_sign` /
  `registry.WealthVariable.net_sign`); liabilities (mortgage, consumer debt) are −1.
- **PMM** draws a real observed donor value from the *k* nearest by predictive score,
  within a caliper and excluding ineligible donors, *before* selection (`donors.pmm_draw`).
- **Three-axis provenance enums** + a **typed, fail-closed registry**
  (`fail_if_unresolved_required`, plus `__post_init__` consistency checks).
- **Disclosure safety:** the probe emits only registry keys, presence booleans, and numeric
  summaries — never row-level data.
- **Monetary values are `float64`**; `pathlib.Path`; RNG via `np.random.default_rng(seed=)`
  only; Python ≥3.14 (no `from __future__ import annotations`); idiomatic `# noqa: <RULE>`
  allowed for localized lint exceptions.

## 4. Branch commits (`feat/wealth-imputation-harness`, off `main` @ 26ecc91)

```
7b13c29 fix(wealth): address Pro code-review findings in the foundation   <-- the fix wave
6fd7a70 docs: add foundation code-review bundle for external Pro review
5f39257 feat(wealth): Milestone-0 disclosure-safe probe report + pytask wiring
208fbdd feat(wealth): predictive-mean-matching multi-donor draw
e6ccf97 feat(wealth): household plain-sum aggregation + PUNR residual
a3af3e3 feat(wealth): one-shot ownership-share resolution to person amounts
466828b feat(wealth): typed registry contract with fail-closed verification
819f2dd feat(wealth): canonical component ontology + provenance enums
41fe852 feat(wealth): asinh-scaled transform with round-trip inverse
f194d24 feat(wealth): scaffold wealth_imputation package + add scikit-learn
(be8f7fe: plan-doc constraint tweak)
```

## 5. Focus for this re-review

1. **Did each Section-6 fix land correctly?** Especially the PMM caliper/exclusion ordering
   and the aggregation panel guards.
2. **Did the fixes introduce defects?** New validation that is too strict/loose, dtype or
   index-alignment regressions, off-by-one in the donor filtering.
3. **Tests:** do the new tests actually exercise the fixed behavior (e.g. the
   inside/outside-caliper test would fail against the old code)?
4. **Residual risk** in the foundation surface only (not the deferred work).

## 6. First-review blocking findings and how they were addressed

1. **PMM could select a donor outside the caliper** → `donors.pmm_draw` now builds the
   eligible set (exclusions, then caliper) **before** the nearest-*k* selection; added a
   `_fail_if_pmm_inputs_invalid` guard (1-D, equal donor lengths, non-empty, `k>=1`, finite
   non-negative caliper, finite scores/values) and a regression test where one donor is
   inside and another outside the caliper.
2. **No recipient-specific exclusion mechanism** → added the `exclude` parameter
   (per-recipient donor indices) and a `PmmResult` exposing `values` (float64),
   `donor_indices`, and `distances`; plus an "excluded nearest donor is never selected" test.
3. **Aggregation accepted incomplete/duplicated panels** → `household_component_sum` now
   requires `p_id, hh_id, survey_year, component, person_component_value`, rejects duplicate
   `(p_id, survey_year, component)` rows and missing person values, and returns `float64`.
4. **`punr_residual` could silently misalign** → it now requires identical indexes before
   subtracting; `float64`; a liability test asserts debt `200 − 150 = +50` (positive
   magnitudes; the sign is applied only when building net wealth).
5. **`float64` invariant unenforced** → enforced in `transforms` (output cast; non-finite
   scale rejected), `shares` (cast + finite/`[0,1]` validation), `donors` (values cast),
   and `aggregate` (member sum + residual cast); tests cover float32 and integer-donor
   inputs.
6. **Share resolution + registry signs did not fail closed** → `resolve_person_amount`
   validates identical indexes, finite observed values, and observed shares in `[0, 1]`;
   the registry **drops the contradictable `sign` field** (net sign derives from the
   component via `net_sign`), types `aggregation_rule` as an `AggregationRule` enum, and a
   frozen `__post_init__` rejects inverted expected bounds and VERIFIED entries with empty
   evidence.

Known minor items from the first review were also fixed: `math.isclose` for float
assertions; the "determinism" test now compares two same-seed runs (values + donor
indices); the probe test asserts the exact disclosure-safe schema.

## 7. Intentionally deferred (out of scope — do not flag)

- Donor-exclusion *logic* population (recipient/co-owner/household/lineage): the `exclude`
  interface exists; filling it needs real rosters (data wiring).
- `task_probe.py` pytask dependency declaration / schema-only catalog reads (the probe is
  gated on Milestone 0).
- Should-fix-later: scalable nearest-neighbour instead of per-recipient `argsort`; wider
  missing-code representation (labelled/categorical codes); a combined release gate for
  unresolved/absent/duplicate/contradictory required entries.

## 7a. Requested verdict

1. Ready to merge? **Yes | No | With fixes**, with reasoning.
2. Any first-review fix that did **not** land correctly (file:line).
3. Any **new** defect introduced by the fixes (file:line).
4. Any Section-3 invariant still violated or only partial.

## 8. Fixed code under review

(Empty `__init__.py` markers for the package and test package are omitted.)

### Source modules

#### src/soep_preparation/wealth_imputation/transforms.py

```python
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
```

#### src/soep_preparation/wealth_imputation/components.py

```python
"""Canonical wealth ontology and three-axis cell-provenance enums."""

from enum import Enum


class CanonicalComponent(Enum):
    """Wealth components, normalized across waves by the registry."""

    OWNER_OCCUPIED_PROPERTY_GROSS = "owner_occupied_property_gross"
    OWNER_OCCUPIED_MORTGAGE = "owner_occupied_mortgage"
    OTHER_REAL_ESTATE = "other_real_estate"
    FINANCIAL_ASSETS = "financial_assets"
    PRIVATE_PENSION = "private_pension"
    BUSINESS_ASSETS = "business_assets"
    VEHICLES = "vehicles"
    CONSUMER_DEBT = "consumer_debt"


class RawInputState(Enum):
    """Axis A — the raw response state of a person-component cell."""

    OBSERVED_COMPLETE = "observed_complete"
    MISSING_SHARE = "missing_share"
    MISSING_AMOUNT = "missing_amount"
    MISSING_FILTER = "missing_filter"
    PUNR = "punr"
    STRUCTURAL_ABSENT = "structural_absent"
    INCONSISTENT = "inconsistent"
    AMBIGUOUS = "ambiguous"


class AlignmentEvidence(Enum):
    """Axis B — how a raw-derived value compares to the official completed value."""

    AGREES_ALL = "agrees_all"
    AGREES_MEAN = "agrees_mean"
    OFFICIAL_DIFFERS = "official_differs"
    FLAG_CONFLICT = "flag_conflict"
    UNAVAILABLE = "unavailable"


class HarnessAction(Enum):
    """Axis C — the action this harness took on a cell."""

    PRESERVED = "preserved"
    DETERMINISTIC_EDIT = "deterministic_edit"
    OWNERSHIP_IMPUTED = "ownership_imputed"
    AMOUNT_PMM = "amount_pmm"
    SHARE_IMPUTED = "share_imputed"
    PUNR_RESIDUAL = "punr_residual"
    STATISTICAL_REPLACEMENT = "statistical_replacement"
    SUPPRESSED = "suppressed"


LIABILITY_COMPONENTS: frozenset[CanonicalComponent] = frozenset(
    {
        CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        CanonicalComponent.CONSUMER_DEBT,
    }
)


def component_sign(component: CanonicalComponent) -> int:
    """Return `-1` for liability components and `+1` for assets.

    Args:
        component: The wealth component.

    Returns:
        The sign used when summing components into net wealth.

    """
    return -1 if component in LIABILITY_COMPONENTS else 1
```

#### src/soep_preparation/wealth_imputation/registry.py

```python
"""Typed semantic contract for raw wealth variables.

Each `WealthVariable` declares one raw questionnaire item for one component in one
wave, together with the verification status that gates it. Production reads only
verified (or inferred) entries; an unresolved entry that is `required_for_release`
fails closed via `fail_if_unresolved_required`. The net-wealth sign is derived from
the canonical component (`net_sign`), never carried as a separate, contradictable
field.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from soep_preparation.wealth_imputation.components import (
    CanonicalComponent,
    component_sign,
)


class VerificationStatus(Enum):
    """Whether a registry entry's variable mapping has been confirmed."""

    VERIFIED = "verified"
    INFERRED = "inferred"
    UNRESOLVED = "unresolved"


class AggregationRule(Enum):
    """How a raw item's person values combine into the household total."""

    PERSON_SHARE_THEN_PLAIN_SUM = "person_share_then_plain_sum"
    """Per-person = joint amount times ownership share; household = plain sum."""
    HOUSEHOLD_DIRECT = "household_direct"
    """Item is asked at household level; no per-person share step."""


@dataclass(frozen=True)
class WealthVariable:
    """One raw wealth item for one component in one wave."""

    component: CanonicalComponent
    """The canonical component this item contributes to."""
    wave: int
    """Survey year of the raw item."""
    source_file: str
    """Themed-file / raw-file name (a `RAW_DATA_FILES` catalog key)."""
    raw_variable: str
    """Raw column name in `source_file`."""
    concept: str
    """Questionnaire concept in plain language."""
    unit: str
    """Measurement unit, e.g. `"euro"` or `"share"`."""
    universe: str
    """Population the item applies to (filter)."""
    missing_codes: tuple[int, ...]
    """Negative SOEP codes treated as missing for this item."""
    bracket_variable: str | None
    """Range follow-up variable for `don't know`, if any."""
    ownership_flag: str | None
    """Ownership yes/no filter variable, if any."""
    ownership_share_variable: str | None
    """Per-person ownership-share variable, if any."""
    level: Literal["person", "household"]
    """Whether the raw item is asked of persons or households."""
    aggregation_rule: AggregationRule
    """How person items combine to the household."""
    expected_min: float | None
    """Plausible lower bound for range checks, if known."""
    expected_max: float | None
    """Plausible upper bound for range checks, if known."""
    verification_status: VerificationStatus
    """Whether the mapping is verified, inferred, or unresolved."""
    verification_evidence: str
    """Citation/evidence for the mapping."""
    codebook_version: str
    """SOEP version the evidence is drawn from."""
    required_for_release: bool
    """Whether the advertised full total depends on this entry."""

    def __post_init__(self) -> None:
        """Validate internal consistency; fail closed on contradictions."""
        if (
            self.expected_min is not None
            and self.expected_max is not None
            and self.expected_min > self.expected_max
        ):
            msg = (
                f"expected_min ({self.expected_min}) exceeds expected_max "
                f"({self.expected_max}) for {self.raw_variable}."
            )
            raise ValueError(msg)
        if (
            self.verification_status is VerificationStatus.VERIFIED
            and not self.verification_evidence.strip()
        ):
            msg = f"VERIFIED entry {self.raw_variable} requires non-empty evidence."
            raise ValueError(msg)

    @property
    def net_sign(self) -> int:
        """Net-wealth sign for this component (+1 asset, -1 liability)."""
        return component_sign(self.component)


def fail_if_unresolved_required(entries: Sequence[WealthVariable]) -> None:
    """Raise if any release-critical entry is unresolved.

    Args:
        entries: Registry entries to check.

    Raises:
        ValueError: If a `required_for_release` entry is `UNRESOLVED`.
    """
    offenders = [
        f"{entry.component.value}/{entry.wave}/{entry.raw_variable}"
        for entry in entries
        if entry.required_for_release
        and entry.verification_status is VerificationStatus.UNRESOLVED
    ]
    if offenders:
        joined = "\n".join(offenders)
        msg = (
            "Release-critical registry entries are unresolved:\n"
            f"{joined}\n"
            "Resolve them against the codebook (Milestone 0) before release."
        )
        raise ValueError(msg)
```

#### src/soep_preparation/wealth_imputation/shares.py

```python
"""Convert jointly-held asset totals to per-person amounts via ownership shares.

Applied exactly once, when building the person-level panel from raw answers. The
resulting `person_component_value` is final and share-resolved, so household
aggregation downstream is a plain sum (never a second share multiply).
"""

import numpy as np
import pandas as pd


def _fail_if_share_inputs_invalid(
    joint_amount: pd.Series, ownership_share: pd.Series
) -> None:
    if not joint_amount.index.equals(ownership_share.index):
        msg = "joint_amount and ownership_share must share an identical index."
        raise ValueError(msg)
    observed_share = ownership_share.dropna().astype("float64").to_numpy()
    if not np.all(np.isfinite(observed_share)):
        msg = "ownership_share must be finite where observed."
        raise ValueError(msg)
    if np.any((observed_share < 0) | (observed_share > 1)):
        msg = "observed ownership_share must lie in [0, 1]."
        raise ValueError(msg)
    observed_amount = joint_amount.dropna().astype("float64").to_numpy()
    if not np.all(np.isfinite(observed_amount)):
        msg = "joint_amount must be finite where observed."
        raise ValueError(msg)


def resolve_person_amount(
    joint_amount: pd.Series, ownership_share: pd.Series
) -> pd.Series:
    """Return the person's share of a jointly-held asset value.

    Args:
        joint_amount: Total value of the asset, same index as `ownership_share`;
            finite where observed.
        ownership_share: The person's ownership share in `[0, 1]` where observed;
            NA if unknown.

    Returns:
        `joint_amount * ownership_share` as float64, with NA propagated where either
        input is NA.

    Raises:
        ValueError: On a mismatched index, a non-finite observed value, or an
            observed share outside `[0, 1]`.
    """
    _fail_if_share_inputs_invalid(joint_amount, ownership_share)
    return (joint_amount * ownership_share).astype("float64")
```

#### src/soep_preparation/wealth_imputation/aggregate.py

```python
"""Aggregate share-resolved person amounts to households and isolate PUNR residuals.

Household wealth is the plain sum of members' share-resolved person amounts plus a
household-level partial-unit-nonresponse (PUNR) residual — the part of the official
household total not covered by represented members. Aggregation fails closed on an
invalid person panel (duplicate person-component rows or missing represented values),
so silent double-counting or a fake PUNR residual cannot arise.
"""

import pandas as pd

_PERSON_PANEL_COLUMNS = (
    "p_id",
    "hh_id",
    "survey_year",
    "component",
    "person_component_value",
)


def _fail_if_person_panel_invalid(person_panel: pd.DataFrame) -> None:
    missing = [col for col in _PERSON_PANEL_COLUMNS if col not in person_panel.columns]
    if missing:
        msg = f"person_panel is missing required columns: {missing}"
        raise ValueError(msg)
    if person_panel.duplicated(subset=["p_id", "survey_year", "component"]).any():
        msg = (
            "person_panel has duplicate (p_id, survey_year, component) rows; each "
            "person-component cell must be unique before aggregation."
        )
        raise ValueError(msg)
    if person_panel["person_component_value"].isna().any():
        msg = (
            "person_panel has missing person_component_value entries; resolve item "
            "nonresponse before aggregation so it is not mistaken for a PUNR residual."
        )
        raise ValueError(msg)


def household_component_sum(person_panel: pd.DataFrame) -> pd.DataFrame:
    """Sum share-resolved person amounts within household, year, and component.

    Args:
        person_panel: Columns `p_id`, `hh_id`, `survey_year`, `component`,
            `person_component_value`. Each `(p_id, survey_year, component)` must be
            unique and `person_component_value` must be non-missing.

    Returns:
        Columns `hh_id`, `survey_year`, `component`, `member_sum` (float64).

    Raises:
        ValueError: If required columns are missing, person-component rows are
            duplicated, or person values are missing.
    """
    _fail_if_person_panel_invalid(person_panel)
    grouped = (
        person_panel.groupby(["hh_id", "survey_year", "component"], observed=True)[
            "person_component_value"
        ]
        .sum()
        .reset_index(name="member_sum")
    )
    grouped["member_sum"] = grouped["member_sum"].astype("float64")
    return grouped


def punr_residual(household_total: pd.Series, member_sum: pd.Series) -> pd.Series:
    """Return the household total minus the represented-member sum.

    Both series must share an identical index so the subtraction is positional, not
    re-aligned by pandas. The residual is positive when the official household total
    exceeds the represented-member sum (partial-unit nonresponse), in the same sign
    convention as the inputs (debt carried as positive magnitudes).

    Args:
        household_total: Official household component total, keyed.
        member_sum: Sum of represented members' share-resolved amounts, same index.

    Returns:
        The residual attributed to partial-unit nonresponse (float64).

    Raises:
        ValueError: If the two series do not share an identical index.
    """
    if not household_total.index.equals(member_sum.index):
        msg = "household_total and member_sum must share an identical index."
        raise ValueError(msg)
    return (household_total - member_sum).astype("float64")
```

#### src/soep_preparation/wealth_imputation/donors.py

```python
"""Predictive-mean-matching donor draws.

Each recipient borrows an observed amount from one of its `k` nearest *eligible*
donors in predictive-score space, so imputed values stay on the real observed
support and the back-transformation of `asinh`-scale predictions never has to be
computed analytically. Eligibility — recipient-specific exclusions and the caliper —
is enforced *before* the nearest `k` are chosen, so an out-of-caliper or excluded
donor can never be drawn.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class PmmResult:
    """Outcome of a PMM draw, with donor diagnostics."""

    values: np.ndarray
    """Drawn donor values as float64, shape `(n_recipients,)`."""
    donor_indices: np.ndarray
    """Index into the donor arrays of the drawn donor, shape `(n_recipients,)`."""
    distances: np.ndarray
    """Score distance to the drawn donor as float64, shape `(n_recipients,)`."""


def _fail_if_pmm_inputs_invalid(  # noqa: PLR0913
    recipient_scores: np.ndarray,
    donor_scores: np.ndarray,
    donor_values: np.ndarray,
    k: int,
    caliper: float | None,
    exclude: Sequence[Sequence[int]] | None,
) -> None:
    for name, arr in (
        ("recipient_scores", recipient_scores),
        ("donor_scores", donor_scores),
        ("donor_values", donor_values),
    ):
        if arr.ndim != 1:
            msg = f"{name} must be 1-D, got {arr.ndim}-D"
            raise ValueError(msg)
        if not np.all(np.isfinite(arr)):
            msg = f"{name} must be finite (no NaN/inf)"
            raise ValueError(msg)
    if donor_scores.shape[0] != donor_values.shape[0]:
        msg = (
            "donor_scores and donor_values must have equal length, got "
            f"{donor_scores.shape[0]} and {donor_values.shape[0]}"
        )
        raise ValueError(msg)
    if donor_scores.shape[0] == 0:
        msg = "donors must be non-empty"
        raise ValueError(msg)
    if k < 1:
        msg = f"k must be >= 1, got {k}"
        raise ValueError(msg)
    if caliper is not None and (not np.isfinite(caliper) or caliper < 0):
        msg = f"caliper must be finite and non-negative, got {caliper}"
        raise ValueError(msg)
    if exclude is not None and len(exclude) != recipient_scores.shape[0]:
        msg = (
            "exclude must have one entry per recipient, got "
            f"{len(exclude)} for {recipient_scores.shape[0]} recipients"
        )
        raise ValueError(msg)


def pmm_draw(  # noqa: PLR0913
    recipient_scores: np.ndarray,
    donor_scores: np.ndarray,
    donor_values: np.ndarray,
    k: int,
    rng: np.random.Generator,
    caliper: float | None = None,
    exclude: Sequence[Sequence[int]] | None = None,
) -> PmmResult:
    """Draw one near, eligible donor's observed value per recipient.

    For each recipient the donor set is filtered to the eligible candidates — those
    not in that recipient's `exclude` list and (if a `caliper` is given) within it —
    *before* the nearest `k` are selected and one is drawn with `rng`. This
    guarantees an out-of-caliper or excluded donor is never returned.

    Args:
        recipient_scores: Finite predictive scores, shape `(n_recipients,)`.
        donor_scores: Finite predictive scores, shape `(n_donors,)`.
        donor_values: Finite observed values, shape `(n_donors,)`.
        k: Number of nearest eligible donors to sample from (>= 1).
        rng: NumPy random generator.
        caliper: Maximum allowed score distance to a donor, if set (>= 0).
        exclude: Per-recipient sequences of donor indices to exclude, if any.

    Returns:
        A `PmmResult` with drawn values (float64), donor indices, and distances.

    Raises:
        ValueError: On invalid inputs, or if a recipient has no eligible donor.
    """
    _fail_if_pmm_inputs_invalid(
        recipient_scores, donor_scores, donor_values, k, caliper, exclude
    )
    all_indices = np.arange(donor_scores.shape[0])
    n_recipients = recipient_scores.shape[0]
    values = np.empty(n_recipients, dtype=np.float64)
    donor_indices = np.empty(n_recipients, dtype=np.intp)
    distances_out = np.empty(n_recipients, dtype=np.float64)
    for i, score in enumerate(recipient_scores):
        eligible = all_indices
        if exclude is not None:
            excluded = np.asarray(tuple(exclude[i]), dtype=np.intp)
            eligible = eligible[~np.isin(eligible, excluded)]
        distances = np.abs(donor_scores[eligible] - score)
        if caliper is not None:
            within = distances <= caliper
            eligible = eligible[within]
            distances = distances[within]
        if eligible.shape[0] == 0:
            msg = f"No eligible donor within caliper {caliper} for recipient {i}."
            raise ValueError(msg)
        nearest = np.argsort(distances)[:k]
        chosen = rng.choice(nearest)
        donor_indices[i] = eligible[chosen]
        values[i] = float(donor_values[eligible[chosen]])
        distances_out[i] = float(distances[chosen])
    return PmmResult(
        values=values, donor_indices=donor_indices, distances=distances_out
    )
```

#### src/soep_preparation/wealth_imputation/probe.py

```python
"""Milestone-0 source probe: confirm registry variables exist, disclosure-safe.

Reports only registry keys and presence booleans — never row-level data — so it is
safe to read from logs/artifacts under the no-data-access rule.
"""

from collections.abc import Mapping, Sequence

from soep_preparation.wealth_imputation.registry import (
    VerificationStatus,
    WealthVariable,
)


def assemble_probe_report(
    entries: Sequence[WealthVariable],
    available_columns: Mapping[str, frozenset[str]],
) -> dict:
    """Build a disclosure-safe presence report for registry entries.

    Args:
        entries: Registry entries to probe.
        available_columns: Source-file name → set of its column names.

    Returns:
        A dict with per-entry presence flags and a numeric summary. Contains no
        row-level data.
    """
    rows = []
    for entry in entries:
        present = entry.raw_variable in available_columns.get(
            entry.source_file, frozenset()
        )
        rows.append(
            {
                "component": entry.component.value,
                "wave": entry.wave,
                "source_file": entry.source_file,
                "raw_variable": entry.raw_variable,
                "present": present,
                "verification_status": entry.verification_status.value,
            }
        )
    n_present = sum(row["present"] for row in rows)
    n_unresolved_required = sum(
        entry.required_for_release
        and entry.verification_status is VerificationStatus.UNRESOLVED
        for entry in entries
    )
    summary = {
        "n_entries": len(rows),
        "n_present": n_present,
        "n_missing": len(rows) - n_present,
        "n_unresolved_required": n_unresolved_required,
    }
    return {"entries": rows, "summary": summary}
```

#### src/soep_preparation/wealth_imputation/task_probe.py

```python
"""Milestone-0 probe task: write the disclosure-safe registry presence report."""

import json
from pathlib import Path
from typing import Annotated

from pytask import Product

from soep_preparation.config import BLD, RAW_DATA_FILES, get_raw_data_file_names
from soep_preparation.wealth_imputation.probe import assemble_probe_report
from soep_preparation.wealth_imputation.registry_content import REGISTRY_ENTRIES


def task_wealth_imputation_probe(
    report_path: Annotated[Path, Product] = BLD
    / "wealth_imputation"
    / "milestone_0_probe.json",
) -> None:
    """Probe registry variables against available raw columns; write a JSON report.

    Reads only column names from each needed `RAW_DATA_FILES` entry and writes a
    disclosure-safe presence report (no row-level data).
    """
    needed = {entry.source_file for entry in REGISTRY_ENTRIES}
    available = {
        name: frozenset(RAW_DATA_FILES[name].load().columns)
        for name in get_raw_data_file_names()
        if name in needed
    }
    report = assemble_probe_report(REGISTRY_ENTRIES, available)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
```

#### src/soep_preparation/wealth_imputation/registry_content.py

```python
"""Registry entries for the raw wealth battery (populated in the data-wiring plan)."""

from soep_preparation.wealth_imputation.registry import WealthVariable

REGISTRY_ENTRIES: tuple[WealthVariable, ...] = ()
```

### Tests

#### tests/wealth_imputation/test_transforms.py

```python
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


def test_asinh_scaled_returns_float64_for_float32_input():
    """A float32 input is promoted to a float64 transformed output."""
    values = pd.Series([1234.5, -50.0], dtype="float32")
    assert asinh_scaled(values, scale=1000.0).dtype == np.float64


def test_asinh_scaled_rejects_nonpositive_scale():
    """Scale validation rejects zero and negative values."""
    with pytest.raises(ValueError, match="scale must be positive"):
        asinh_scaled(pd.Series([1.0]), scale=0.0)


def test_asinh_scaled_rejects_non_finite_scale():
    """Scale validation rejects non-finite values."""
    with pytest.raises(ValueError, match="finite"):
        asinh_scaled(pd.Series([1.0]), scale=float("inf"))
```

#### tests/wealth_imputation/test_components.py

```python
"""Tests for canonical component ontology and provenance enums."""

from soep_preparation.wealth_imputation.components import (
    LIABILITY_COMPONENTS,
    CanonicalComponent,
    component_sign,
)


def test_component_sign_is_negative_for_liabilities_positive_for_assets() -> None:
    """Return -1 for liabilities and +1 for assets."""
    assert component_sign(CanonicalComponent.CONSUMER_DEBT) == -1
    assert component_sign(CanonicalComponent.OWNER_OCCUPIED_MORTGAGE) == -1
    assert component_sign(CanonicalComponent.FINANCIAL_ASSETS) == 1


def test_liability_components_are_exactly_mortgage_and_consumer_debt() -> None:
    """LIABILITY_COMPONENTS contains only mortgage and consumer debt."""
    assert (
        frozenset(
            {
                CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
                CanonicalComponent.CONSUMER_DEBT,
            }
        )
        == LIABILITY_COMPONENTS
    )
```

#### tests/wealth_imputation/test_registry.py

```python
import pytest

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.registry import (
    AggregationRule,
    VerificationStatus,
    WealthVariable,
    fail_if_unresolved_required,
)


def _entry(  # noqa: PLR0913
    *,
    status: VerificationStatus,
    required: bool,
    component: CanonicalComponent = CanonicalComponent.FINANCIAL_ASSETS,
    evidence: str = "codebook p.42",
    expected_min: float | None = 0.0,
    expected_max: float | None = None,
) -> WealthVariable:
    return WealthVariable(
        component=component,
        wave=2017,
        source_file="pl",
        raw_variable="f0100a",
        concept="financial assets value",
        unit="euro",
        universe="adults 17+",
        missing_codes=(-1, -2, -8),
        bracket_variable=None,
        ownership_flag=None,
        ownership_share_variable=None,
        level="person",
        aggregation_rule=AggregationRule.PERSON_SHARE_THEN_PLAIN_SUM,
        expected_min=expected_min,
        expected_max=expected_max,
        verification_status=status,
        verification_evidence=evidence,
        codebook_version="V41",
        required_for_release=required,
    )


def test_fail_if_unresolved_required_raises_for_required_unresolved_entry():
    entries = [_entry(status=VerificationStatus.UNRESOLVED, required=True)]
    with pytest.raises(
        ValueError, match="Release-critical registry entries are unresolved"
    ):
        fail_if_unresolved_required(entries)


def test_fail_if_unresolved_required_allows_unresolved_optional_entry():
    entries = [_entry(status=VerificationStatus.UNRESOLVED, required=False)]
    fail_if_unresolved_required(entries)  # no raise


def test_wealth_variable_is_frozen():
    entry = _entry(status=VerificationStatus.VERIFIED, required=True)
    with pytest.raises(AttributeError):
        entry.raw_variable = "x"  # ty: ignore[invalid-assignment]


def test_net_sign_is_minus_one_for_liability_component():
    entry = _entry(
        status=VerificationStatus.VERIFIED,
        required=True,
        component=CanonicalComponent.CONSUMER_DEBT,
    )
    assert entry.net_sign == -1


def test_net_sign_is_plus_one_for_asset_component():
    entry = _entry(status=VerificationStatus.VERIFIED, required=True)
    assert entry.net_sign == 1


def test_wealth_variable_rejects_inverted_expected_bounds():
    with pytest.raises(ValueError, match="exceeds expected_max"):
        _entry(
            status=VerificationStatus.INFERRED,
            required=False,
            expected_min=10.0,
            expected_max=1.0,
        )


def test_verified_entry_requires_non_empty_evidence():
    with pytest.raises(ValueError, match="requires non-empty evidence"):
        _entry(status=VerificationStatus.VERIFIED, required=True, evidence="")
```

#### tests/wealth_imputation/test_shares.py

```python
import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.shares import resolve_person_amount


def test_resolve_person_amount_applies_share_once():
    joint = pd.Series([200_000.0, 200_000.0])
    share = pd.Series([0.5, 1.0])
    result = resolve_person_amount(joint, share)
    pd.testing.assert_series_equal(
        result, pd.Series([100_000.0, 200_000.0]), check_names=False
    )


def test_resolve_person_amount_missing_share_yields_na():
    joint = pd.Series([200_000.0])
    share = pd.Series([pd.NA], dtype="Float64")
    result = resolve_person_amount(joint, share)
    assert pd.isna(result.iloc[0])


def test_resolve_person_amount_returns_float64():
    result = resolve_person_amount(pd.Series([200_000.0]), pd.Series([0.5]))
    assert result.dtype == np.float64


def test_resolve_person_amount_rejects_mismatched_index():
    joint = pd.Series([200_000.0], index=[0])
    share = pd.Series([0.5], index=[1])
    with pytest.raises(ValueError, match="identical index"):
        resolve_person_amount(joint, share)


@pytest.mark.parametrize("bad_share", [-0.1, 1.5])
def test_resolve_person_amount_rejects_share_outside_unit_interval(bad_share: float):
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        resolve_person_amount(pd.Series([200_000.0]), pd.Series([bad_share]))


def test_resolve_person_amount_rejects_non_finite_amount():
    with pytest.raises(ValueError, match="finite"):
        resolve_person_amount(pd.Series([np.inf]), pd.Series([0.5]))
```

#### tests/wealth_imputation/test_aggregate.py

```python
import math

import numpy as np
import pandas as pd
import pytest

from soep_preparation.wealth_imputation.aggregate import (
    household_component_sum,
    punr_residual,
)


def _panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "p_id": [1, 2, 3, 1],
            "hh_id": [1, 1, 2, 1],
            "survey_year": [2017, 2017, 2017, 2012],
            "component": ["financial_assets"] * 4,
            "person_component_value": [100.0, 50.0, 200.0, 70.0],
        }
    )


def test_household_component_sum_groups_by_household_year_and_component() -> None:
    out = household_component_sum(_panel())
    keyed = {
        (hh_id, survey_year, component): member_sum
        for hh_id, survey_year, component, member_sum in zip(
            out["hh_id"],
            out["survey_year"],
            out["component"],
            out["member_sum"],
            strict=True,
        )
    }
    assert keyed == {
        (1, 2017, "financial_assets"): 150.0,
        (2, 2017, "financial_assets"): 200.0,
        (1, 2012, "financial_assets"): 70.0,
    }


def test_household_component_sum_returns_float64_member_sum() -> None:
    out = household_component_sum(_panel())
    assert out["member_sum"].dtype == np.float64


def test_household_component_sum_rejects_duplicate_person_component_rows() -> None:
    panel = _panel()
    with pytest.raises(ValueError, match="duplicate"):
        household_component_sum(pd.concat([panel, panel.iloc[[0]]], ignore_index=True))


def test_household_component_sum_rejects_missing_person_value() -> None:
    panel = _panel()
    panel.loc[0, "person_component_value"] = pd.NA
    with pytest.raises(ValueError, match="missing person_component_value"):
        household_component_sum(panel)


def test_household_component_sum_rejects_missing_required_column() -> None:
    with pytest.raises(ValueError, match="missing required columns"):
        household_component_sum(_panel().drop(columns=["p_id"]))


def test_punr_residual_is_zero_when_member_sum_equals_household_total() -> None:
    total = pd.Series([150.0, 200.0])
    member_sum = pd.Series([150.0, 200.0])
    pd.testing.assert_series_equal(
        punr_residual(total, member_sum), pd.Series([0.0, 0.0]), check_names=False
    )


def test_punr_residual_is_positive_when_household_exceeds_member_sum() -> None:
    total = pd.Series([150.0])
    member_sum = pd.Series([100.0])
    assert math.isclose(punr_residual(total, member_sum).iloc[0], 50.0)


def test_punr_residual_for_debt_uses_positive_magnitudes() -> None:
    # Debt is carried as positive magnitudes: 200 household - 150 members = +50 PUNR.
    household_debt = pd.Series([200.0])
    member_debt = pd.Series([150.0])
    assert math.isclose(punr_residual(household_debt, member_debt).iloc[0], 50.0)


def test_punr_residual_rejects_mismatched_index() -> None:
    total = pd.Series([150.0], index=[0])
    member_sum = pd.Series([100.0], index=[1])
    with pytest.raises(ValueError, match="identical index"):
        punr_residual(total, member_sum)
```

#### tests/wealth_imputation/test_donors.py

```python
import math

import numpy as np
import pytest

from soep_preparation.wealth_imputation.donors import pmm_draw


def test_pmm_draw_is_deterministic_under_same_seed():
    donor_scores = np.array([0.0, 1.0, 5.0, 10.0])
    donor_values = np.array([10.0, 11.0, 50.0, 100.0])
    recipient_scores = np.array([0.4, 6.0])
    first = pmm_draw(
        recipient_scores,
        donor_scores,
        donor_values,
        k=2,
        rng=np.random.default_rng(seed=0),
    )
    second = pmm_draw(
        recipient_scores,
        donor_scores,
        donor_values,
        k=2,
        rng=np.random.default_rng(seed=0),
    )
    np.testing.assert_array_equal(first.values, second.values)
    np.testing.assert_array_equal(first.donor_indices, second.donor_indices)


def test_pmm_draw_never_selects_donor_outside_caliper():
    # Nearest donor (0.1) is inside the caliper; the other (100.0) is outside it.
    donor_scores = np.array([0.1, 100.0])
    donor_values = np.array([10.0, 999.0])
    recipient_scores = np.array([0.0])
    rng = np.random.default_rng(seed=1)
    for _ in range(20):
        result = pmm_draw(
            recipient_scores, donor_scores, donor_values, k=2, rng=rng, caliper=0.2
        )
        assert result.donor_indices[0] == 0
        assert math.isclose(result.values[0], 10.0)


def test_pmm_draw_never_selects_excluded_donor():
    donor_scores = np.array([0.0, 1.0])
    donor_values = np.array([10.0, 11.0])
    recipient_scores = np.array([0.0])
    rng = np.random.default_rng(seed=0)
    for _ in range(20):
        result = pmm_draw(
            recipient_scores, donor_scores, donor_values, k=1, rng=rng, exclude=[[0]]
        )
        assert result.donor_indices[0] == 1
        assert math.isclose(result.values[0], 11.0)


def test_pmm_draw_raises_when_no_donor_within_caliper():
    donor_scores = np.array([0.0, 1.0])
    donor_values = np.array([10.0, 11.0])
    recipient_scores = np.array([100.0])
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="No eligible donor"):
        pmm_draw(
            recipient_scores, donor_scores, donor_values, k=1, rng=rng, caliper=0.5
        )


def test_pmm_draw_rejects_k_below_one():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="k must be >= 1"):
        pmm_draw(np.array([0.0]), np.array([0.0]), np.array([1.0]), k=0, rng=rng)


def test_pmm_draw_rejects_empty_donors():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="non-empty"):
        pmm_draw(np.array([0.0]), np.array([]), np.array([]), k=1, rng=rng)


def test_pmm_draw_rejects_mismatched_donor_lengths():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="equal length"):
        pmm_draw(np.array([0.0]), np.array([0.0, 1.0]), np.array([10.0]), k=1, rng=rng)


def test_pmm_draw_rejects_non_finite_recipient_score():
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="finite"):
        pmm_draw(np.array([np.nan]), np.array([0.0]), np.array([10.0]), k=1, rng=rng)


def test_pmm_draw_returns_float64_values_for_integer_donors():
    donor_values = np.array([10, 11], dtype=np.int64)
    result = pmm_draw(
        np.array([0.0]),
        np.array([0.0, 1.0]),
        donor_values,
        k=1,
        rng=np.random.default_rng(seed=0),
    )
    assert result.values.dtype == np.float64
```

#### tests/wealth_imputation/test_probe.py

```python
import json

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.probe import assemble_probe_report
from soep_preparation.wealth_imputation.registry import (
    AggregationRule,
    VerificationStatus,
    WealthVariable,
)

_EXPECTED_ENTRY_KEYS = {
    "component",
    "wave",
    "source_file",
    "raw_variable",
    "present",
    "verification_status",
}
_EXPECTED_SUMMARY_KEYS = {
    "n_entries",
    "n_present",
    "n_missing",
    "n_unresolved_required",
}


def _entry(raw_variable: str, *, required: bool) -> WealthVariable:
    return WealthVariable(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        wave=2017,
        source_file="pl",
        raw_variable=raw_variable,
        concept="financial assets",
        unit="euro",
        universe="adults 17+",
        missing_codes=(-1,),
        bracket_variable=None,
        ownership_flag=None,
        ownership_share_variable=None,
        level="person",
        aggregation_rule=AggregationRule.PERSON_SHARE_THEN_PLAIN_SUM,
        expected_min=0.0,
        expected_max=None,
        verification_status=VerificationStatus.UNRESOLVED,
        verification_evidence="",
        codebook_version="V41",
        required_for_release=required,
    )


def test_assemble_probe_report_flags_present_and_missing_without_leaking_data():
    entries = [
        _entry("present_var", required=True),
        _entry("absent_var", required=True),
    ]
    available = {"pl": frozenset({"present_var", "other"})}
    report = assemble_probe_report(entries, available)
    assert report["summary"]["n_present"] == 1
    assert report["summary"]["n_missing"] == 1
    assert report["summary"]["n_unresolved_required"] == 2  # noqa: PLR2004
    presence = {row["raw_variable"]: row["present"] for row in report["entries"]}
    assert presence == {"present_var": True, "absent_var": False}


def test_assemble_probe_report_has_exact_disclosure_safe_schema():
    report = assemble_probe_report(
        [_entry("present_var", required=True)], {"pl": frozenset({"present_var"})}
    )
    assert set(report.keys()) == {"entries", "summary"}
    assert set(report["summary"].keys()) == _EXPECTED_SUMMARY_KEYS
    assert set(report["entries"][0].keys()) == _EXPECTED_ENTRY_KEYS
    # Disclosure-safe: the whole report serialises to JSON (only primitives/keys).
    json.dumps(report)
```
