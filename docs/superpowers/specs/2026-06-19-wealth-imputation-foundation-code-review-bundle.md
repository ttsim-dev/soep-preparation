# Code-review bundle — wealth-imputation harness FOUNDATION

**For:** an external senior code review of the foundation implementation. Self-contained —
the actual committed source + tests are embedded at the end (Section 7). No repo access
needed.

## 1. How to use this bundle

Review the **code** in Section 7 against the plan/invariants below. This is the *foundation*
slice only — pure helpers, a typed registry contract, and a disclosure-safe probe. The
model/draw stage, the data-wired registry population, the PUNR completion layer, and the
validation harness are **deliberately deferred** to follow-on plans — do **not** flag their
absence. End with the Section 6 verdict (numbered, file:line).

## 2. What this is

The code-only foundation of a SOEP wealth-imputation harness (a provisional 2022
household-wealth proxy). Eight modules + a Milestone-0 probe, all unit-tested on synthetic
fixtures with **no access to real survey data** (a hard project constraint). Built
task-by-task via TDD; each task already passed a per-task spec+quality review. This is the
final whole-branch pass before merge.

## 3. Architecture invariants the code must honour

From the design spec (`2026-06-19-soep-wealth-imputation-harness-design.md`):

- **Person-level construction, then aggregation.** The model unit is the person-component
  cell; households are a downstream **plain sum** over members.
- **Ownership share is applied exactly once** — when turning a joint questionnaire answer
  into a per-person amount (`shares.resolve_person_amount`). It must **never** be
  re-multiplied at aggregation (`aggregate.household_component_sum` is a plain groupby-sum).
- **PUNR residual** = `household_total − member_sum` (`aggregate.punr_residual`); positive
  when the official household total exceeds the represented-member sum.
- **Net wealth signs:** liabilities (owner-occupied mortgage, consumer debt) carry sign −1
  (`components.component_sign` / `LIABILITY_COMPONENTS`).
- **PMM** draws a real observed donor value from the *k* nearest by predictive score, within
  a caliper, excluding ineligible donors (`donors.pmm_draw`) — keeps imputations on real
  support and sidesteps `asinh` back-transform bias.
- **Three-axis provenance enums** (`RawInputState` / `AlignmentEvidence` / `HarnessAction`)
  and a **typed, fail-closed registry** (`registry.WealthVariable` +
  `fail_if_unresolved_required`): an `UNRESOLVED` entry that is `required_for_release` must
  raise.
- **Disclosure safety:** the probe (`probe.assemble_probe_report` / `task_probe.py`) may emit
  only registry keys, presence booleans, and numeric summaries — **never** row-level data.
- **Monetary values stay `float64`** (no smallest-float downcast); paths via `pathlib.Path`;
  RNG via `np.random.default_rng(seed=...)` only; Python ≥3.14 (no `from __future__ import
  annotations`); `# noqa: <RULE>` is acceptable for localized lint exceptions (repo
  convention).

## 4. Commits on the branch (`feat/wealth-imputation-harness`, off `main` @ 26ecc91)

```
5f39257 feat(wealth): Milestone-0 disclosure-safe probe report + pytask wiring
be8f7fe docs(plan): allow idiomatic # noqa lint suppressions (match repo convention)
208fbdd feat(wealth): predictive-mean-matching multi-donor draw
e6ccf97 feat(wealth): household plain-sum aggregation + PUNR residual
a3af3e3 feat(wealth): one-shot ownership-share resolution to person amounts
466828b feat(wealth): typed registry contract with fail-closed verification
819f2dd feat(wealth): canonical component ontology + provenance enums
41fe852 feat(wealth): asinh-scaled transform with round-trip inverse
f194d24 feat(wealth): scaffold wealth_imputation package + add scikit-learn
```

Status: `prek run --all-files` fully passes; 15/15 `tests/wealth_imputation/` pass; the
full project suite ran green under the (transitively bumped) pytask 0.6.0.

## 5. Focus for this review

1. **Cross-module coherence:** do the helper signatures/types/names compose into the
   pipeline the spec describes? Any drift that would bite the model/draw stage?
2. **Load-bearing invariants:** share-applied-once then plain-sum (no double-apply); PUNR
   residual sign/semantics; `component_sign`; PMM caliper + (future) exclusion hooks.
3. **Disclosure-safety** of `probe.py` / `task_probe.py` — can any value leak?
4. **Test quality:** real-behavior, concrete-value assertions; anything that asserts nothing
   or that mislabels what it tests.
5. **Foundation traps:** anything here that would silently mislead or block the deferred
   data-wiring / model / PUNR / validation work.

## 6. Known Minor findings — please triage (must-fix-before-merge vs defer)

- `tests/wealth_imputation/test_aggregate.py` — a residual assertion uses `==` on a float
  (project rule: use `math.isclose`). The value (50.0) is exact so it won't flake, but it
  violates the global constraint.
- `tests/wealth_imputation/test_donors.py` — the test named for "determinism under seed" is
  actually a *membership* test (`in {10.0, 11.0}`); it verifies k-nearest selection but does
  **not** pin the seeded draw, so it doesn't verify determinism.
- Cosmetic: `probe.py` builds a `list` local then re-iterates `entries` (could be one pass);
  test dtype coverage is float64 rather than pyarrow in a couple of places; a couple of test
  files lack a module docstring.

## 6a. Requested verdict

1. Ready to merge? **Yes | No | With fixes**, with reasoning.
2. Must-fix before merge (ranked, file:line).
3. Should-fix / defer-to-data-wiring.
4. Any invariant in Section 3 the code violates or only partially satisfies.
5. Any place a known finding (Section 6) is actually more severe than "Minor".

## 7. Code under review

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
fails closed via `fail_if_unresolved_required`.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from soep_preparation.wealth_imputation.components import CanonicalComponent


class VerificationStatus(Enum):
    """Whether a registry entry's variable mapping has been confirmed."""

    VERIFIED = "verified"
    INFERRED = "inferred"
    UNRESOLVED = "unresolved"


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
    sign: int
    """`+1` for assets, `-1` for liabilities."""
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
    aggregation_rule: str
    """How person items combine to the household (e.g. `sum_member_shares`)."""
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

import pandas as pd


def resolve_person_amount(
    joint_amount: pd.Series, ownership_share: pd.Series
) -> pd.Series:
    """Return the person's share of a jointly-held asset value.

    Args:
        joint_amount: Total value of the asset reported for the household/asset.
        ownership_share: The person's ownership share in `[0, 1]`; NA if unknown.

    Returns:
        `joint_amount * ownership_share`, with NA propagated where the share is NA.
    """
    return joint_amount * ownership_share
```

#### src/soep_preparation/wealth_imputation/aggregate.py

```python
"""Aggregate share-resolved person amounts to households and isolate PUNR residuals.

Household wealth is the plain sum of members' share-resolved person amounts plus a
household-level partial-unit-nonresponse (PUNR) residual — the part of the official
household total not covered by represented members.
"""

import pandas as pd


def household_component_sum(person_panel: pd.DataFrame) -> pd.DataFrame:
    """Sum share-resolved person amounts within household, year, and component.

    Args:
        person_panel: Columns `hh_id`, `survey_year`, `component`,
            `person_component_value`.

    Returns:
        Columns `hh_id`, `survey_year`, `component`, `member_sum`.

    """
    return (
        person_panel.groupby(["hh_id", "survey_year", "component"], observed=True)[
            "person_component_value"
        ]
        .sum()
        .reset_index(name="member_sum")
    )


def punr_residual(household_total: pd.Series, member_sum: pd.Series) -> pd.Series:
    """Return the household total minus the represented-member sum.

    Args:
        household_total: Official household component total.
        member_sum: Sum of represented members' share-resolved amounts.

    Returns:
        The residual to be attributed to partial-unit nonresponse.

    """
    return household_total - member_sum
```

#### src/soep_preparation/wealth_imputation/donors.py

```python
"""Predictive-mean-matching donor draws.

Each recipient borrows an observed amount from one of its `k` nearest donors in
predictive-score space, so imputed values stay on the real observed support and the
back-transformation of `asinh`-scale predictions never has to be computed analytically.
"""

import numpy as np


def pmm_draw(  # noqa: PLR0913
    recipient_scores: np.ndarray,
    donor_scores: np.ndarray,
    donor_values: np.ndarray,
    k: int,
    rng: np.random.Generator,
    caliper: float | None = None,
) -> np.ndarray:
    """Draw one near-donor observed value per recipient.

    Args:
        recipient_scores: Predictive scores for recipients, shape `(n_recipients,)`.
        donor_scores: Predictive scores for donors, shape `(n_donors,)`.
        donor_values: Observed values for donors, shape `(n_donors,)`.
        k: Number of nearest donors to sample from.
        rng: NumPy random generator.
        caliper: If set, the maximum allowed score distance to a donor.

    Returns:
        One drawn value per recipient, shape `(n_recipients,)`.

    Raises:
        ValueError: If a recipient has no donor within `caliper`.
    """
    drawn = np.empty(recipient_scores.shape[0], dtype=donor_values.dtype)
    for i, score in enumerate(recipient_scores):
        distances = np.abs(donor_scores - score)
        order = np.argsort(distances)[:k]
        if caliper is not None and distances[order[0]] > caliper:
            msg = f"No donor within caliper {caliper} for recipient {i}."
            raise ValueError(msg)
        chosen = rng.choice(order)
        drawn[i] = donor_values[chosen]
    return drawn
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


def test_asinh_scaled_rejects_nonpositive_scale():
    """Scale validation rejects zero and negative values."""
    with pytest.raises(ValueError, match="scale must be positive"):
        asinh_scaled(pd.Series([1.0]), scale=0.0)
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
    VerificationStatus,
    WealthVariable,
    fail_if_unresolved_required,
)


def _entry(*, status: VerificationStatus, required: bool) -> WealthVariable:
    return WealthVariable(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        wave=2017,
        source_file="pl",
        raw_variable="f0100a",
        concept="financial assets value",
        unit="euro",
        sign=1,
        universe="adults 17+",
        missing_codes=(-1, -2, -8),
        bracket_variable=None,
        ownership_flag=None,
        ownership_share_variable=None,
        level="person",
        aggregation_rule="sum_member_shares",
        expected_min=0.0,
        expected_max=None,
        verification_status=status,
        verification_evidence="codebook p.42",
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
```

#### tests/wealth_imputation/test_shares.py

```python
import pandas as pd

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
```

#### tests/wealth_imputation/test_aggregate.py

```python
import pandas as pd

from soep_preparation.wealth_imputation.aggregate import (
    household_component_sum,
    punr_residual,
)


def test_household_component_sum_sums_members_within_household_component() -> None:
    panel = pd.DataFrame(
        {
            "hh_id": [1, 1, 2],
            "survey_year": [2017, 2017, 2017],
            "component": ["financial_assets", "financial_assets", "financial_assets"],
            "person_component_value": [100.0, 50.0, 200.0],
        }
    )
    out = household_component_sum(panel)
    sums = dict(zip(out["hh_id"], out["member_sum"], strict=True))
    assert sums == {1: 150.0, 2: 200.0}


def test_punr_residual_is_zero_when_member_sum_equals_household_total() -> None:
    total = pd.Series([150.0, 200.0])
    member_sum = pd.Series([150.0, 200.0])
    pd.testing.assert_series_equal(
        punr_residual(total, member_sum),
        pd.Series([0.0, 0.0]),
        check_names=False,
    )


def test_punr_residual_is_positive_when_household_exceeds_member_sum() -> None:
    total = pd.Series([150.0])
    member_sum = pd.Series([100.0])
    expected_residual = 50.0
    assert punr_residual(total, member_sum).iloc[0] == expected_residual
```

#### tests/wealth_imputation/test_donors.py

```python
import numpy as np
import pytest

from soep_preparation.wealth_imputation.donors import pmm_draw


def test_pmm_draw_returns_a_near_donor_value_deterministic_under_seed():
    donor_scores = np.array([0.0, 1.0, 5.0, 10.0])
    donor_values = np.array([10.0, 11.0, 50.0, 100.0])
    recipient_scores = np.array([0.4])
    rng = np.random.default_rng(seed=0)
    drawn = pmm_draw(recipient_scores, donor_scores, donor_values, k=2, rng=rng)
    # nearest two donors to 0.4 are scores 0.0 and 1.0 -> values 10.0 or 11.0
    assert drawn[0] in {10.0, 11.0}


def test_pmm_draw_raises_when_no_donor_within_caliper():
    donor_scores = np.array([0.0, 1.0])
    donor_values = np.array([10.0, 11.0])
    recipient_scores = np.array([100.0])
    rng = np.random.default_rng(seed=0)
    with pytest.raises(ValueError, match="No donor within caliper"):
        pmm_draw(
            recipient_scores, donor_scores, donor_values, k=1, rng=rng, caliper=0.5
        )
```

#### tests/wealth_imputation/test_probe.py

```python
from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.probe import assemble_probe_report
from soep_preparation.wealth_imputation.registry import (
    VerificationStatus,
    WealthVariable,
)


def _entry(raw_variable: str, *, required: bool) -> WealthVariable:
    return WealthVariable(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        wave=2017,
        source_file="pl",
        raw_variable=raw_variable,
        concept="financial assets",
        unit="euro",
        sign=1,
        universe="adults 17+",
        missing_codes=(-1,),
        bracket_variable=None,
        ownership_flag=None,
        ownership_share_variable=None,
        level="person",
        aggregation_rule="sum_member_shares",
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
```
