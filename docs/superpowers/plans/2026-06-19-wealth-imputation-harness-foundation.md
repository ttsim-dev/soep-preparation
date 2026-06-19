# Wealth-Imputation Harness — Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the code-only foundation of the SOEP wealth-imputation harness — package
scaffolding, the typed registry contract, pure numeric/logic helpers, and the Milestone-0
probe's report assembly — all unit-tested on synthetic fixtures with no access to real data.

**Architecture:** A new sibling subpackage `src/soep_preparation/wealth_imputation/`
consuming the existing `RAW_DATA_FILES`/`MODULES` catalogs. This plan delivers the pure,
data-independent core: enums for the canonical wealth ontology and three-axis provenance,
a frozen-dataclass registry entry with fail-closed verification, and pure helpers
(`asinh` transform, share resolution, plain-sum household aggregation, PUNR residual, PMM
donor draw, probe-report assembly). Data-wired registry population and the model/draw stage
are later plans gated on Milestone 0.

**Tech Stack:** Python 3.14, pandas ≥3.0 (pyarrow dtypes), numpy, scikit-learn (new),
pytask, pytest, ty, ruff.

## Global Constraints

- Python ≥ 3.14; no `from __future__ import annotations`.
- Type hints mandatory in every signature; `X | None` not `Optional`; `collections.abc`
  for abstract types.
- Google-style docstrings, imperative summaries; describe current state, no history.
- Immutability: `@dataclass(frozen=True)`, `tuple`/`Mapping` over `list`/`dict` for stored
  state; `Enum` for categoricals (never string-literal flags).
- Paths via `pathlib.Path`; float compare via `math.isclose`/`np.isclose`; RNG via
  `np.random.default_rng(seed=...)` only.
- pyarrow-backed dtypes for data columns; monetary outputs stay `float64` (no
  smallest-float downcast).
- Run everything through pixi: `pixi run -e py314 tests`, `pixi run ty`,
  `prek run --all-files`. Re-lock (`pixi lock`) and stage `pixi.lock` in the same commit as
  any `pyproject.toml` change.
- No `# pragma`; suppress types only with `# ty: ignore[rule-name]`.
- TDD throughout: failing test first, watch it fail, minimal implementation, commit.

---

### Task 1: Package scaffolding + scikit-learn dependency

**Files:**
- Create: `src/soep_preparation/wealth_imputation/__init__.py`
- Create: `tests/wealth_imputation/__init__.py`
- Modify: `pyproject.toml` (add `scikit-learn` dependency)
- Modify: `pixi.lock` (regenerated)

**Interfaces:**
- Produces: the importable package `soep_preparation.wealth_imputation`.

- [ ] **Step 1: Create the package `__init__.py`**

```python
"""Provisional SOEP wealth-imputation harness (2022 household-wealth proxy)."""
```

- [ ] **Step 2: Create the test package marker**

`tests/wealth_imputation/__init__.py` — empty file.

- [ ] **Step 3: Add scikit-learn**

Run: `pixi add scikit-learn`
Then: `pixi lock`
Expected: `pyproject.toml` gains `scikit-learn` under dependencies; `pixi.lock` updated.

- [ ] **Step 4: Verify the package imports**

Run: `pixi run -e py314 python -c "import soep_preparation.wealth_imputation"`
Expected: no output, exit 0.

- [ ] **Step 5: Commit**

```bash
git add src/soep_preparation/wealth_imputation/__init__.py tests/wealth_imputation/__init__.py pyproject.toml pixi.lock
git commit -m "feat(wealth): scaffold wealth_imputation package + add scikit-learn"
```

---

### Task 2: `asinh` transform helpers

**Files:**
- Create: `src/soep_preparation/wealth_imputation/transforms.py`
- Test: `tests/wealth_imputation/test_transforms.py`

**Interfaces:**
- Produces:
  `asinh_scaled(values: pd.Series, scale: float) -> pd.Series`,
  `inverse_asinh_scaled(transformed: pd.Series, scale: float) -> pd.Series`.
  Round-trip identity: `inverse_asinh_scaled(asinh_scaled(y, s), s) == y`.

- [ ] **Step 1: Write the failing round-trip test**

```python
import numpy as np
import pandas as pd

from soep_preparation.wealth_imputation.transforms import (
    asinh_scaled,
    inverse_asinh_scaled,
)


def test_asinh_scaled_round_trip_recovers_values_including_zero_and_negatives():
    values = pd.Series([-50000.0, 0.0, 1234.5, 1_000_000.0])
    restored = inverse_asinh_scaled(asinh_scaled(values, scale=10_000.0), scale=10_000.0)
    np.testing.assert_allclose(restored.to_numpy(), values.to_numpy(), atol=1e-4)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_transforms.py -v`
Expected: FAIL with `ModuleNotFoundError` / `ImportError` (transforms not defined).

- [ ] **Step 3: Write minimal implementation**

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

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_transforms.py -v`
Expected: PASS.

- [ ] **Step 5: Add the scale-guard test**

```python
import pytest


def test_asinh_scaled_rejects_nonpositive_scale():
    with pytest.raises(ValueError):
        asinh_scaled(pd.Series([1.0]), scale=0.0)
```

Run: `pixi run -e py314 tests tests/wealth_imputation/test_transforms.py -v` → PASS.

- [ ] **Step 6: Commit**

```bash
git add src/soep_preparation/wealth_imputation/transforms.py tests/wealth_imputation/test_transforms.py
git commit -m "feat(wealth): asinh-scaled transform with round-trip inverse"
```

---

### Task 3: Canonical ontology + three-axis provenance enums

**Files:**
- Create: `src/soep_preparation/wealth_imputation/components.py`
- Test: `tests/wealth_imputation/test_components.py`

**Interfaces:**
- Produces enums:
  - `CanonicalComponent`: `OWNER_OCCUPIED_PROPERTY_GROSS`, `OWNER_OCCUPIED_MORTGAGE`,
    `OTHER_REAL_ESTATE`, `FINANCIAL_ASSETS`, `PRIVATE_PENSION`, `BUSINESS_ASSETS`,
    `VEHICLES`, `CONSUMER_DEBT`.
  - `RawInputState`: `OBSERVED_COMPLETE`, `MISSING_SHARE`, `MISSING_AMOUNT`,
    `MISSING_FILTER`, `PUNR`, `STRUCTURAL_ABSENT`, `INCONSISTENT`, `AMBIGUOUS`.
  - `AlignmentEvidence`: `AGREES_ALL`, `AGREES_MEAN`, `OFFICIAL_DIFFERS`, `FLAG_CONFLICT`,
    `UNAVAILABLE`.
  - `HarnessAction`: `PRESERVED`, `DETERMINISTIC_EDIT`, `OWNERSHIP_IMPUTED`, `AMOUNT_PMM`,
    `SHARE_IMPUTED`, `PUNR_RESIDUAL`, `STATISTICAL_REPLACEMENT`, `SUPPRESSED`.
  - `LIABILITY_COMPONENTS: frozenset[CanonicalComponent]` =
    `{OWNER_OCCUPIED_MORTGAGE, CONSUMER_DEBT}`.
  - `component_sign(component) -> int` returning `-1` for liabilities else `+1`.

- [ ] **Step 1: Write the failing test**

```python
from soep_preparation.wealth_imputation.components import (
    CanonicalComponent,
    LIABILITY_COMPONENTS,
    component_sign,
)


def test_component_sign_is_negative_for_liabilities_positive_for_assets():
    assert component_sign(CanonicalComponent.CONSUMER_DEBT) == -1
    assert component_sign(CanonicalComponent.OWNER_OCCUPIED_MORTGAGE) == -1
    assert component_sign(CanonicalComponent.FINANCIAL_ASSETS) == 1


def test_liability_components_are_exactly_mortgage_and_consumer_debt():
    assert LIABILITY_COMPONENTS == frozenset(
        {
            CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
            CanonicalComponent.CONSUMER_DEBT,
        }
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_components.py -v`
Expected: FAIL (`ImportError`).

- [ ] **Step 3: Write minimal implementation**

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

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_components.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/soep_preparation/wealth_imputation/components.py tests/wealth_imputation/test_components.py
git commit -m "feat(wealth): canonical component ontology + provenance enums"
```

---

### Task 4: Registry semantic contract with fail-closed verification

**Files:**
- Create: `src/soep_preparation/wealth_imputation/registry.py`
- Test: `tests/wealth_imputation/test_registry.py`

**Interfaces:**
- Consumes: `CanonicalComponent` (Task 3).
- Produces:
  - `VerificationStatus(Enum)`: `VERIFIED`, `INFERRED`, `UNRESOLVED`.
  - `WealthVariable` frozen dataclass with fields:
    `component: CanonicalComponent`, `wave: int`, `source_file: str`,
    `raw_variable: str`, `concept: str`, `unit: str`, `sign: int`, `universe: str`,
    `missing_codes: tuple[int, ...]`, `bracket_variable: str | None`,
    `ownership_flag: str | None`, `ownership_share_variable: str | None`,
    `level: Literal["person", "household"]`, `aggregation_rule: str`,
    `expected_min: float | None`, `expected_max: float | None`,
    `verification_status: VerificationStatus`, `verification_evidence: str`,
    `codebook_version: str`, `required_for_release: bool`.
  - `fail_if_unresolved_required(entries: Sequence[WealthVariable]) -> None` — raises
    `ValueError` if any `required_for_release` entry is `UNRESOLVED`.

- [ ] **Step 1: Write the failing tests**

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
    with pytest.raises(ValueError):
        fail_if_unresolved_required(entries)


def test_fail_if_unresolved_required_allows_unresolved_optional_entry():
    entries = [_entry(status=VerificationStatus.UNRESOLVED, required=False)]
    fail_if_unresolved_required(entries)  # no raise


def test_wealth_variable_is_frozen():
    entry = _entry(status=VerificationStatus.VERIFIED, required=True)
    with pytest.raises(AttributeError):
        entry.raw_variable = "x"  # ty: ignore[invalid-assignment]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_registry.py -v`
Expected: FAIL (`ImportError`).

- [ ] **Step 3: Write minimal implementation**

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

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_registry.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/soep_preparation/wealth_imputation/registry.py tests/wealth_imputation/test_registry.py
git commit -m "feat(wealth): typed registry contract with fail-closed verification"
```

---

### Task 5: Share resolution (raw joint amount → person amount)

**Files:**
- Create: `src/soep_preparation/wealth_imputation/shares.py`
- Test: `tests/wealth_imputation/test_shares.py`

**Interfaces:**
- Produces:
  `resolve_person_amount(joint_amount: pd.Series, ownership_share: pd.Series) -> pd.Series`
  — multiplies the asset's total value by the person's ownership share **exactly once**,
  yielding the per-person amount. Shares are expected in `[0, 1]`; a NA share with a
  present amount yields NA (caller routes it to `MISSING_SHARE`).

- [ ] **Step 1: Write the failing test**

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

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_shares.py -v`
Expected: FAIL (`ImportError`).

- [ ] **Step 3: Write minimal implementation**

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

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_shares.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/soep_preparation/wealth_imputation/shares.py tests/wealth_imputation/test_shares.py
git commit -m "feat(wealth): one-shot ownership-share resolution to person amounts"
```

---

### Task 6: Household plain-sum aggregation + PUNR residual

**Files:**
- Create: `src/soep_preparation/wealth_imputation/aggregate.py`
- Test: `tests/wealth_imputation/test_aggregate.py`

**Interfaces:**
- Consumes: `CanonicalComponent` (Task 3).
- Produces:
  - `household_component_sum(person_panel: pd.DataFrame) -> pd.DataFrame` — sums
    `person_component_value` over members within `hh_id × survey_year × component`.
    Input columns: `hh_id`, `survey_year`, `component`, `person_component_value`.
    Output columns: `hh_id`, `survey_year`, `component`, `member_sum`.
  - `punr_residual(household_total: pd.Series, member_sum: pd.Series) -> pd.Series` —
    `household_total - member_sum`, aligned by index.

- [ ] **Step 1: Write the failing tests**

```python
import pandas as pd

from soep_preparation.wealth_imputation.aggregate import (
    household_component_sum,
    punr_residual,
)


def test_household_component_sum_sums_members_within_household_component():
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


def test_punr_residual_is_zero_when_member_sum_equals_household_total():
    total = pd.Series([150.0, 200.0])
    member_sum = pd.Series([150.0, 200.0])
    pd.testing.assert_series_equal(
        punr_residual(total, member_sum),
        pd.Series([0.0, 0.0]),
        check_names=False,
    )


def test_punr_residual_is_positive_when_household_exceeds_member_sum():
    total = pd.Series([150.0])
    member_sum = pd.Series([100.0])
    assert punr_residual(total, member_sum).iloc[0] == 50.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_aggregate.py -v`
Expected: FAIL (`ImportError`).

- [ ] **Step 3: Write minimal implementation**

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
    grouped = (
        person_panel.groupby(["hh_id", "survey_year", "component"], observed=True)[
            "person_component_value"
        ]
        .sum()
        .reset_index(name="member_sum")
    )
    return grouped


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

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_aggregate.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/soep_preparation/wealth_imputation/aggregate.py tests/wealth_imputation/test_aggregate.py
git commit -m "feat(wealth): household plain-sum aggregation + PUNR residual"
```

---

### Task 7: PMM multi-donor draw

**Files:**
- Create: `src/soep_preparation/wealth_imputation/donors.py`
- Test: `tests/wealth_imputation/test_donors.py`

**Interfaces:**
- Produces:
  `pmm_draw(recipient_scores, donor_scores, donor_values, k, rng, caliper=None) ->
  np.ndarray` — for each recipient, pick the `k` donors nearest in predictive score
  (within `caliper` if given) and randomly draw one donor's observed value. `rng` is a
  `numpy.random.Generator`. Raises `ValueError` if no donor is within `caliper`.

- [ ] **Step 1: Write the failing tests**

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
    with pytest.raises(ValueError):
        pmm_draw(
            recipient_scores, donor_scores, donor_values, k=1, rng=rng, caliper=0.5
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_donors.py -v`
Expected: FAIL (`ImportError`).

- [ ] **Step 3: Write minimal implementation**

```python
"""Predictive-mean-matching donor draws.

Each recipient borrows an observed amount from one of its `k` nearest donors in
predictive-score space, so imputed values stay on the real observed support and the
back-transformation of `asinh`-scale predictions never has to be computed analytically.
"""

import numpy as np


def pmm_draw(
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

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_donors.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/soep_preparation/wealth_imputation/donors.py tests/wealth_imputation/test_donors.py
git commit -m "feat(wealth): predictive-mean-matching multi-donor draw"
```

---

### Task 8: Milestone-0 probe report assembly (pure) + pytask wiring

**Files:**
- Create: `src/soep_preparation/wealth_imputation/probe.py`
- Create: `src/soep_preparation/wealth_imputation/task_probe.py`
- Test: `tests/wealth_imputation/test_probe.py`

**Interfaces:**
- Consumes: `WealthVariable`, `VerificationStatus` (Task 4).
- Produces:
  - `assemble_probe_report(entries, available_columns) -> dict` — pure. `entries` is a
    `Sequence[WealthVariable]`; `available_columns` is a
    `Mapping[str, frozenset[str]]` (source file → its column names). Returns a
    disclosure-safe dict: per entry `{component, wave, source_file, raw_variable,
    present: bool, verification_status}`, plus a `summary` with `n_entries`,
    `n_present`, `n_missing`, and `n_unresolved_required`. **No row-level data.**
  - `task_probe.py` — a pytask task reading `RAW_DATA_FILES` keys' columns and writing the
    report JSON to `BLD`. (Runs on gpu-01; not unit-tested with real data.)

- [ ] **Step 1: Write the failing test (pure assembly only)**

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
    entries = [_entry("present_var", required=True), _entry("absent_var", required=True)]
    available = {"pl": frozenset({"present_var", "other"})}
    report = assemble_probe_report(entries, available)
    assert report["summary"]["n_present"] == 1
    assert report["summary"]["n_missing"] == 1
    assert report["summary"]["n_unresolved_required"] == 2
    presence = {row["raw_variable"]: row["present"] for row in report["entries"]}
    assert presence == {"present_var": True, "absent_var": False}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_probe.py -v`
Expected: FAIL (`ImportError`).

- [ ] **Step 3: Write the pure assembly implementation**

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

- [ ] **Step 4: Run test to verify it passes**

Run: `pixi run -e py314 tests tests/wealth_imputation/test_probe.py -v`
Expected: PASS.

- [ ] **Step 5: Write the pytask wiring (no unit test; runs on gpu-01)**

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

Note: this imports `REGISTRY_ENTRIES` from `registry_content.py`, which is populated in
the follow-on data-wiring plan. Until then, create a minimal
`registry_content.py` exposing `REGISTRY_ENTRIES: tuple[WealthVariable, ...] = ()` so the
task is importable; the probe then reports zero entries.

- [ ] **Step 6: Create the placeholder registry-content module**

```python
"""Registry entries for the raw wealth battery (populated in the data-wiring plan)."""

from soep_preparation.wealth_imputation.registry import WealthVariable

REGISTRY_ENTRIES: tuple[WealthVariable, ...] = ()
```

- [ ] **Step 7: Run the full new test module + ty**

Run: `pixi run -e py314 tests tests/wealth_imputation/ -v`
Expected: PASS.
Run: `pixi run ty`
Expected: no new errors.

- [ ] **Step 8: Commit**

```bash
git add src/soep_preparation/wealth_imputation/probe.py src/soep_preparation/wealth_imputation/task_probe.py src/soep_preparation/wealth_imputation/registry_content.py tests/wealth_imputation/test_probe.py
git commit -m "feat(wealth): Milestone-0 disclosure-safe probe report + pytask wiring"
```

---

### Task 9: Quality gate + branch push

**Files:** none (verification only).

- [ ] **Step 1: Run the full quality suite**

Run: `prek run --all-files`
Expected: all hooks pass (ruff, ty, formatting, codespell, mdformat).

- [ ] **Step 2: Run the project test subset**

Run: `pixi run -e py314 tests tests/wealth_imputation/ -n 7`
Expected: all pass.

- [ ] **Step 3: Push the branch**

```bash
git push -u origin feat/wealth-imputation-harness
```

---

## Out of scope (follow-on plans, gated on Milestone 0)

- **Data wiring:** populate `registry_content.py` from the V41 codebooks (verification
  status per entry); run the Milestone-0 probe on gpu-01; build the component-wave evidence
  matrix; resolve the open data gates (sample P/`psample`, person/household weights, 2002
  semantics, imputation-method confirmation).
- **Raw-state + editing:** three-axis provenance classification on real columns;
  deterministic editing; observed-gold construction.
- **Eligible-person roster + PUNR completion layer** on real data.
- **Model + draw stage:** HGB propensity + amount models, chained components, bootstrap +
  PMM replicates, interval calibration, validation (rolling-origin, masking, four metric
  blocks), 2022 provisional output. (Spec phases 5–9.)

## Self-Review

- **Spec coverage (foundation slice):** asinh transform ✓ (Task 2); canonical ontology +
  three-axis provenance enums ✓ (Task 3); typed fail-closed registry ✓ (Task 4); one-shot
  share resolution ✓ (Task 5); plain-sum aggregation + PUNR residual ✓ (Task 6); PMM
  multi-donor draw ✓ (Task 7); disclosure-safe Milestone-0 probe ✓ (Task 8). Model/draw,
  masking, roster, editing, registry population are explicitly deferred above.
- **Placeholder scan:** none — every code step shows complete code. `registry_content.py`
  ships as an empty tuple by design (Task 8 Step 6), not a placeholder.
- **Type consistency:** `WealthVariable` field names/types are identical across Tasks 4 and
  8; `assemble_probe_report` signature matches its test and the task wiring;
  `pmm_draw`/`resolve_person_amount`/`household_component_sum` signatures match their tests.
