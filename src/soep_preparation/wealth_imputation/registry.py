"""Typed semantic contract for raw wealth variables.

Each `WealthVariable` declares one raw questionnaire item for one component in one
wave, together with the verification status that gates it. Production reads only
verified (or inferred) entries; an unresolved entry that is `required_for_release`
fails closed via `fail_if_unresolved_required`. The net-wealth sign is derived from
the canonical component (`net_sign`), never carried as a separate, contradictable
field.
"""

import math
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
    PERSON_DIRECT_PLAIN_SUM = "person_direct_plain_sum"
    """Per-person amount is already person-specific; household = plain sum, no share."""
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
        for name, bound in (
            ("expected_min", self.expected_min),
            ("expected_max", self.expected_max),
        ):
            if bound is not None and not math.isfinite(bound):
                msg = f"{name} must be finite, got {bound}."
                raise ValueError(msg)
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
        self._fail_if_aggregation_rule_inconsistent()

    def _fail_if_aggregation_rule_inconsistent(self) -> None:
        has_share = self.ownership_share_variable is not None
        if self.aggregation_rule is AggregationRule.HOUSEHOLD_DIRECT:
            if self.level != "household":
                msg = "HOUSEHOLD_DIRECT requires level='household'."
                raise ValueError(msg)
            if has_share:
                msg = "HOUSEHOLD_DIRECT must not set an ownership_share_variable."
                raise ValueError(msg)
        elif self.aggregation_rule is AggregationRule.PERSON_SHARE_THEN_PLAIN_SUM:
            if self.level != "person":
                msg = "PERSON_SHARE_THEN_PLAIN_SUM requires level='person'."
                raise ValueError(msg)
            if not has_share:
                msg = (
                    "PERSON_SHARE_THEN_PLAIN_SUM requires an ownership_share_variable."
                )
                raise ValueError(msg)
        elif self.aggregation_rule is AggregationRule.PERSON_DIRECT_PLAIN_SUM:
            if self.level != "person":
                msg = "PERSON_DIRECT_PLAIN_SUM requires level='person'."
                raise ValueError(msg)
            if has_share:
                msg = (
                    "PERSON_DIRECT_PLAIN_SUM must not set an ownership_share_variable."
                )
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
