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
