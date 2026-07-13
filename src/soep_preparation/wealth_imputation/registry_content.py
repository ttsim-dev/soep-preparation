"""Registry entries for the SOEP wealth battery, one per component and survey wave.

The variable names are the DIW generated-wealth columns confirmed present in the
SOEP-Core V41 `pwealth` file by the Milestone-0 probe. SOEP-Core ships a *reduced*
wealth file: it carries the headline per-component imputed values and the overall
totals, but not the full DIW research breakdown documented in SOEP Survey Paper 272
(Grabka & Westermeier 2015). Each value column carries five implicate suffixes
`a`-`e`; the registry uses the `a` implicate as the probe representative of the
family.

Five components are available directly and identically across every wave:

- owner-occupied property, gross (`p0100`)
- financial assets (`f0100`)
- private insurances (`h0100`)
- vehicles (`v0100`)
- consumer debt (`c0100`)

Three canonical components are not separately sourceable from the SOEP-Core file and
are recorded as UNRESOLVED with evidence:

- owner-occupied mortgage: no separate debt column; derive as `p0100` - `p0110`.
- other real estate (`e0100`) and business assets (`b0100`): absent; subsumed in the
  overall totals `w0101`/`w0111`, separable only from the DIW full wealth file.

Verification status per `(component, wave)`:

- Available components, pre-2022: VERIFIED against the live V41 columns (probe).
- Available components, 2022: UNRESOLVED and release-critical -- DIW has not released
  the generated 2022 wealth module, so the harness must predict it.
  `fail_if_unresolved_required` blocks release until Milestone 0 resolves these.
- Unavailable components: UNRESOLVED in every wave, but not release-critical (the
  net-wealth target reads the overall totals directly).
"""

from dataclasses import dataclass

from soep_preparation.wealth_imputation.components import CanonicalComponent
from soep_preparation.wealth_imputation.registry import (
    AggregationRule,
    VerificationStatus,
    WealthVariable,
)

# `RAW_DATA_FILES` catalog key of the individual-level wealth file.
_SOURCE_FILE = "pwealth"

# Wave whose generated wealth module DIW has not released; the harness predicts it.
_PREDICTION_WAVE = 2022

# SOEP wealth-module waves (a five-year cycle).
_WAVES: tuple[int, ...] = (2002, 2007, 2012, 2017, _PREDICTION_WAVE)

_EV_PROBE = (
    "Confirmed present in SOEP-Core V41 pwealth.dta by the Milestone-0 probe "
    "(observed_columns)."
)
_EV_2022 = (
    "DIW has not released the generated 2022 wealth module; the 2022 values cannot "
    "be confirmed and the harness must predict this component. Resolve at Milestone "
    "0 / on DIW release."
)
_EV_ABSENT_MORTGAGE = (
    "No separate mortgage-debt column in SOEP-Core V41 pwealth.dta (Milestone-0 "
    "probe). Owner-occupied NET value p0110 is available directly, so the net-wealth "
    "target does not require sourcing mortgage independently (mortgage = p0100 - "
    "p0110)."
)
_EV_ABSENT_FOLDED = (
    "Absent in SOEP-Core V41 pwealth.dta (Milestone-0 probe): the reduced SOEP-Core "
    "wealth file carries only headline components and overall totals. This component "
    "is subsumed in the overall totals w0101/w0111 and would require the DIW full "
    "wealth research file to separate."
)

_SHARE = AggregationRule.PERSON_SHARE_THEN_PLAIN_SUM
_DIRECT = AggregationRule.PERSON_DIRECT_PLAIN_SUM


@dataclass(frozen=True)
class _ComponentSpec:
    """Wave-invariant attributes of one wealth component at the individual level."""

    component: CanonicalComponent
    """Canonical component this item contributes to."""
    concept: str
    """Questionnaire concept in plain language."""
    universe: str
    """Population the item applies to (the filter)."""
    raw_variable: str
    """Implicate-`a` value column (the expected name even when absent)."""
    ownership_flag: str
    """Ownership filter variable in the DIW research file."""
    ownership_share_variable: str | None
    """Per-person ownership-share variable, or `None` for person-direct items."""
    aggregation_rule: AggregationRule
    """How person values combine into the household total."""
    missing_codes: tuple[int, ...]
    """Negative SOEP codes treated as missing for this item."""
    available_in_v41: bool
    """Whether the value column is present in the SOEP-Core V41 `pwealth` file."""
    absent_evidence: str = ""
    """Evidence explaining the gap when `available_in_v41` is `False`."""
    note: str = ""
    """Extra evidence note appended to every entry for this component."""


_COMPONENT_SPECS: tuple[_ComponentSpec, ...] = (
    _ComponentSpec(
        component=CanonicalComponent.OWNER_OCCUPIED_PROPERTY_GROSS,
        concept="Gross market value of owner-occupied primary residence "
        "(DIW-imputed, implicates a-e).",
        universe="Persons owning a share of their primary residence.",
        raw_variable="p0100a",
        ownership_flag="p10000",
        ownership_share_variable="p00010",
        aggregation_rule=_SHARE,
        missing_codes=(),
        available_in_v41=True,
    ),
    _ComponentSpec(
        component=CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        concept="Outstanding mortgage debt on the owner-occupied primary residence "
        "(derived as gross minus net market value).",
        universe="Persons with mortgage debt on their primary residence.",
        raw_variable="p0010a",
        ownership_flag="p10000",
        ownership_share_variable="p00010",
        aggregation_rule=_SHARE,
        missing_codes=(),
        available_in_v41=False,
        absent_evidence=_EV_ABSENT_MORTGAGE,
    ),
    _ComponentSpec(
        component=CanonicalComponent.OTHER_REAL_ESTATE,
        concept="Gross market value of other real estate "
        "(DIW-imputed, implicates a-e).",
        universe="Persons owning a share of other real estate.",
        raw_variable="e0100a",
        ownership_flag="e10000",
        ownership_share_variable="e00010",
        aggregation_rule=_SHARE,
        missing_codes=(),
        available_in_v41=False,
        absent_evidence=_EV_ABSENT_FOLDED,
    ),
    _ComponentSpec(
        component=CanonicalComponent.FINANCIAL_ASSETS,
        concept="Market value of financial assets (DIW-imputed, implicates a-e).",
        universe="Persons holding a share of financial assets.",
        raw_variable="f0100a",
        ownership_flag="f10000",
        ownership_share_variable="f00010",
        aggregation_rule=_SHARE,
        missing_codes=(),
        available_in_v41=True,
    ),
    _ComponentSpec(
        component=CanonicalComponent.PRIVATE_PENSION,
        concept="Market value of private insurances and building-loan contracts "
        "(DIW-imputed, implicates a-e).",
        universe="Persons holding private insurances or building-loan contracts.",
        raw_variable="h0100a",
        ownership_flag="h10000",
        ownership_share_variable=None,
        aggregation_rule=_DIRECT,
        missing_codes=(),
        available_in_v41=True,
        note="The reduced SOEP-Core file carries private insurances as `h0100` in "
        "every wave; SP272's i0100/h0100/l0100 split applies to the full research "
        "file only.",
    ),
    _ComponentSpec(
        component=CanonicalComponent.BUSINESS_ASSETS,
        concept="Market value of business assets (DIW-imputed, implicates a-e).",
        universe="Persons owning business assets.",
        raw_variable="b0100a",
        ownership_flag="b10000",
        ownership_share_variable=None,
        aggregation_rule=_DIRECT,
        missing_codes=(),
        available_in_v41=False,
        absent_evidence=_EV_ABSENT_FOLDED,
    ),
    _ComponentSpec(
        component=CanonicalComponent.VEHICLES,
        concept="Market value of vehicles / tangible assets "
        "(DIW-imputed, implicates a-e).",
        universe="Persons owning vehicles or other tangible assets.",
        raw_variable="v0100a",
        ownership_flag="v10000",
        ownership_share_variable=None,
        aggregation_rule=_DIRECT,
        missing_codes=(-8,),
        available_in_v41=True,
        note="SOEP V41 uses `v0100`; SP272 lists this under the historical "
        "tangible-assets name `t0100`.",
    ),
    _ComponentSpec(
        component=CanonicalComponent.CONSUMER_DEBT,
        concept="Outstanding consumer credit / debt (DIW-imputed, implicates a-e).",
        universe="Persons with consumer credit or debt.",
        raw_variable="c0100a",
        ownership_flag="c10000",
        ownership_share_variable=None,
        aggregation_rule=_DIRECT,
        missing_codes=(),
        available_in_v41=True,
    ),
)


def _verification(
    spec: _ComponentSpec, wave: int
) -> tuple[VerificationStatus, str, str]:
    """Return the (status, evidence, codebook_version) for one `(component, wave)`."""
    extra = f" {spec.note}" if spec.note else ""
    if not spec.available_in_v41:
        return (
            VerificationStatus.UNRESOLVED,
            spec.absent_evidence + extra,
            "SOEP-Core v41 (absent)",
        )
    if wave == _PREDICTION_WAVE:
        return (
            VerificationStatus.UNRESOLVED,
            _EV_2022 + extra,
            "SOEP-Core v41 (2022 wealth module unreleased)",
        )
    return VerificationStatus.VERIFIED, _EV_PROBE + extra, "SOEP-Core v41"


def _build_entry(spec: _ComponentSpec, wave: int) -> WealthVariable:
    status, evidence, codebook_version = _verification(spec, wave)
    return WealthVariable(
        component=spec.component,
        wave=wave,
        source_file=_SOURCE_FILE,
        raw_variable=spec.raw_variable,
        concept=spec.concept,
        unit="euro",
        universe=spec.universe,
        missing_codes=spec.missing_codes,
        bracket_variable=None,
        ownership_flag=spec.ownership_flag,
        ownership_share_variable=spec.ownership_share_variable,
        level="person",
        aggregation_rule=spec.aggregation_rule,
        expected_min=None,
        expected_max=None,
        verification_status=status,
        verification_evidence=evidence,
        codebook_version=codebook_version,
        required_for_release=spec.available_in_v41,
    )


REGISTRY_ENTRIES: tuple[WealthVariable, ...] = tuple(
    _build_entry(spec, wave) for spec in _COMPONENT_SPECS for wave in _WAVES
)
