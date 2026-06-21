"""Registry entries for the SOEP wealth battery, one per component and survey wave.

Variable names follow the DIW generated-wealth scheme documented in SOEP Survey
Paper 272 (Grabka & Westermeier 2015), Sec. 7.1 (individual level). Each generated
market-value variable carries five implicate suffixes `a`-`e`; the registry uses the
`a` implicate as the probe representative of the `a`-`e` family.

The DIW wealth files are long-format with one stable column per component across all
survey years, so a component uses the *same* variable in every wave. The single
exception is private provision: before 2007 insurances and building-loan contracts
are combined in `i0100`; from 2007 they split into private insurances (`h0100`) and
building-loan contracts (`l0100`). The `PRIVATE_PENSION` component maps to the
insurances measure available in each wave (`i0100` in 2002, `h0100` from 2007).

Verification status is assigned per `(component, wave)`:

- Pre-2022 names read by `clean_modules/pwealth.py` are VERIFIED against the live
  SOEP V41 columns. This is how vehicles is confirmed as `v0100`, which SP272 still
  lists under its historical tangible-assets name `t0100`.
- Other 2002/2007/2012 names are VERIFIED against SP272 Sec. 7.1.
- Remaining pre-2022 names are INFERRED from the wave-invariant convention.
- 2022 is UNRESOLVED and release-critical: DIW has not released the generated 2022
  wealth module, so the column cannot be confirmed and the harness must predict the
  component. `fail_if_unresolved_required` blocks release until Milestone 0 resolves
  these against SOEP V41.
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

# First wealth-module wave; private provision is still combined (`i0100`).
_FIRST_WAVE = 2002

# Wave whose generated wealth module DIW has not released; the harness predicts it.
_PREDICTION_WAVE = 2022

# SOEP wealth-module waves (a five-year cycle).
_WAVES: tuple[int, ...] = (_FIRST_WAVE, 2007, 2012, 2017, _PREDICTION_WAVE)

# Waves whose variable names SP272 Sec. 7.1 documents directly.
_SP272_WAVES = frozenset({2002, 2007, 2012})

# Implicate-`a` value columns read by `clean_modules/pwealth.py` against SOEP V41.
_PWEALTH_V41_COLUMNS = frozenset({"p0100a", "f0100a", "h0100a", "c0100a", "v0100a"})

_EV_SP272 = (
    "SOEP Survey Paper 272 (Grabka & Westermeier 2015), Sec. 7.1 individual-level "
    "variable list."
)
_EV_PWEALTH = (
    "Read in soep_preparation/clean_modules/pwealth.py against SOEP V41 "
    "(long-format column; wave-invariant)."
)
_EV_INFERRED = (
    "DIW wealth files are long-format with wave-invariant column names (SP272 "
    "Sec. 7.1); not separately documented for this wave. Confirm presence via the "
    "Milestone-0 probe on SOEP V41."
)
_EV_2022 = (
    "DIW has not released the generated 2022 wealth module; the column cannot be "
    "confirmed and the harness must predict this component for 2022. Resolve at "
    "Milestone 0 / on DIW release."
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
    """Implicate-`a` value column from 2007 onward."""
    ownership_flag: str
    """Ownership filter variable from 2007 onward."""
    ownership_share_variable: str | None
    """Per-person ownership-share variable, or `None` for person-direct items."""
    aggregation_rule: AggregationRule
    """How person values combine into the household total."""
    missing_codes: tuple[int, ...]
    """Negative SOEP codes treated as missing for this item."""
    raw_variable_2002: str | None = None
    """Value column override for 2002, when it differs from later waves."""
    ownership_flag_2002: str | None = None
    """Ownership filter override for 2002, when it differs from later waves."""
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
    ),
    _ComponentSpec(
        component=CanonicalComponent.OWNER_OCCUPIED_MORTGAGE,
        concept="Outstanding mortgage debt on the owner-occupied primary residence "
        "(DIW-imputed, implicates a-e).",
        universe="Persons with mortgage debt on their primary residence.",
        raw_variable="p0010a",
        ownership_flag="p10000",
        ownership_share_variable="p00010",
        aggregation_rule=_SHARE,
        missing_codes=(),
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
        raw_variable_2002="i0100a",
        ownership_flag_2002="i10000",
        note="Before 2007 insurances and building-loan contracts are combined "
        "(`i0100`); from 2007 the component maps to private insurances (`h0100`), "
        "excluding the separate building-loan column `l0100`.",
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
        note="SP272 Sec. 7.1 lists this under the historical tangible-assets name "
        "`t0100`; SOEP V41 uses `v0100` (see pwealth.py).",
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
    ),
)


def _raw_variable_for_wave(spec: _ComponentSpec, wave: int) -> str:
    if wave == _FIRST_WAVE and spec.raw_variable_2002 is not None:
        return spec.raw_variable_2002
    return spec.raw_variable


def _ownership_flag_for_wave(spec: _ComponentSpec, wave: int) -> str:
    if wave == _FIRST_WAVE and spec.ownership_flag_2002 is not None:
        return spec.ownership_flag_2002
    return spec.ownership_flag


def _verification(
    spec: _ComponentSpec,
    wave: int,
    raw_variable: str,
) -> tuple[VerificationStatus, str, str]:
    """Return the (status, evidence, codebook_version) for one `(component, wave)`."""
    extra = f" {spec.note}" if spec.note else ""
    if wave == _PREDICTION_WAVE:
        return (
            VerificationStatus.UNRESOLVED,
            _EV_2022 + extra,
            "SOEP-Core v41 (2022 wealth module unreleased)",
        )
    if raw_variable in _PWEALTH_V41_COLUMNS:
        return VerificationStatus.VERIFIED, _EV_PWEALTH + extra, "SOEP-Core v41"
    if wave in _SP272_WAVES:
        return (
            VerificationStatus.VERIFIED,
            _EV_SP272 + extra,
            "SOEP Survey Paper 272 (2015)",
        )
    return VerificationStatus.INFERRED, _EV_INFERRED + extra, "SOEP-Core v41"


def _build_entry(spec: _ComponentSpec, wave: int) -> WealthVariable:
    raw_variable = _raw_variable_for_wave(spec, wave)
    status, evidence, codebook_version = _verification(spec, wave, raw_variable)
    return WealthVariable(
        component=spec.component,
        wave=wave,
        source_file=_SOURCE_FILE,
        raw_variable=raw_variable,
        concept=spec.concept,
        unit="euro",
        universe=spec.universe,
        missing_codes=spec.missing_codes,
        bracket_variable=None,
        ownership_flag=_ownership_flag_for_wave(spec, wave),
        ownership_share_variable=spec.ownership_share_variable,
        level="person",
        aggregation_rule=spec.aggregation_rule,
        expected_min=None,
        expected_max=None,
        verification_status=status,
        verification_evidence=evidence,
        codebook_version=codebook_version,
        required_for_release=True,
    )


REGISTRY_ENTRIES: tuple[WealthVariable, ...] = tuple(
    _build_entry(spec, wave) for spec in _COMPONENT_SPECS for wave in _WAVES
)
