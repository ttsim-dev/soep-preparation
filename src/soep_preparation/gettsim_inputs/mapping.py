"""Curated mapping from SOEP final variables to GETTSIM nodes, resolved by policy date.

GETTSIM consumes data as a flat table whose column labels are double-underscore
qualified names (qnames) such as `wohnen__wohnfläche_hh`. A SOEP variable can stand in
for two kinds of GETTSIM node:

- a **root input** (declared with `@policy_input()`) that GETTSIM cannot compute and
  must be supplied; and
- an internally **computed** node, where supplying data *overrides* GETTSIM's
  calculation and prunes that node's subtree (GETTSIM warns about the override unless
  asked not to). Listing computed nodes follows the rule that a mapping may name *any*
  GETTSIM node a SOEP variable reasonably approximates — not only the root inputs.

`_BASE` is the date-invariant table: it pairs each candidate GETTSIM qname with the SOEP
final variable that supplies it, or `None` where no clean source exists (the reason is
in `GAP_NOTES`). The pairing does not change with the policy date — GETTSIM's qname
namespace is stable; nodes only activate and deactivate. What *does* change by date is
**which** of these qnames are part of GETTSIM at all: rules expire (Erziehungsgeld
before 2007, Altersrente für Frauen from 2018) and rules begin (Grundrente from 2021,
Bürgergeld from 2023). `get_soep_to_gettsim(policy_date)` returns the slice of `_BASE`
whose qnames are in GETTSIM's policy environment at that date.

The per-date scope lives in the committed `gettsim_input_scope.json`, generated offline
from GETTSIM (`_generate_input_scope.py`) so this module needs no runtime GETTSIM
dependency. `tests/gettsim_inputs/test_input_scope.py` regenerates it where GETTSIM is
importable and fails if it has drifted. Periods that start before 2005 are flagged
`low_confidence`: GETTSIM's pre-2015 policy environments are not complete (gettsim
issue #962), so an early-date slice may omit nodes.
"""

import datetime
import functools
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

# Index variables carried through unchanged. They are also GETTSIM inputs but are kept
# verbatim by the build helper rather than renamed, so they need no SOEP source entry.
INDEX_VARIABLES: Final = ("p_id", "hh_id", "survey_year")

# Date-invariant SOEP source for each candidate GETTSIM qname (root input or computed
# node), or `None` where no clean source exists. The set of qnames actually in GETTSIM
# at a given date is resolved separately via `get_soep_to_gettsim`.
_BASE: Final[dict[str, str | None]] = {
    # Core demographics.
    "p_id": "p_id",
    "hh_id": "hh_id",
    "alter": None,
    "alter_monate": None,
    "geburtsjahr": "birth_year",
    "geburtsmonat": None,
    "geburtstag": None,
    "weiblich": None,
    "arbeitsstunden_w": "actual_working_hours_w",
    "behinderungsgrad": "disability_degree",
    "schwerbehindert_grad_g": None,
    "wohnort_ost_hh": None,
    "vermögen": None,
    # Housing (`wohnen__*`).
    "wohnen__baujahr_immobilie_hh": None,
    "wohnen__bewohnt_eigentum_hh": None,
    "wohnen__bruttokaltmiete_m_hh": "rent_minus_heating_costs_m_hh",
    "wohnen__heizkosten_m_hh": "heating_costs_m_hh",
    "wohnen__wohnfläche_hh": "living_space_hh",
    "wohngeld__mietstufe_hh": None,
    # Family (`familie__*`).
    "familie__alleinerziehend": None,
    "familie__p_id_ehepartner": "ehepartner_p_id",
    "familie__p_id_elternteil_1": "p_id_mother",
    "familie__p_id_elternteil_2": None,
    # Income (`einnahmen__*`).
    "einnahmen__bruttolohn_m": "gross_labor_income_previous_month_m",
    "einnahmen__kapitalerträge_y": None,
    "einnahmen__renten__aus_berufsständischen_versicherungen_m": None,
    "einnahmen__renten__basisrente_m": None,
    "einnahmen__renten__betriebliche_altersvorsorge_m": None,
    "einnahmen__renten__geförderte_private_vorsorge_m": None,
    "einnahmen__renten__sonstige_private_vorsorge_m": None,
    # Unemployment insurance (`sozialversicherung__arbeitslosen__*`).
    "sozialversicherung__arbeitslosen__arbeitssuchend": None,
    "sozialversicherung__arbeitslosen__mean_nettoeinkommen_in_12_monaten_vor_arbeitslosigkeit_m": None,  # noqa: E501
    "sozialversicherung__arbeitslosen__monate_beitragspflichtig_versichert_in_letzten_30_monaten": None,  # noqa: E501
    "sozialversicherung__arbeitslosen__monate_durchgängigen_bezugs_von_arbeitslosengeld": None,  # noqa: E501
    "sozialversicherung__arbeitslosen__monate_sozialversicherungspflichtiger_beschäftigung_in_letzten_5_jahren": None,  # noqa: E501
    # Health/care insurance (`sozialversicherung__kranken__*`, `__pflege__*`).
    "sozialversicherung__kranken__beitrag__beitrag_private_basiskrankenversicherung_abzüglich_arbeitgeberanteil_m": None,  # noqa: E501
    "sozialversicherung__kranken__beitrag__privat_versichert": None,
    "sozialversicherung__pflege__beitrag__hat_kinder": None,
    # Pension insurance (`sozialversicherung__rente__*`).
    "sozialversicherung__rente__bezieht_rente": None,
    "sozialversicherung__rente__entgeltpunkte": None,
    "sozialversicherung__rente__entgeltpunkte_ost": None,
    "sozialversicherung__rente__entgeltpunkte_west": None,
    "sozialversicherung__rente__altersrente__für_frauen__pflichtsbeitragsjahre_ab_alter_40": None,  # noqa: E501
    "sozialversicherung__rente__altersrente__höchster_bruttolohn_letzte_15_jahre_vor_rente_y": None,  # noqa: E501
    "sozialversicherung__rente__altersrente__wegen_arbeitslosigkeit__arbeitslos_für_1_jahr_nach_alter_58_ein_halb": None,  # noqa: E501
    "sozialversicherung__rente__altersrente__wegen_arbeitslosigkeit__pflichtbeitragsjahre_8_von_10": None,  # noqa: E501
    "sozialversicherung__rente__altersrente__wegen_arbeitslosigkeit__vertrauensschutz_1997": None,  # noqa: E501
    "sozialversicherung__rente__altersrente__wegen_arbeitslosigkeit__vertrauensschutz_2004": None,  # noqa: E501
    "sozialversicherung__rente__ersatzzeiten_monate": None,
    "sozialversicherung__rente__erwerbsminderung__teilweise_erwerbsgemindert": None,
    "sozialversicherung__rente__erwerbsminderung__voll_erwerbsgemindert": None,
    "sozialversicherung__rente__freiwillige_beitragsmonate": None,
    "sozialversicherung__rente__grundrente__bewertungszeiten_monate": None,
    "sozialversicherung__rente__grundrente__bruttolohn_vorjahr_y": None,
    "sozialversicherung__rente__grundrente__einnahmen_aus_kapitalvermögen_vorvorjahr_y": None,  # noqa: E501
    "sozialversicherung__rente__grundrente__einnahmen_aus_selbstständiger_arbeit_vorvorjahr_y": None,  # noqa: E501
    "sozialversicherung__rente__grundrente__einnahmen_aus_vermietung_und_verpachtung_vorvorjahr_y": None,  # noqa: E501
    "sozialversicherung__rente__grundrente__gesamteinnahmen_aus_renten_vorjahr_m": None,
    "sozialversicherung__rente__grundrente__grundrentenzeiten_monate": None,
    "sozialversicherung__rente__grundrente__mean_entgeltpunkte": None,
    "sozialversicherung__rente__jahr_renteneintritt": None,
    "sozialversicherung__rente__kinderberücksichtigungszeiten_monate": None,
    "sozialversicherung__rente__krankheitszeiten_ab_16_bis_24_monate": None,
    "sozialversicherung__rente__monat_renteneintritt": None,
    "sozialversicherung__rente__monate_geringfügiger_beschäftigung": None,
    "sozialversicherung__rente__monate_in_arbeitslosigkeit": None,
    "sozialversicherung__rente__monate_in_arbeitsunfähigkeit": None,
    "sozialversicherung__rente__monate_in_ausbildungssuche": None,
    "sozialversicherung__rente__monate_in_mutterschutz": None,
    "sozialversicherung__rente__monate_in_schulausbildung": None,
    "sozialversicherung__rente__monate_mit_bezug_entgeltersatzleistungen_wegen_arbeitslosigkeit": None,  # noqa: E501
    "sozialversicherung__rente__pflegeberücksichtigungszeiten_monate": None,
    "sozialversicherung__rente__pflichtbeitragsmonate": None,
    # Income tax (`einkommensteuer__*`).
    "einkommensteuer__gemeinsam_veranlagt": None,
    "einkommensteuer__abzüge__beitrag_private_rentenversicherung_m": None,
    "einkommensteuer__abzüge__kinderbetreuungskosten_m": None,
    "einkommensteuer__abzüge__p_id_kinderbetreuungskostenträger": None,
    "einkommensteuer__einkünfte__aus_forst_und_landwirtschaft__betrag_y": None,
    "einkommensteuer__einkünfte__aus_gewerbebetrieb__betrag_y": None,
    "einkommensteuer__einkünfte__aus_nichtselbstständiger_arbeit__tatsächliche_werbungskosten_y": None,  # noqa: E501
    "einkommensteuer__einkünfte__aus_selbstständiger_arbeit__betrag_y": "earnings_from_self_employment_y",  # noqa: E501
    "einkommensteuer__einkünfte__aus_vermietung_und_verpachtung__betrag_y": None,
    "einkommensteuer__einkünfte__ist_hauptberuflich_selbstständig": None,
    "einkommensteuer__einkünfte__sonstige__alle_weiteren_y": None,
    "einkommensteuer__einkünfte__sonstige__rente__alter_beginn_leistungsbezug_berufsständische_altersvorsorge": None,  # noqa: E501
    "einkommensteuer__einkünfte__sonstige__rente__alter_beginn_leistungsbezug_betriebliche_altersvorsorge": None,  # noqa: E501
    "einkommensteuer__einkünfte__sonstige__rente__alter_beginn_leistungsbezug_sonstige_private_vorsorge": None,  # noqa: E501
    # Wage tax (`lohnsteuer__*`).
    "lohnsteuer__steuerklasse": None,
    # Child benefit (`kindergeld__*`).
    "kindergeld__in_ausbildung": None,
    "kindergeld__p_id_empfänger": None,
    # Maintenance (`unterhalt__*`).
    "unterhalt__anspruch_m": None,
    "unterhalt__tatsächlich_erhaltener_betrag_m": None,
    # Parental benefits (`elterngeld__*`).
    "elterngeld__bisherige_bezugsmonate": None,
    "elterngeld__claimed": None,
    "elterngeld__mean_nettoeinkommen_in_12_monaten_vor_geburt_m": None,
    "elterngeld__zu_versteuerndes_einkommen_vorjahr_y_sn": None,
    # Child-rearing benefit (`erziehungsgeld__*`), pre-2007 rules.
    "erziehungsgeld__bruttolohn_vorjahr_nach_abzug_werbungskosten_y": None,
    "erziehungsgeld__budgetsatz": None,
    "erziehungsgeld__p_id_empfänger": None,
    # Citizen's income / basic income support
    # (`bürgergeld__*`, `arbeitslosengeld_2__*`).
    "bürgergeld__bezug_im_vorjahr": None,
    "bürgergeld__p_id_einstandspartner": "partner_p_id",
    "arbeitslosengeld_2__p_id_einstandspartner": "partner_p_id",
    # Evaluation date (set by GETTSIM at call time, not from data).
    "evaluation_day": None,
    "evaluation_month": None,
    "evaluation_year": None,
    # Computed transfer amounts (non-root override candidates). Supplying any of these
    # replaces GETTSIM's own calculation; the SOEP reported amount is at a different
    # aggregation level or reference period than the node, so each stays `None` with a
    # `GAP_NOTES` entry rather than feeding a mismatched value (see the override caution
    # in `docs/using_with_gettsim.md`).
    "kindergeld__betrag_m": None,
    "wohngeld__betrag_m_wthh": None,
    "elterngeld__betrag_m": None,
    "unterhaltsvorschuss__betrag_m": None,
    "kinderzuschlag__betrag_m_bg": None,
}

# Date-invariant union of every candidate pairing. `get_soep_to_gettsim(policy_date)`
# narrows this to the qnames in GETTSIM's policy environment at that date.
BASE_MAPPING: Final[MappingProxyType[str, str | None]] = MappingProxyType(_BASE)

# Reasons a GETTSIM qname has no clean SOEP source. Keyed by GETTSIM qname; only nodes
# deemed plausibly derivable or with an instructive mismatch are listed. Nodes absent
# here are simply not available in the survey. The notes describe the gap conceptually;
# look up the exact current SOEP variable names in
# `create_metadata/variable_to_metadata_mapping.yaml`.
GAP_NOTES: Final[MappingProxyType[str, str]] = MappingProxyType(
    {
        "alter": "Derive from `birth_year` and the survey year; unit/timing differs.",
        "alter_monate": "No month-granular age in SOEP final variables.",
        "geburtsmonat": "`birth_month` is categorical, not int8.",
        "geburtstag": "No day-of-birth in SOEP final variables.",
        "weiblich": (
            "Derive bool from `gender` ('Female'). Required for pre-2018 "
            "Altersrente-für-Frauen rules."
        ),
        "schwerbehindert_grad_g": "No marker-G disability flag in SOEP.",
        "wohnort_ost_hh": "Derive from the federal-state-of-residence variable.",
        "vermögen": "Pick a net-wealth concept from the wealth modules.",
        "wohnen__baujahr_immobilie_hh": (
            "SOEP has building-year ranges (`building_year_hh_min`/`_max`), not a "
            "single year."
        ),
        "wohnen__bewohnt_eigentum_hh": "Derive from `rented_or_owned` ('Owner').",
        "wohngeld__mietstufe_hh": "No Mietstufe in SOEP final variables.",
        "familie__alleinerziehend": "No single-parent flag in SOEP.",
        "familie__p_id_elternteil_2": (
            "Only a mother pointer exists in SOEP (`p_id_mother`); no father pointer."
        ),
        "einnahmen__kapitalerträge_y": "SOEP only has household-level capital income.",
        "einnahmen__renten__aus_berufsständischen_versicherungen_m": (
            "SOEP is annual (`berufsständische_altersvorsorge_y`)."
        ),
        "einnahmen__renten__basisrente_m": "No monthly Basisrente in SOEP.",
        "einnahmen__renten__betriebliche_altersvorsorge_m": (
            "SOEP is annual (`betriebliche_altersversorgung_y`)."
        ),
        "einnahmen__renten__geförderte_private_vorsorge_m": (
            "SOEP is annual (`riester_rente_y`)."
        ),
        "einnahmen__renten__sonstige_private_vorsorge_m": (
            "SOEP is annual (`private_altersvorsorge_y`)."
        ),
        "sozialversicherung__arbeitslosen__arbeitssuchend": (
            "Derive from a registered-unemployment variable."
        ),
        "sozialversicherung__kranken__beitrag__privat_versichert": (
            "Derive from a health-insurance-type variable."
        ),
        "sozialversicherung__pflege__beitrag__hat_kinder": (
            "Derive from a number-of-children variable."
        ),
        "sozialversicherung__rente__bezieht_rente": (
            "Derive bool from a pension-receipt variable."
        ),
        "sozialversicherung__rente__entgeltpunkte": (
            "Not in SOEP; needs FDZ-RV linkage (`rv_id`)."
        ),
        "sozialversicherung__rente__entgeltpunkte_ost": (
            "Not in SOEP; needs FDZ-RV linkage (`rv_id`)."
        ),
        "sozialversicherung__rente__entgeltpunkte_west": (
            "Not in SOEP; needs FDZ-RV linkage (`rv_id`)."
        ),
        "sozialversicherung__rente__jahr_renteneintritt": (
            "No statutory pension-claim start event in SOEP. "
            "`first_pension_receipt_year` is the first year of positive statutory "
            "pension income (or a retirement-calendar status proxy): left-censored "
            "by late panel entry and includes non-statutory states, so it is not the "
            "actual entry year."
        ),
        "sozialversicherung__rente__monat_renteneintritt": (
            "SOEP only resolves the year of first pension receipt."
        ),
        "einkommensteuer__gemeinsam_veranlagt": (
            "Derive from marital/partnership status."
        ),
        "einkommensteuer__abzüge__beitrag_private_rentenversicherung_m": (
            "SOEP is a monthly contribution; concept differs."
        ),
        "einkommensteuer__abzüge__kinderbetreuungskosten_m": (
            "SOEP childcare-cost variable has a different concept."
        ),
        "einkommensteuer__einkünfte__aus_vermietung_und_verpachtung__betrag_y": (
            "SOEP only has household-level rental income."
        ),
        "einkommensteuer__einkünfte__ist_hauptberuflich_selbstständig": (
            "Derive from a self-employment variable."
        ),
        "lohnsteuer__steuerklasse": "No tax bracket in SOEP final variables.",
        "kindergeld__in_ausbildung": "Derive from an education/training variable.",
        "kindergeld__p_id_empfänger": "No recipient pointer in SOEP.",
        "unterhalt__anspruch_m": "No entitlement amount in SOEP.",
        "unterhalt__tatsächlich_erhaltener_betrag_m": (
            "SOEP only has child-specific maintenance received "
            "(`kindesunterhalt_received_m`); mapping it to the generic GETTSIM input "
            "would feed a narrower-than-intended amount. Revisit if a generic monthly "
            "maintenance-received amount is constructed."
        ),
        "bürgergeld__bezug_im_vorjahr": "Derive from prior-year Bürgergeld receipt.",
        "kindergeld__betrag_m": (
            "Computed node (per child). SOEP `kindergeld_m_hh` is a household total, "
            "so it cannot supply the per-child amount without an allocation rule; "
            "overriding it would replace GETTSIM's calculation with a coarser figure."
        ),
        "wohngeld__betrag_m_wthh": (
            "Computed node at the wohngeldrechtlicher-Teilhaushalt level. SOEP "
            "`wohngeld_m_hh` is household-level and groups differently, so it is not a "
            "clean substitute for the wthh amount."
        ),
        "elterngeld__betrag_m": (
            "Computed node. SOEP records Mutterschaftsgeld receipt but no direct "
            "monthly Elterngeld amount, so there is no clean source."
        ),
        "unterhaltsvorschuss__betrag_m": (
            "Computed node (monthly). SOEP `unterhaltsvorschuss_received_y` is annual; "
            "concept and reference period differ."
        ),
        "kinderzuschlag__betrag_m_bg": (
            "Computed node at the Bedarfsgemeinschaft level. SOEP "
            "`kinderzuschlag_currently_received_m_hh` is household-level and groups "
            "differently."
        ),
    }
)

_SCOPE_PATH: Final = Path(__file__).with_name("gettsim_input_scope.json")


@dataclass(frozen=True)
class MappingPeriod:
    """One policy-date period over which GETTSIM's in-scope qnames are constant."""

    start_date: datetime.date
    """First policy date in the period (inclusive)."""
    end_date: datetime.date
    """Last policy date in the period (inclusive)."""
    low_confidence: bool
    """`True` for periods starting before 2005, where GETTSIM's policy environment is
    not complete (issue #962) and the slice may omit nodes."""
    in_scope: frozenset[str]
    """`_BASE` qnames that are part of GETTSIM's policy environment in this period."""


@functools.cache
def _periods() -> tuple[MappingPeriod, ...]:
    """Load the committed per-period GETTSIM scope, ordered by start date."""
    raw = json.loads(_SCOPE_PATH.read_text(encoding="utf-8"))
    periods = tuple(
        MappingPeriod(
            start_date=datetime.date.fromisoformat(entry["start_date"]),
            end_date=datetime.date.fromisoformat(entry["end_date"]),
            low_confidence=entry["low_confidence"],
            in_scope=frozenset(entry["in_scope"]),
        )
        for entry in raw["periods"]
    )
    return tuple(sorted(periods, key=lambda period: period.start_date))


def mapping_periods() -> tuple[MappingPeriod, ...]:
    """Return all policy-date periods, ordered by start date."""
    return _periods()


def period_for_date(policy_date: datetime.date) -> MappingPeriod:
    """Return the mapping period containing `policy_date`.

    Args:
        policy_date: The GETTSIM policy date.

    Returns:
        The `MappingPeriod` whose inclusive `[start_date, end_date]` contains
        `policy_date`.

    Raises:
        ValueError: If no period covers `policy_date`.
    """
    for period in _periods():
        if period.start_date <= policy_date <= period.end_date:
            return period
    covered = _periods()
    msg = (
        f"No GETTSIM-input mapping period covers {policy_date.isoformat()}. "
        f"Covered range: {covered[0].start_date.isoformat()} to "
        f"{covered[-1].end_date.isoformat()}."
    )
    raise ValueError(msg)


def get_soep_to_gettsim(
    policy_date: datetime.date,
) -> MappingProxyType[str, str | None]:
    """Return the SOEP-to-GETTSIM mapping in scope at `policy_date`.

    The result is the slice of `_BASE` whose qnames are part of GETTSIM's policy
    environment at `policy_date` (root inputs and computed nodes alike). The SOEP
    sources are date-invariant; only the set of qnames changes with the date.

    Args:
        policy_date: The GETTSIM policy date.

    Returns:
        GETTSIM qname to SOEP variable name (or `None`), restricted to the qnames in
        scope at `policy_date`.

    Raises:
        ValueError: If no period covers `policy_date`.
    """
    period = period_for_date(policy_date)
    return MappingProxyType(
        {qname: _BASE[qname] for qname in _BASE if qname in period.in_scope}
    )
