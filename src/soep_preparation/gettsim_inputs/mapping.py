"""Curated mapping from GETTSIM data inputs to SOEP final variables.

GETTSIM declares its required data inputs via the `@policy_input()` decorator. The
full set is enumerable at runtime as
`main(main_target=MainTarget.labels.policy_inputs, policy_date_str=...)`. The inputs
are double-underscore qualified names (qnames) such as `wohnen__wohnfläche_hh`.

`SOEP_TO_GETTSIM` maps each GETTSIM input qname to the name of the SOEP final variable
that supplies it, or to `None` where no SOEP source exists yet. Only 1:1 or obviously
derivable matches carry a value; anything uncertain stays `None`, with the reason
recorded in `GAP_NOTES`, so the gap is visible rather than papered over. Every
non-`None` value is an actual key in
`create_metadata/variable_to_metadata_mapping.yaml` (enforced by a test).

The keys cover the union of GETTSIM data inputs across the SOEP survey range. The set of
required inputs is policy-date dependent: rules that expire (such as Altersrente für
Frauen) drop their inputs from later policy dates, so the mapping is a superset of any
single date's requirements.
"""

from types import MappingProxyType
from typing import Final

# Index variables carried through unchanged. They are also GETTSIM inputs but are kept
# verbatim by the build helper rather than renamed, so they need no SOEP source entry.
INDEX_VARIABLES: Final = ("p_id", "hh_id", "survey_year")

SOEP_TO_GETTSIM: Final[MappingProxyType[str, str | None]] = MappingProxyType(
    {
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
        "familie__p_id_elternteil_1": "mother_p_id",
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
        "sozialversicherung__rente__grundrente__gesamteinnahmen_aus_renten_vorjahr_m": None,  # noqa: E501
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
    }
)

# Reasons a GETTSIM input has no SOEP source yet. Keyed by GETTSIM input qname; only
# inputs deemed plausibly derivable or with an instructive mismatch are listed. Inputs
# absent here are simply not available in the survey. The notes describe the gap
# conceptually; look up the exact current SOEP variable names in
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
            "Only a mother pointer exists in SOEP (`mother_p_id`); no father pointer."
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
    }
)
