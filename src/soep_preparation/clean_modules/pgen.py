"""Clean and convert SOEP pgen variables to appropriate data types."""

import re

import pandas as pd

from soep_preparation.utilities import month_mapping
from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    create_dummy,
    object_to_bool_categorical,
    object_to_float,
    object_to_int_categorical,
    object_to_str_categorical,
    replace_not_applicable_answer,
    translate_categories,
)

# `Werkstatt für behinderte Menschen` is the SGB-IX sheltered-employment institution and
# stays German (on the naming allow-list); every other label is translated to English.
_EMPLOYMENT_STATUS_EN = {
    "Ausbildung, Lehre": "In education/training",
    "Nicht erwerbstätig": "Not employed",
    "Teilzeitbeschäftigung": "Part-time employed",
    "Unregelmässig, geringfügig erwerbstät.": "Irregularly/marginally employed",
    "Voll erwerbstätig": "Full-time employed",
    "Werkstatt für behinderte Menschen (1998-2020)": (
        "Werkstatt für behinderte Menschen (1998-2020)"
    ),
    "in Kurzarbeit (2021-2023)": "On short-time work (2021-2023)",
}

# `NE` = nicht erwerbstätig (not employed); kept as the SOEP code prefix.
_LABOR_FORCE_STATUS_EN = {
    "Erwerbstätig": "Employed",
    "Erwerbstätig, aber inaktiv in den letzten 7 Tagen (seit 2000)": (
        "Employed but inactive in the last 7 days (since 2000)"
    ),
    "NE: 65 Jahre und älter": "NE: 65 years and older",
    "NE: Altersteilzeit mit Arbeitszeit Null": (
        "NE: partial retirement with zero working hours"
    ),
    "NE: Mutterschutz/Elternzeit (seit 1991)": (
        "NE: maternity/parental leave (since 1991)"
    ),
    "NE: aber bezahlte Arbeit in den letzten 7 Tagen (seit 1999)": (
        "NE: but paid work in the last 7 days (since 1999)"
    ),
    "NE: aber bezahlte Nebentätigkeit (seit 2017)": (
        "NE: but paid secondary job (since 2017)"
    ),
    "NE: aber gelegentliche Nebentätigkeit (1985-2016)": (
        "NE: but occasional secondary job (1985-2016)"
    ),
    "NE: aber regelmäßig bezahlte Nebentätigkeit (1985-2016)": (
        "NE: but regularly paid secondary job (1985-2016)"
    ),
    "NE: arbeitslos registriert": "NE: registered unemployed",
    "NE: derzeit in Ausbildung": "NE: currently in education/training",
    "NE: im Militär-/Zivildienst": "NE: in military/civilian service",
    "Nicht erwerbstätig (NE): keine weitere Angabe": (
        "Not employed (NE): no further information"
    ),
}

_REASON_EMPLOYMENT_ENDED_EN = {
    "Arbeitserlaubnis nicht verlängert (seit 2019)": (
        "Work permit not renewed (since 2019)"
    ),
    "Arbeitsverhältnis befristet (1985-1998)": "Fixed-term employment (1985-1998)",
    "Arbeitsverhältnis befristet, Ausbildungsverhältnis beendet (seit 1999)": (
        "Fixed-term employment / apprenticeship ended (since 1999)"
    ),
    "Aufgabe eigenes Geschäfts": "Gave up own business",
    "Ausbildungsverhältnis beendet (1985-1998)": "Apprenticeship ended (1985-1998)",
    "Betriebsstilllegung (1991-1998, seit 2001)": (
        "Business closure (1991-1998, since 2001)"
    ),
    "Beurlaubung, Mutterschutz, Elternzeit (1991-1998, seit 2011)": (
        "Leave of absence, maternity/parental leave (1991-1998, since 2011)"
    ),
    "Beurlaubung, freigstellt (1999-2010)": "Leave of absence, released (1999-2010)",
    "Eigene Kündigung": "Own resignation",
    "Einvernehmliche Auflösung, Auflösungsvertrag (1985-1990, seit 1999)": (
        "Mutual termination / termination agreement (1985-1990, since 1999)"
    ),
    "Kündigung durch Arbeitgeber": "Dismissal by employer",
    "Sonst. inkl. Stilllegung, Rente/Pension, beurlaubt, freigestellt (1987-1990)": (
        "Other incl. closure, pension, leave, release (1987-1990)"
    ),
    "Sonst. inkl. Vorruhestand, Stilllegung, Rente/Pension, beurl., freig. (1985-1986)": (  # noqa: E501
        "Other incl. early retirement, closure, pension, leave, release (1985-1986)"
    ),
    "Sonst. inkl. einvernehm. Auflösung/Auflösungsvertrag (1991-1998)": (
        "Other incl. mutual termination/termination agreement (1991-1998)"
    ),
    "Versetzung durch Betrieb (1985-1998)": "Transfer by employer (1985-1998)",
    "Versetzung eigener Wunsch (1985-1998)": "Transfer at own request (1985-1998)",
    "Vorruhestandsregelung (1987-1998)": "Early-retirement scheme (1987-1998)",
    "in Rente, Pension gegangen (seit 1991)": (
        "Retired / went into pension (since 1991)"
    ),
}

# German nationality labels (SOEP `pgnation`) to English. Mostly countries (older SOEP
# transliterations, e.g. Tuerkei); a few are regions/groups kept as their English term.
_FIRST_NATIONALITY_EN = {
    "Aegypten": "Egypt",
    "Afghanistan": "Afghanistan",
    "Afrika": "Africa",
    "Albanien": "Albania",
    "Algerien": "Algeria",
    "Angola": "Angola",
    "Argentinien": "Argentina",
    "Armenien": "Armenia",
    "Aserbaidschan": "Azerbaijan",
    "Australien": "Australia",
    "Bahamas": "Bahamas",
    "Bangladesch": "Bangladesh",
    "Belgien": "Belgium",
    "Benelux": "Benelux",
    "Benin": "Benin",
    "Bolivien": "Bolivia",
    "Bosnien/Herzegowina": "Bosnia/Herzegovina",
    "Botswana": "Botswana",
    "Brasilien": "Brazil",
    "Bulgarien": "Bulgaria",
    "Burkina Faso": "Burkina Faso",
    "Cap Verde": "Cape Verde",
    "Chile": "Chile",
    "China": "China",
    "Costa Rica": "Costa Rica",
    "Deutschland": "Germany",
    "Dominikanische Republik": "Dominican Republic",
    "Dänemark": "Denmark",
    "Ecuador": "Ecuador",
    "El Salvador": "El Salvador",
    "Elfenbeinkueste": "Cote d'Ivoire",
    "Eritrea": "Eritrea",
    "Estland": "Estonia",
    "Ethnische Minderheiten (z.B. Jesiden, Roma)": (
        "Ethnic minorities (e.g. Yazidis, Roma)"
    ),
    "Finnland": "Finland",
    "Frankreich": "France",
    "Gambia": "Gambia",
    "Georgien": "Georgia",
    "Ghana": "Ghana",
    "Griechenland": "Greece",
    "Grobritannien": "Great Britain",
    "Guinea": "Guinea",
    "Holland": "Netherlands",
    "Honduras": "Honduras",
    "Indien": "India",
    "Indonesien": "Indonesia",
    "Inguschetien": "Ingushetia",
    "Irak": "Iraq",
    "Iran": "Iran",
    "Irland": "Ireland",
    "Israel": "Israel",
    "Italien": "Italy",
    "Jamaika": "Jamaica",
    "Japan": "Japan",
    "Jemen": "Yemen",
    "Jordanien": "Jordan",
    "Jugoslawien/MontenegroSerbien": "Yugoslavia/Montenegro-Serbia",
    "Kabardino-Balkarien": "Kabardino-Balkaria",
    "Kambodscha": "Cambodia",
    "Kamerun": "Cameroon",
    "Kanada": "Canada",
    "Kasachstan": "Kazakhstan",
    "Kaukasus": "Caucasus",
    "Kenia": "Kenya",
    "Kirgistan": "Kyrgyzstan",
    "Kolumbien": "Colombia",
    "Kongo": "Congo",
    "Korea": "Korea",
    "Kosovo": "Kosovo",
    "Kosovo-Albaner": "Kosovo Albanians",
    "Kroatien": "Croatia",
    "Kuba": "Cuba",
    "Kurdistan": "Kurdistan",
    "Lettland": "Latvia",
    "Libanon": "Lebanon",
    "Libyen": "Libya",
    "Litauen": "Lithuania",
    "Luxemburg": "Luxembourg",
    "Madagaskar": "Madagascar",
    "Makedonien": "Macedonia",
    "Malaysia": "Malaysia",
    "Mali": "Mali",
    "Marokko": "Morocco",
    "Mauritius": "Mauritius",
    "Mexiko": "Mexico",
    "Mocambique": "Mozambique",
    "Moldawien": "Moldova",
    "Mongolei": "Mongolia",
    "Montenegro": "Montenegro",
    "Myanmar": "Myanmar",
    "Namibia": "Namibia",
    "Nepal": "Nepal",
    "Neuseeland": "New Zealand",
    "Nicaragua": "Nicaragua",
    "Niger": "Niger",
    "Nigeria": "Nigeria",
    "Norwegen": "Norway",
    "Oesterreich": "Austria",
    "Pakistan": "Pakistan",
    "Palstina": "Palestine",
    "Paraquay": "Paraguay",
    "Peru": "Peru",
    "Philippinen": "Philippines",
    "Polen": "Poland",
    "Portugal": "Portugal",
    "Ruanda": "Rwanda",
    "Rumänien": "Romania",
    "Russland": "Russia",
    "Sambia": "Zambia",
    "Samoa": "Samoa",
    "Saudi Arabien": "Saudi Arabia",
    "Schweden": "Sweden",
    "Schweiz": "Switzerland",
    "Senegal": "Senegal",
    "Serbien": "Serbia",
    "Seychellen": "Seychelles",
    "Sierra Leone": "Sierra Leone",
    "Simbabwe": "Zimbabwe",
    "Singapur": "Singapore",
    "Slowakei": "Slovakia",
    "Slowenien": "Slovenia",
    "Somalia": "Somalia",
    "Spanien": "Spain",
    "Sri Lanka": "Sri Lanka",
    "St. Lucia": "St. Lucia",
    "Staatenlos": "Stateless",
    "Sudan": "Sudan",
    "Suedafrika": "South Africa",
    "Syrien": "Syria",
    "Tadschikistan": "Tajikistan",
    "Taiwan": "Taiwan",
    "Tansania": "Tanzania",
    "Thailand": "Thailand",
    "Togo": "Togo",
    "Tschad": "Chad",
    "Tschechien": "Czechia",
    "Tschetschenien": "Chechnya",
    "Tuerkei": "Turkey",
    "Tunesien": "Tunisia",
    "Turkmenistan": "Turkmenistan",
    "USA": "United States",
    "Uganda": "Uganda",
    "Ukraine": "Ukraine",
    "Ungarn": "Hungary",
    "Uruguay": "Uruguay",
    "Usbekistan": "Uzbekistan",
    "Venezuela": "Venezuela",
    "Vietnam": "Vietnam",
    "Weissruland": "Belarus",
    "Äthiopien": "Ethiopia",
}


def _education(
    casmin: pd.Series[pd.Categorical],
    isced: pd.Series[pd.Categorical],
) -> pd.Series[pd.Categorical]:
    """Transform education variable to three levels."""
    out = casmin.combine_first(isced).map(
        {
            "in school": "PRIMARY_AND_LOWER_SECONDARY",
            "inadequately completed": "PRIMARY_AND_LOWER_SECONDARY",
            "general elementary school": "PRIMARY_AND_LOWER_SECONDARY",
            "basic vocational qualification": "PRIMARY_AND_LOWER_SECONDARY",
            "intermediate vocational": "UPPER_SECONDARY",
            "intermediate general qualification": "UPPER_SECONDARY",
            "general maturity certificate": "UPPER_SECONDARY",
            "vocational maturity certificate": "UPPER_SECONDARY",
            "lower tertiary education": "TERTIARY",
            "higher tertiary education": "TERTIARY",
            "Primary education": "PRIMARY_AND_LOWER_SECONDARY",
            "Lower secondary education": "PRIMARY_AND_LOWER_SECONDARY",
            "Upper secondary education": "UPPER_SECONDARY",
            "Post-secondary non-tertiary education": "UPPER_SECONDARY",
            "Short-cycle tertiary education": "UPPER_SECONDARY",
            "Bachelor s or equivalent level": "TERTIARY",
            "Master s or equivalent level": "TERTIARY",
            "Doctoral or equivalent level": "TERTIARY",
        },
    )
    cat_type = pd.CategoricalDtype(
        categories=pd.Series(
            ["PRIMARY_AND_LOWER_SECONDARY", "UPPER_SECONDARY", "TERTIARY"],
        ).astype("string[pyarrow]"),
        ordered=True,
    )
    return out.astype(cat_type)


def _in_education(
    employment: pd.Series[pd.Categorical],
    occupation: pd.Series[pd.Categorical],
) -> pd.Series[pd.Categorical]:
    in_education = [
        "Auszubildende (1984-1999), Lehrlinge (1990 Ost)",
        "Auszubildende, gewerblich-technisch (ab 2000)",
        "Auszubildende, kaufmännisch (ab 2000)",
        "NE: in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
        "Volontär*innen, Praktikant*innen",
    ]
    out = create_dummy(series=employment, value_for_comparison="In education/training")
    # Set in_education to missing if out of the labor force -- could mean anything.
    out = out.where(employment != "Not employed", pd.NA)
    return out.fillna(
        create_dummy(
            series=occupation, value_for_comparison=in_education, comparison_type="isin"
        )
    )


def _self_employed_occupations(
    occupation: pd.Series[pd.Categorical],
) -> list:
    """Occupation names that indicate self employment."""
    occupation_names = list(occupation.dropna().unique())
    occupations_of_interest = re.compile("^.*Freiberufler.*$|^.*selb(st)?stä.*$")
    return [
        name
        for name in occupation_names
        if occupations_of_interest.match(name) is not None
    ]


def _weekly_working_hours_fill_non_working(
    working_hours: pd.Series,
    employment_status: pd.Series,
) -> pd.Series:
    out = object_to_float(working_hours)
    return out.where(employment_status != "Not employed", 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the pgen module.

    Args:
        raw_data: The raw pgen data.

    Returns:
        The processed pgen data.
    """
    out = pd.DataFrame()

    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["month_interview"] = object_to_int_categorical(
        series=raw_data["imonth"],
        renaming=month_mapping.de,
        ordered=True,
    )
    out["education_isced_97"] = object_to_str_categorical(
        raw_data["pgisced97"],
        renaming={
            "general elemantary": "general elementary",  # codespell:ignore elemantary
            "higher education": "higher education",
            "higher vocational": "higher vocational",
            "in school": "in school",
            "inadequately": "inadequately",
            "middle vocational": "middle vocational",
            "vocational + Abi": "vocational + Abi",
        },
    )
    out["education_isced"] = object_to_str_categorical(raw_data["pgisced11"])
    out["education_isced_cat"] = apply_smallest_int_dtype(
        out["education_isced"].cat.codes
    )
    out["education_casmin"] = object_to_str_categorical(
        series=raw_data["pgcasmin"],
        nr_identifiers=2,
    )
    out["education_casmin_cat"] = apply_smallest_int_dtype(
        out["education_casmin"].cat.codes
    )
    out["highest_education"] = _education(
        casmin=out["education_casmin"],
        isced=out["education_isced"],
    )

    # individual current status
    # there are multiple categories for ['Kurdistan', 'Malaysia', 'Montenegro']
    # they have been reduced into one category each
    out["first_nationality"] = translate_categories(
        object_to_str_categorical(raw_data["pgnation"]), _FIRST_NATIONALITY_EN
    )
    out["german"] = create_dummy(
        series=out["first_nationality"], value_for_comparison="Germany"
    )
    out["refugee_status"] = object_to_str_categorical(raw_data["pgstatus_refu"])
    out["marital_status"] = object_to_str_categorical(raw_data["pgfamstd"])
    out["labor_force_status"] = translate_categories(
        object_to_str_categorical(raw_data["pglfs"]), _LABOR_FORCE_STATUS_EN
    )
    out["occupation_status"] = object_to_str_categorical(raw_data["pgstib"])
    out["employment_status"] = translate_categories(
        object_to_str_categorical(raw_data["pgemplst"]), _EMPLOYMENT_STATUS_EN
    )
    out["total_full_time_working_experience"] = object_to_float(raw_data["pgexpft"])
    out["total_part_time_working_experience"] = object_to_float(raw_data["pgexppt"])
    out["total_unemployment_experience"] = object_to_float(raw_data["pgexpue"])
    out["tenure"] = object_to_float(raw_data["pgerwzeit"])
    out["retired"] = create_dummy(
        series=out["occupation_status"], value_for_comparison="NE: in Rentenbezug"
    )
    out["in_education"] = _in_education(
        employment=out["employment_status"],
        occupation=out["occupation_status"],
    )
    out["self_employed"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison=_self_employed_occupations(out["occupation_status"]),
        comparison_type="isin",
    )
    out["military"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison="NE: Wehr- und Zivildienst",
    )

    out["employed"] = (
        create_dummy(
            series=out["employment_status"],
            value_for_comparison="Not employed",
            comparison_type="neq",
        )
    ) & (~out["in_education"])

    out["not_employed"] = create_dummy(
        series=out["employment_status"],
        value_for_comparison="Not employed",
    )
    out["registered_unemployed"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison="NE: arbeitslos gemeldet",
    )
    out["employed_full_time"] = create_dummy(
        series=out["employment_status"], value_for_comparison="Full-time employed"
    )
    out["employed_part_time"] = create_dummy(
        series=out["employment_status"], value_for_comparison="Part-time employed"
    )
    out["irregularly_or_marginally_employed"] = create_dummy(
        series=out["employment_status"],
        value_for_comparison="Irregularly/marginally employed",
    )
    out["werkstatt_für_behinderte"] = create_dummy(
        series=out["employment_status"],
        value_for_comparison="Werkstatt für behinderte Menschen (1998-2020)",
    )
    out["civil_servant"] = create_dummy(
        series=out["occupation_status"],
        value_for_comparison="Beamt*innen",
        comparison_type="startswith",
    )
    out["mutterschutz_elternzeit"] = create_dummy(
        series=out["labor_force_status"],
        value_for_comparison="NE: maternity/parental leave (since 1991)",
    )

    # individual work information
    out["gross_labor_income_previous_month_m"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["pglabgro"], value=0)
    )
    out["net_labor_income_previous_month_m"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["pglabnet"], value=0)
    )
    out["actual_working_hours_w"] = _weekly_working_hours_fill_non_working(
        working_hours=raw_data["pgtatzeit"],
        employment_status=out["employment_status"],
    )
    out["contractual_working_hours_w"] = _weekly_working_hours_fill_non_working(
        working_hours=raw_data["pgvebzeit"],
        employment_status=out["employment_status"],
    )
    out["in_public_service"] = object_to_bool_categorical(
        series=raw_data["pgoeffd"],
        renaming={"[2] nein": False, "[1] ja": True},
        ordered=True,
    )
    out["firm_size"] = object_to_str_categorical(raw_data["pgallbet"])
    out["firm_size_detailed_but_inconsistent_categories"] = object_to_str_categorical(
        raw_data["pgbetr"].replace(
            {-5: "[-5] in Fragebogenversion nicht enthalten"},
        ),
    )
    out["reason_employment_ended"] = translate_categories(
        object_to_str_categorical(raw_data["pgjobend"]),
        _REASON_EMPLOYMENT_ENDED_EN,
    )

    return out
