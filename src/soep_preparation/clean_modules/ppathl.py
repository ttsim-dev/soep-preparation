"""Clean and convert SOEP ppathl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities import month_mapping
from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    convert_to_float,
    object_to_int,
    object_to_str_categorical,
    translate_categories,
)

# German country names (SOEP `corigin`) to English. Proper nouns; English country names
# are standard, so this is a straight relabelling.
_COUNTRY_OF_BIRTH_EN = {
    "Afghanistan": "Afghanistan",
    "Albanien": "Albania",
    "Algerien": "Algeria",
    "Amerikanisch-Samoa": "American Samoa",
    "Angola": "Angola",
    "Antarktis": "Antarctica",
    "Argentinien": "Argentina",
    "Armenien": "Armenia",
    "Aruba": "Aruba",
    "Aserbaidschan": "Azerbaijan",
    "Australien": "Australia",
    "Bahamas": "Bahamas",
    "Bahrain": "Bahrain",
    "Bangladesch": "Bangladesh",
    "Barbados": "Barbados",
    "Belgien": "Belgium",
    "Benin": "Benin",
    "Bolivien": "Bolivia",
    "Bosnien und Herzegowina": "Bosnia and Herzegovina",
    "Botsuana": "Botswana",
    "Brasilien": "Brazil",
    "Britische Jungferninseln": "British Virgin Islands",
    "Britisches Territorium im Indischen Ozean": "British Indian Ocean Territory",
    "Bulgarien": "Bulgaria",
    "Burkina Faso": "Burkina Faso",
    "Burundi": "Burundi",
    "Cabo Verde": "Cabo Verde",
    "Chile": "Chile",
    "China": "China",
    "Costa Rica": "Costa Rica",
    "Côte d’Ivoire": "Cote d'Ivoire",
    "Deutschland": "Germany",
    "Dominica": "Dominica",
    "Dominikanische Republik": "Dominican Republic",
    "Dschibuti": "Djibouti",
    "Dänemark": "Denmark",
    "Ecuador": "Ecuador",
    "El Salvador": "El Salvador",
    "Eritrea": "Eritrea",
    "Estland": "Estonia",
    "Fidschi": "Fiji",
    "Finnland": "Finland",
    "Frankreich": "France",
    "Gabun": "Gabon",
    "Gambia": "Gambia",
    "Georgien": "Georgia",
    "Ghana": "Ghana",
    "Grenada": "Grenada",
    "Griechenland": "Greece",
    "Guatemala": "Guatemala",
    "Guernsey": "Guernsey",
    "Guinea": "Guinea",
    "Guinea-Bissau": "Guinea-Bissau",
    "Guyana": "Guyana",
    "Haiti": "Haiti",
    "Heard und McDonaldinseln": "Heard and McDonald Islands",
    "Honduras": "Honduras",
    "Hongkong": "Hong Kong",
    "Indien": "India",
    "Indonesien": "Indonesia",
    "Irak": "Iraq",
    "Iran": "Iran",
    "Irland": "Ireland",
    "Island": "Iceland",
    "Israel": "Israel",
    "Italien": "Italy",
    "Jamaika": "Jamaica",
    "Japan": "Japan",
    "Jemen": "Yemen",
    "Jordanien": "Jordan",
    "Kambodscha": "Cambodia",
    "Kamerun": "Cameroon",
    "Kanada": "Canada",
    "Kasachstan": "Kazakhstan",
    "Katar": "Qatar",
    "Kenia": "Kenya",
    "Kirgistan": "Kyrgyzstan",
    "Kleinere Amerikanische Überseeinseln": "United States Minor Outlying Islands",
    "Kolumbien": "Colombia",
    "Komoren": "Comoros",
    "Kongo": "Congo",
    "Kongo, Demokratische Republik": "Congo, Democratic Republic",
    "Korea, Demokratische Volksrepublik": "Korea, Democratic People's Republic",
    "Korea, Republik": "Korea, Republic",
    "Kosovo": "Kosovo",
    "Kroatien": "Croatia",
    "Kuba": "Cuba",
    "Kuwait": "Kuwait",
    "Laos": "Laos",
    "Lesotho": "Lesotho",
    "Lettland": "Latvia",
    "Libanon": "Lebanon",
    "Liberia": "Liberia",
    "Libyen": "Libya",
    "Litauen": "Lithuania",
    "Luxemburg": "Luxembourg",
    "Macau": "Macau",
    "Madagaskar": "Madagascar",
    "Malawi": "Malawi",
    "Malaysia": "Malaysia",
    "Malediven": "Maldives",
    "Mali": "Mali",
    "Malta": "Malta",
    "Marokko": "Morocco",
    "Mauretanien": "Mauritania",
    "Mauritius": "Mauritius",
    "Mexiko": "Mexico",
    "Moldau": "Moldova",
    "Monaco": "Monaco",
    "Mongolei": "Mongolia",
    "Montenegro": "Montenegro",
    "Mosambik": "Mozambique",
    "Myanmar": "Myanmar",
    "Namibia": "Namibia",
    "Nepal": "Nepal",
    "Neuseeland": "New Zealand",
    "Nicaragua": "Nicaragua",
    "Niederlande": "Netherlands",
    "Niger": "Niger",
    "Nigeria": "Nigeria",
    "Nordmazedonien": "North Macedonia",
    "Norwegen": "Norway",
    "Oman": "Oman",
    "Pakistan": "Pakistan",
    "Palästina": "Palestine",
    "Panama": "Panama",
    "Papua-Neuguinea": "Papua New Guinea",
    "Paraguay": "Paraguay",
    "Peru": "Peru",
    "Philippinen": "Philippines",
    "Polen": "Poland",
    "Portugal": "Portugal",
    "Puerto Rico": "Puerto Rico",
    "Ruanda": "Rwanda",
    "Rumänien": "Romania",
    "Russische Föderation": "Russian Federation",
    "Salomonen": "Solomon Islands",
    "Sambia": "Zambia",
    "Samoa": "Samoa",
    "Saudi-Arabien": "Saudi Arabia",
    "Schweden": "Sweden",
    "Schweiz": "Switzerland",
    "Senegal": "Senegal",
    "Serbien": "Serbia",
    "Sierra Leone": "Sierra Leone",
    "Simbabwe": "Zimbabwe",
    "Singapur": "Singapore",
    "Slowakei": "Slovakia",
    "Slowenien": "Slovenia",
    "Somalia": "Somalia",
    "Spanien": "Spain",
    "Sri Lanka": "Sri Lanka",
    "St. Kitts und Nevis": "St. Kitts and Nevis",
    "Sudan": "Sudan",
    "Suriname": "Suriname",
    "Syrien": "Syria",
    "Südafrika": "South Africa",
    "Südsudan": "South Sudan",
    "Tadschikistan": "Tajikistan",
    "Taiwan": "Taiwan",
    "Tansania": "Tanzania",
    "Thailand": "Thailand",
    "Togo": "Togo",
    "Tonga": "Tonga",
    "Trinidad und Tobago": "Trinidad and Tobago",
    "Tschad": "Chad",
    "Tschechien": "Czechia",
    "Tunesien": "Tunisia",
    "Turkmenistan": "Turkmenistan",
    "Türkei": "Turkey",
    "Uganda": "Uganda",
    "Ukraine": "Ukraine",
    "Ungarn": "Hungary",
    "Uruguay": "Uruguay",
    "Usbekistan": "Uzbekistan",
    "Venezuela": "Venezuela",
    "Vereinigte Arabische Emirate": "United Arab Emirates",
    "Vereinigte Staaten": "United States",
    "Vereinigtes Königreich": "United Kingdom",
    "Vietnam": "Vietnam",
    "Weißrussland": "Belarus",
    "Zentralafrikanische Republik": "Central African Republic",
    "Zypern": "Cyprus",
    "Ägypten": "Egypt",
    "Äquatorialguinea": "Equatorial Guinea",
    "Äthiopien": "Ethiopia",
    "Österreich": "Austria",
}


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the ppathl module.

    Args:
        raw_data: The raw ppathl data.

    Returns:
        The processed ppathl data.
    """
    out = pd.DataFrame()

    out["hh_id"] = apply_smallest_int_dtype(
        raw_data["hid"].replace({-2: pd.NA, -3: pd.NA})
    )
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    # individual characteristics
    out["born_in_germany"] = object_to_str_categorical(raw_data["germborn"])
    out["country_of_birth"] = translate_categories(
        object_to_str_categorical(raw_data["corigin"]), _COUNTRY_OF_BIRTH_EN
    )
    out["birth_month_ppathl"] = object_to_int(
        series=raw_data["gebmonat"],
        renaming=month_mapping.de,
    )
    out["place_of_residence_1989"] = object_to_str_categorical(raw_data["loc1989"])
    out["migration_background"] = object_to_str_categorical(raw_data["migback"])
    out["birth_federal_state"] = object_to_str_categorical(raw_data["birthregion"])

    # individual current information
    out["survey_status_current"] = object_to_str_categorical(raw_data["netto"])
    out["place_of_residence_current"] = object_to_str_categorical(
        series=raw_data["sampreg"],
        renaming={
            "[1] Westdeutschland, alte Bundeslaender": (
                "West Germany (old federal states)"
            ),
            "[2] Ostdeutschland, neue Bundeslaender": (
                "East Germany (new federal states)"
            ),
        },
    )
    out["year_of_immigration"] = object_to_int(
        raw_data["immiyear"].replace(
            {
                -1: "[-1] Keine Angabe",
                -2: "[-2] Trifft nicht zu",
            },
        ),
    )
    out["sexual_orientation"] = object_to_str_categorical(raw_data["sexor"])
    out["partnership_status"] = object_to_str_categorical(raw_data["partner"])
    out["partner_p_id"] = object_to_int(
        raw_data["parid"].replace(
            {
                -1: "[-1] Keine Angabe",
                -2: "[-2] Trifft nicht zu",
            },
        ),
    )

    # individual staying probabilities and weighting factors
    out["individual_staying_probability"] = convert_to_float(raw_data["pbleib"])
    out["individual_weighting_factor"] = convert_to_float(raw_data["phrf"])
    out["individual_weighting_factor_new_only"] = convert_to_float(raw_data["phrf0"])
    out["individual_weighting_factor_without_new"] = convert_to_float(raw_data["phrf1"])

    # SOEP-RV record-linkage identifier (`ID SUF Rentenversicherung`): the bridge
    # to the FDZ-RV pension records, populated for respondents who consented to
    # the linkage and missing otherwise.
    out["rv_id"] = object_to_int(raw_data["rv_id"])
    return out
