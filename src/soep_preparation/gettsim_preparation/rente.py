"""Functions to calculate the year of retirement."""

import pandas as pd


def _renteneintritt_pot_jahr(geb_jahr, geb_monat, erf_vollzeit, erf_teilzeit):
    """Inputs:
        geb_jahr
        geb_monat
        erf_vollzeit
        erf_teilzeit


    Altersrente für besonders langjährig Versicherte:
        min. 63 Jahr alt und 45 Versicherungsjahre.
        Summe Erfahrung Voll-& Teilzeit als Substitut für Beitragsjahre. Jedoch
        nicht vollständige Betrachtung siehe:
            https://www.deutsche-rentenversicherung.de/SharedDocs/Downloads/DE/
            Broschueren/national/die_richtige_altersrente_für_sie.html

        Jahrgänge <=1952 haben Renteneintrittsalter von 63
        Jahrgänge [1953,1964] bekommen zwei Monate pro Jahrgang mehr dazu
        Jahrgänge >=1964 haben Renteneintrittsalter von 65


    Regelaltersrente:
        Jahrgänge <=1946 haben Renteneintrittsalter von 65
        Jahrgänge [1947,1958] bekommen einen Monat pro Jahrgang mehr dazu
        Jahrgänge [1959,1964] bekommen zwei Monate pro Jahrgang mehr dazu
        Jahrgänge >=1964 haben Renteneintrittsalter von 67
    """
    # Altersrente für besonders langjährig Versicherte:
    beitragsjahre = erf_vollzeit + erf_teilzeit

    if beitragsjahre >= 45:
        if geb_jahr <= 1952:
            return geb_jahr + 63

        if 1953 <= geb_jahr < 1964:
            rentenalter_anstieg_m = geb_jahr - 1952
            if (rentenalter_anstieg_m + geb_monat) >= 12:
                return geb_jahr + 63 + 1
            if (rentenalter_anstieg_m + geb_monat) >= 24:
                return geb_jahr + 63 + 2
            return geb_jahr + 63

        if geb_jahr >= 1964:
            return geb_jahr + 65

    # Regelaltersrente:
    elif geb_jahr <= 1946:
        return geb_jahr + 65

    elif 1947 <= geb_jahr <= 1958:
        rentenalter_anstieg_m = geb_jahr - 1946
        if (rentenalter_anstieg_m + geb_monat) >= 12:
            return geb_jahr + 65 + 1
        return geb_jahr + 65

    elif 1959 <= geb_jahr < 1964:
        rentenalter_anstieg_m = (geb_jahr - 1958) * 2
        if (rentenalter_anstieg_m + geb_monat) >= 12:
            return geb_jahr + 66 + 1
        return geb_jahr + 66

    elif geb_jahr >= 1964:
        return geb_jahr + 67


def renteneintritt_jahr(data: pd.DataFrame) -> pd.Series:
    """Calculate year of retirement

    Args:
        data: The merged SOEP dataset.

    Returns:
        Variable for year of retirement.
    """
    # Identify the first year of retirement
    data["retired_year"] = (
        data[data["retired"] == True].groupby("p_id")["survey_year"].transform("min")
    )

    # set retirement_entry_year as first year of retirement
    data["retirement_entry_year"] = data["retired_year"].fillna(
        data.groupby("p_id")["retired_year"].transform("mean"),
    )

    # for people without a calculated first year of retirement use year_pot_retirement
    # Assume no early or late retirement
    data["year_pot_retirement"] = data[
        ["birth_year", "birth_month", "exp_full_time", "exp_part_time"]
    ].apply(lambda x: _renteneintritt_pot_jahr(*x), axis=1)
    # fill missings
    return data["retirement_entry_year"].fillna(
        data.groupby("p_id")["year_pot_retirement"].transform("mean"),
    )
