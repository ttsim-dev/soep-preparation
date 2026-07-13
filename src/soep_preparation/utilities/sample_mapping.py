"""German-to-English labels for the SOEP sample identifier (`hsample`).

Shared by the `design` and `hpathl` clean modules, which both expose the sample under
slightly different category sets (`design` carries `2018 Aufstockung`, `hpathl` carries
`2018 Soziale Stadt`), so the map covers the union. IAB-BAMF-SOEP / IAB-SOEP study names
are proper nouns and stay verbatim.
"""

to_english = {
    "1984 Ausgangs-Sample (West)": "1984 Original sample (West)",
    "1984 Migration (bis 1983, West)": "1984 Migration (until 1983, West)",
    "1990 Ausgangs-Sample (Ost)": "1990 Original sample (East)",
    "1994/5 Migration (1984-92/94, West)": "1994/5 Migration (1984-92/94, West)",
    "1998 Aufstockung": "1998 Refreshment",
    "2000 Aufstockung": "2000 Refreshment",
    "2002 Hoch-Einkommen": "2002 High-income",
    "2006 Aufstockung": "2006 Refreshment",
    "2009 Innovation Sample": "2009 Innovation Sample",
    "2010 Familientypen(NiedrigEinkommen,AlleinErziehend,MehrKindFamilien)": (
        "2010 Family types (low-income, single-parent, multi-child)"
    ),
    "2010 Geburtskohorten (2007-2010)": "2010 Birth cohorts (2007-2010)",
    "2011 Aufstockung": "2011 Refreshment",
    "2011 Familientypen(AlleinErziehend,MehrKindFamilien)": (
        "2011 Family types (single-parent, multi-child)"
    ),
    "2012 Aufstockung": "2012 Refreshment",
    "2017 Aufstockung (PIAAC-L)": "2017 Refreshment (PIAAC-L)",
    "2018 Aufstockung": "2018 Refreshment",
    "2018 Soziale Stadt": "2018 Socially deprived neighbourhoods",
    "2019 Lesbische-Schwule-Bisexuelle (LGB) Personen": (
        "2019 Lesbian-Gay-Bisexual (LGB) persons"
    ),
    "2019 Top-Gesellschafter": "2019 Top shareholders",
    "2022 Aufstockung": "2022 Refreshment",
    "2024 Aufstockung": "2024 Refreshment",
    "IAB-BAMF-SOEP 2016 Flucht (2013-2015)": "IAB-BAMF-SOEP 2016 Refugees (2013-2015)",
    "IAB-BAMF-SOEP 2016 Flucht/Familie (2013-2015)": (
        "IAB-BAMF-SOEP 2016 Refugees/family (2013-2015)"
    ),
    "IAB-BAMF-SOEP 2017 Flucht (2013-2016)": "IAB-BAMF-SOEP 2017 Refugees (2013-2016)",
    "IAB-BAMF-SOEP 2020 Schutzsuchende": "IAB-BAMF-SOEP 2020 Asylum seekers",
    "IAB-BAMF-SOEP 2023 Schutzsuchende": "IAB-BAMF-SOEP 2023 Asylum seekers",
    "IAB-SOEP 2013 Migration (1995-2010)": "IAB-SOEP 2013 Migration (1995-2010)",
    "IAB-SOEP 2015 Migration (2009-2013)": "IAB-SOEP 2015 Migration (2009-2013)",
    "IAB-SOEP 2020 Migration": "IAB-SOEP 2020 Migration",
    "IAB-SOEP 2020 Migration-Fachkräfteeinwanderung": (
        "IAB-SOEP 2020 Migration - skilled-worker immigration"
    ),
    "IAB-SOEP 2022 Aufstockung": "IAB-SOEP 2022 Refreshment",
    "IAB-SOEP 2023 Aufstockung": "IAB-SOEP 2023 Refreshment",
    "IAB-SOEP 2024 Aufstockung": "IAB-SOEP 2024 Refreshment",
}
