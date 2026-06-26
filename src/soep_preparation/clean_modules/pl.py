"""Clean and convert SOEP pl variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    convert_to_float,
    create_dummy,
    object_to_bool_categorical,
    object_to_float,
    object_to_int,
    object_to_str_categorical,
    replace_not_applicable_answer,
    translate_categories,
)

_HEALTH_INSURANCE_2022_EN = {
    "Ausschließlich privat versichert": "Privately insured only",
    "In einer gesetzlichen Krankenversicherung": "In statutory health insurance",
}


def _private_rente_beitrag_m_ein_umfragejahr(
    private_rente_beitrag_jahr: pd.Series[int],
    eingezahlte_monate: pd.Series[int],
    eingezahlt: pd.Series[pd.Categorical],
    survey_year: int,
) -> pd.Series[int]:
    # For 2013 it was recorded in Euros, for 2018 in cents.
    # Converting 2018 values to Euros.
    relevant_suvery_year = 2018
    if survey_year == relevant_suvery_year:
        out = object_to_float(private_rente_beitrag_jahr)
        out /= 100
    else:
        out = object_to_float(private_rente_beitrag_jahr)
    out *= eingezahlte_monate / 12
    return out.where(eingezahlt.astype("bool[pyarrow]"), 0)


def _private_rente_beitrag_m(private_rente_data: pd.DataFrame) -> pd.Series:
    """Combine private pension contributions and forward-fill within individuals."""
    combined_years = pd.Series(
        private_rente_data["private_rente_contribution_m_2013"].mask(
            private_rente_data["survey_year"] == 2018,  # noqa: PLR2004
            private_rente_data["private_rente_contribution_m_2018"],
        ),
        name="private_rente_contribution_m",
    )

    data = pd.concat([private_rente_data["p_id"], combined_years], axis=1)
    out = data.groupby("p_id")["private_rente_contribution_m"].ffill()
    return convert_to_float(out)


def _calculate_frailty(frailty_inputs: pd.DataFrame) -> pd.Series:
    return convert_to_float(frailty_inputs.mean(axis=1))


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:  # noqa: PLR0915
    """Create cleaned variables from the pl module.

    Args:
        raw_data: The raw pl data.

    Returns:
        The processed pl data.
    """
    out = pd.DataFrame()
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    out["person_number_surveyed"] = object_to_int(raw_data["pnr"])
    out["years_worked_last_job"] = object_to_int(raw_data["plb0301"])
    out["months_worked_last_job"] = object_to_float(raw_data["plb0302"])
    out["hourly_wage_current"] = object_to_float(raw_data["plh0354_h"])

    # non-working or partly working conditions
    out["altersteilzeit"] = object_to_bool_categorical(
        series=raw_data["plb0103"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_2001"] = object_to_bool_categorical(
        series=raw_data["plb0179"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["employment_ended_reason_pl"] = object_to_str_categorical(
        raw_data["plb0304_v14"]
    )
    out["employment_ended_reason_1999_pl"] = object_to_str_categorical(
        raw_data["plb0304_v13"]
    )
    out["employment_ended_business_closure"] = object_to_str_categorical(
        raw_data["plb0304_v11"]
    )
    out["active_work_search_last_four_weeks"] = object_to_bool_categorical(
        series=raw_data["plb0424_v2"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_type"] = object_to_str_categorical(raw_data["plb0460"])
    out["net_labor_income_m_average"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["plb0471_h"], value=0)
    )
    out["mutterschaftsgeld_received_pl"] = object_to_bool_categorical(
        series=raw_data["plc0126_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
    )
    out["arbeitslosengeld_received_last_month"] = object_to_bool_categorical(
        series=raw_data["plc0130_v1"],
        renaming={"[1] Ja": True},
        ordered=True,
    )
    # bezog arbeitslosengeld m3-m5 available 2017 through 2020
    out["arbeitslosengeld_received_months_3_to_5"] = object_to_bool_categorical(
        series=raw_data["plc0130_v2"],
        renaming={"[1] Ja": True, 2: False},
        ordered=True,
    )
    out["mutterschaftsgeld_received_last_month"] = object_to_bool_categorical(
        series=raw_data["plc0152_v1"],
        renaming={"[1] Ja": True},
    )
    out["mutterschaftsgeld_received_last_month_m"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["plc0153_h"], value=0)
    )
    out["mutterschaftsgeld_received_m"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["plc0155_h"], value=0)
    )
    out["kindesunterhalt_received_m_pl"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["plc0178"], value=0)
    )

    # private pension plan
    out["paid_into_private_rente"] = object_to_bool_categorical(
        series=raw_data["plc0437"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["private_rente_number_of_months_paid_in"] = object_to_int(raw_data["plc0438"])
    out["private_rente_contribution_m_2013"] = _private_rente_beitrag_m_ein_umfragejahr(
        private_rente_beitrag_jahr=raw_data["plc0439_v1"],
        eingezahlte_monate=out["private_rente_number_of_months_paid_in"],
        eingezahlt=out["paid_into_private_rente"],
        survey_year=2013,
    )
    out["private_rente_contribution_m_2018"] = _private_rente_beitrag_m_ein_umfragejahr(
        private_rente_beitrag_jahr=raw_data["plc0439_v2"],
        eingezahlte_monate=out["private_rente_number_of_months_paid_in"],
        eingezahlt=out["paid_into_private_rente"],
        survey_year=2018,
    )
    out["private_rente_contribution_m"] = _private_rente_beitrag_m(
        out[
            [
                "p_id",
                "survey_year",
                "private_rente_contribution_m_2013",
                "private_rente_contribution_m_2018",
            ]
        ],
    )

    # own old-age / disability pension. The questionnaire (Q121) does not split
    # Altersrente from Erwerbsminderungsrente — "Deutsche Rentenversicherung" is a
    # single row. Any EM-Rente inference is project-specific (it depends on the
    # policy environment) and belongs in the consuming project, not this general
    # cleaning library — e.g. flag EM-Rente when this is True and age is below the
    # GETTSIM earliest old-age claiming age for the person's cohort and sex (an own
    # pension below that age cannot be an old-age pension).
    # SPOT-CHECK on data: the `plc0232_h` Ja/Nein label strings (harmonized
    # variables sometimes carry English "[1] Yes" / "[2] No").
    out["receives_own_pension"] = object_to_bool_categorical(
        series=raw_data["plc0232_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["own_pension_gross_m"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["plc0233_v2"], value=0)
    )

    # health and medical characteristics
    out["type_of_health_insurance_1999_to_2020"] = object_to_str_categorical(
        raw_data["ple0097_v1"]
    )
    # there is no information on the type of health insurance in 2021
    out["type_of_health_insurance_2022"] = translate_categories(
        object_to_str_categorical(raw_data["ple0097_v2"]), _HEALTH_INSURANCE_2022_EN
    )
    out["motor_disability"] = create_dummy(
        series=raw_data["plj0582"],
        value_for_comparison=1,
    )
    out["disability_degree"] = object_to_int(
        replace_not_applicable_answer(series=raw_data["ple0041_h"], value=0)
    )
    # Officially recognized reduced earning capacity OR severe disability (yes/no),
    # SOEP `ple0040` (Q128). It bundles Erwerbsminderung and Schwerbehinderung; the
    # GdB degree (`disability_degree` above) isolates the Schwerbehinderung part
    # (GdB >= 50). Disentangle / compose downstream — e.g. a "cannot work" signal
    # that is True only when this holds and the GdB is below 50, dropping pure
    # Schwerbehinderung (who may still work). SPOT-CHECK on data: the "[1] Ja" /
    # "[2] Nein" label strings.
    out["reduced_earning_capacity_or_severely_disabled"] = object_to_bool_categorical(
        series=raw_data["ple0040"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["med_difficulty_stairs_pl"] = object_to_str_categorical(
        raw_data["ple0004"],
        renaming={
            "[3] Gar nicht": "Not at all",
            "[2] Ein wenig": "A little",
            "[1] Stark": "A lot",
        },
        ordered=True,
    )
    out["med_difficulty_demanding_activities_pl"] = object_to_str_categorical(
        series=raw_data["ple0005"],
        renaming={
            "[3] Gar nicht": "Not at all",
            "[2] Ein wenig": "A little",
            "[1] Stark": "A lot",
        },
        ordered=True,
    )
    out["med_height_pl"] = object_to_float(raw_data["ple0006"])
    out["med_weight_pl"] = object_to_float(raw_data["ple0007"])
    out["bmi_pl"] = out["med_weight_pl"] / ((out["med_height_pl"] / 100) ** 2)
    out["obese_pl"] = create_dummy(
        series=out["bmi_pl"], value_for_comparison=30, comparison_type="geq"
    )
    out["med_subjective_status_pl"] = object_to_str_categorical(
        series=raw_data["ple0008"],
        renaming={
            "[1] Sehr gut": "Very good",
            "[2] Gut": "Good",
            "[3] Zufriedenstellend": "Satisfactory",
            "[4] Weniger gut": "Poor",
            "[5] Schlecht": "Bad",
        },
        ordered=True,
    )
    out["med_sleep_disorder_pl"] = object_to_bool_categorical(
        series=raw_data["ple0011_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_diabetes_pl"] = object_to_bool_categorical(
        series=raw_data["ple0012_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_asthma_pl"] = object_to_bool_categorical(
        raw_data["ple0013_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_heart_disease_pl"] = object_to_bool_categorical(
        series=raw_data["ple0014_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_cancer_pl"] = object_to_bool_categorical(
        series=raw_data["ple0015_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_stroke_pl"] = object_to_bool_categorical(
        series=raw_data["ple0016_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_migraine_pl"] = object_to_bool_categorical(
        series=raw_data["ple0017_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_hypertension_pl"] = object_to_bool_categorical(
        series=raw_data["ple0018_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_depression_pl"] = object_to_bool_categorical(
        series=raw_data["ple0019_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_dementia_pl"] = object_to_bool_categorical(
        series=raw_data["ple0020_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_joint_disease_pl"] = object_to_bool_categorical(
        series=raw_data["ple0021_v1"],
        renaming={"[1] genannt": True},
    )
    out["med_back_pl"] = object_to_bool_categorical(
        series=raw_data["ple0022_v1"],
        renaming={"[1] genannt": True},
    )
    out["med_other_pl"] = object_to_bool_categorical(
        series=raw_data["ple0023_v1"],
        renaming={"[1] genannt": True},
        ordered=True,
    )
    out["med_smoker_pl"] = object_to_bool_categorical(
        series=raw_data["ple0081_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    frailty_inputs = out[
        [
            "med_sleep_disorder_pl",
            "med_diabetes_pl",
            "med_asthma_pl",
            "med_heart_disease_pl",
            "med_cancer_pl",
            "med_stroke_pl",
            "med_migraine_pl",
            "med_hypertension_pl",
            "med_depression_pl",
            "med_dementia_pl",
            "med_joint_disease_pl",
            "med_back_pl",
            "med_other_pl",
            "med_smoker_pl",
        ]
    ].assign(
        med_schwierigkeiten_treppen_dummy=create_dummy(
            series=out["med_difficulty_stairs_pl"],
            value_for_comparison=["A little", "A lot"],
            comparison_type="isin",
        ),
        med_schwierigkeiten_taten_dummy=create_dummy(
            series=out["med_difficulty_demanding_activities_pl"],
            value_for_comparison=["A little", "A lot"],
            comparison_type="isin",
        ),
        med_subjective_status_dummy=create_dummy(
            series=out["med_subjective_status_pl"],
            value_for_comparison=["Satisfactory", "Poor", "Bad"],
            comparison_type="isin",
        ),
    )
    out["frailty_pl"] = _calculate_frailty(frailty_inputs=frailty_inputs)

    # personal positions, norms, and political variables
    out["political_spectrum_left_to_right"] = object_to_int(
        series=raw_data["plh0004"],
        renaming={
            "[0] 0 ganz links": 0,
            "[1] 1": 1,
            "[2] 2": 2,
            "[3] 3": 3,
            "[4] 4": 4,
            "[5] 5": 5,
            "[6] 6": 6,
            "[7] 7": 7,
            "[8] 8": 8,
            "[9] 9": 9,
            "[10] 10 ganz rechts": 10,
        },
    )
    out["political_interest"] = object_to_str_categorical(
        series=raw_data["plh0007"],
        renaming={
            "[1] Sehr stark": "Very strong",
            "[2] Stark": "Strong",
            "[3] Nicht so stark": "Not so strong",
            "[4] Ueberhaupt nicht": "Not at all",
        },
        ordered=True,
    )
    out["party_affiliation_dummy"] = create_dummy(
        series=raw_data["plh0011_h"],
        value_for_comparison="[1] Yes",
        comparison_type="equal",
    )
    out["party_affiliation"] = object_to_str_categorical(raw_data["plh0012_h"])
    out["party_affiliation_intensity"] = object_to_str_categorical(
        series=raw_data["plh0013_h"],
        renaming={
            "[1] Sehr stark": "Very strong",
            "[2] Ziemlich stark": "Fairly strong",
            "[3] Maessig": "Moderate",
            "[4] Ziemlich schwach": "Fairly weak",
            "[5] Sehr schwach": "Very weak",
        },
        ordered=True,
    )
    out["relevance_career"] = object_to_str_categorical(
        series=raw_data["plh0107"],
        renaming={
            "[1] 1 Sehr wichtig": "Very important",
            "[2] 2 Wichtig": "Important",
            "[3] 3 Weniger wichtig": "Less important",
            "[4] 4 Ganz unwichtig": "Not at all important",
        },
        ordered=True,
    )
    out["relevance_children"] = object_to_str_categorical(
        series=raw_data["plh0110"],
        renaming={
            "[1] 1 Sehr wichtig": "Very important",
            "[2] 2 Wichtig": "Important",
            "[3] 3 Weniger wichtig": "Less important",
            "[4] 4 Ganz unwichtig": "Not at all important",
        },
        ordered=True,
    )
    out["life_satisfaction_low_to_high"] = object_to_int(
        series=raw_data["plh0182"],
        renaming={
            "[0] 0 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 0,
            "[1] 1 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 1,
            "[2] 2 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 2,
            "[3] 3 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 3,
            "[4] 4 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 4,
            "[5] 5 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 5,
            "[6] 6 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 6,
            "[7] 7 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 7,
            "[8] 8 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 8,
            "[9] 9 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 9,
            "[10] 10 Zufrieden: Skala 0-Niedrig bis 10-Hoch": 10,
        },
    )
    out["general_trust"] = object_to_str_categorical(
        series=raw_data["plh0192"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Lehne eher ab": "Somewhat disagree",
            "[4] Lehne voll ab": "Strongly disagree",
        },
        ordered=True,
    )
    out["confession"] = object_to_str_categorical(raw_data["plh0258_v9"])
    out["confession_specific"] = object_to_str_categorical(raw_data["plh0258_h"])
    out["norm_child_suffers_under_6"] = object_to_str_categorical(
        series=raw_data["plh0298_v1"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["norm_child_suffers_under_6_low_to_high_2018"] = object_to_int(
        series=raw_data["plh0298_v2"],
        renaming={
            "[1] Stimme ueberhaupt nicht zu": 1,
            "[2] Skala von 1-7": 2,
            "[3] Skala von 1-7": 3,
            "[4] Skala von 1-7": 4,
            "[5] Skala von 1-7": 5,
            "[6] Skala von 1-7": 6,
            "[7] Stimme voll zu": 7,
        },
    )
    out["norm_marry_when_together"] = object_to_str_categorical(
        series=raw_data["plh0300_v1"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["norm_marry_when_together_low_to_high_2018"] = object_to_int(
        series=raw_data["plh0300_v2"],
        renaming={
            "[1] Stimme ueberhaupt nicht zu": 1,
            "[2] Skala von 1-7": 2,
            "[3] Skala von 1-7": 3,
            "[4] Skala von 1-7": 4,
            "[5] Skala von 1-7": 5,
            "[6] Skala von 1-7": 6,
            "[7] Stimme voll zu": 7,
        },
    )
    out["norm_women_family_priority"] = object_to_str_categorical(
        series=raw_data["plh0301"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["norm_child_suffers_under_3"] = object_to_str_categorical(
        series=raw_data["plh0302_v1"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["norm_child_suffers_under_3_low_to_high_2018"] = object_to_int(
        series=raw_data["plh0302_v2"],
        renaming={
            "[1] Stimme ueberhaupt nicht zu": 1,
            "[2] Skala von 1-7": 2,
            "[3] Skala von 1-7": 3,
            "[4] Skala von 1-7": 4,
            "[5] Skala von 1-7": 5,
            "[6] Skala von 1-7": 6,
            "[7] Stimme voll zu": 7,
        },
    )
    out["norm_men_chores"] = object_to_str_categorical(
        series=raw_data["plh0303"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["norm_child_suffers_father_career"] = object_to_str_categorical(
        series=raw_data["plh0304"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["norm_genders_similar"] = object_to_str_categorical(
        series=raw_data["plh0308_v1"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["norm_genders_similar_low_to_high_2018"] = object_to_int(
        series=raw_data["plh0308_v2"],
        renaming={
            "[1] Stimme ueberhaupt nicht zu": 1,
            "[2] Skala von 1-7": 2,
            "[3] Skala von 1-7": 3,
            "[4] Skala von 1-7": 4,
            "[5] Skala von 1-7": 5,
            "[6] Skala von 1-7": 6,
            "[7] Stimme voll zu": 7,
        },
    )
    out["norm_career_mothers_same_warmth"] = object_to_str_categorical(
        series=raw_data["plh0309"],
        renaming={
            "[1] Stimme voll zu": "Strongly agree",
            "[2] Stimme eher zu": "Somewhat agree",
            "[3] Stimme eher nicht zu": "Somewhat disagree",
            "[4] Stimme ueberhaupt nicht zu": "Strongly disagree",
        },
        ordered=True,
    )
    out["importance_faith"] = object_to_str_categorical(
        series=raw_data["plh0343_v1"],
        renaming={
            "[1] sehr wichtig": "Very important",
            "[2] wichtig": "Important",
            "[3] weniger wichtig": "Less important",
            "[4] ganz unwichtig": "Not at all important",
        },
        ordered=True,
    )
    out["importance_faith_v2"] = object_to_str_categorical(
        series=raw_data["plh0343_v2"],
        renaming={
            "[1] sehr wichtig": "Very important",
            "[2] wichtig": "Important",
            "[3] weniger wichtig": "Less important",
            "[4] ganz unwichtig": "Not at all important",
        },
        ordered=True,
    )
    out["trust_public_admin_low_to_high"] = object_to_int(
        series=raw_data["plm0672"],
        renaming={
            "[0] Skala von 0-10: Überhaupt kein Vertrauen": 0,
            "[1] Skala von 0-10": 1,
            "[2] Skala von 0-10": 2,
            "[3] Skala von 0-10": 3,
            "[4] Skala von 0-10": 4,
            "[5] Skala von 0-10": 5,
            "[6] Skala von 0-10": 6,
            "[7] Skala von 0-10": 7,
            "[8] Skala von 0-10": 8,
            "[9] Skala von 0-10": 9,
            "[10] Skala von 0-10: Volles Vertrauen": 10,
        },
    )
    out["trust_government_low_to_high"] = object_to_int(
        series=raw_data["plm0673"],
        renaming={
            "[0] Skala von 0-10: Überhaupt kein Vertrauen": 0,
            "[1] Skala von 0-10": 1,
            "[2] Skala von 0-10": 2,
            "[3] Skala von 0-10": 3,
            "[4] Skala von 0-10": 4,
            "[5] Skala von 0-10": 5,
            "[6] Skala von 0-10": 6,
            "[7] Skala von 0-10": 7,
            "[8] Skala von 0-10": 8,
            "[9] Skala von 0-10": 9,
            "[10] Skala von 0-10: Volles Vertrauen": 10,
        },
    )

    # time spent
    # work
    out["hours_work_workday"] = object_to_float(raw_data["pli0038_h"])
    out["hours_work_sat"] = object_to_float(raw_data["pli0003_h"])
    out["hours_work_sun"] = object_to_float(raw_data["pli0007_h"])

    # hobbies
    out["hours_hobbies_workday"] = object_to_float(raw_data["pli0051"])
    out["hours_hobbies_sat"] = object_to_float(raw_data["pli0036"])
    out["hours_hobbies_sun"] = object_to_float(raw_data["pli0010"])

    # errands
    out["hours_errands_workday"] = object_to_float(raw_data["pli0040"])
    out["hours_errands_sat"] = object_to_float(raw_data["pli0054"])
    out["hours_errands_sun"] = object_to_float(raw_data["pli0011"])

    # housework
    out["hours_housework_workday"] = object_to_float(raw_data["pli0043_h"])
    out["hours_housework_sat"] = object_to_float(raw_data["pli0012_h"])
    out["hours_housework_sun"] = object_to_float(raw_data["pli0016_h"])

    # childcare
    out["hours_childcare_workday"] = object_to_float(raw_data["pli0044_h"])
    out["hours_childcare_sat"] = object_to_float(raw_data["pli0019_h"])
    out["hours_childcare_sun"] = object_to_float(raw_data["pli0022_h"])

    # care
    out["hours_care_workday"] = object_to_float(raw_data["pli0046"])
    out["hours_care_sat"] = object_to_float(raw_data["pli0055"])
    out["hours_care_sun"] = object_to_float(raw_data["pli0057"])

    # repairs
    out["hours_repairs_workday"] = object_to_float(raw_data["pli0049_h"])
    out["hours_repairs_sat"] = object_to_float(raw_data["pli0031_h"])
    out["hours_repairs_sun"] = object_to_float(raw_data["pli0034_v4"])

    # sleep
    out["hours_sleep_workday"] = object_to_float(raw_data["pli0059"])
    out["hours_sleep_weekend"] = object_to_float(raw_data["pli0060"])
    return out
