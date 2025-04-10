"""Functions to pre-process variables for a raw pl dataset."""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_float,
    object_to_int,
    object_to_int_categorical,
    object_to_str_categorical,
)


def _priv_rente_beitr_year(
    priv_rente_beitr_year: "pd.Series[int]",
    eingezahlte_monate: "pd.Series[int]",
    eingezahlt: "pd.Series[pd.Categorical]",
    survey_year: int,
) -> "pd.Series[int]":
    relevant_suvery_year = 2018
    if survey_year == relevant_suvery_year:
        out = object_to_int(priv_rente_beitr_year)
        out /= 100
    else:
        out = object_to_float(priv_rente_beitr_year)
    out *= eingezahlte_monate / 12
    return out.where(eingezahlt.astype("bool[pyarrow]"), 0)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pl dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["lfd_pnr"] = object_to_int(raw_data["pnr"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["altersteilzeit_02_14"] = object_to_bool_categorical(
        raw_data["plb0103"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_2001"] = object_to_bool_categorical(
        raw_data["plb0179"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["dauer_letzte_stelle_j"] = object_to_int(
        raw_data["plb0301"],
    )
    out["dauer_letzte_stelle_m"] = object_to_float(
        raw_data["plb0302"],
    )
    out["letzte_stelle_betriebsstilll"] = object_to_str_categorical(
        raw_data["plb0304_v11"],
    )
    out["letzte_stelle_grund_1999"] = object_to_str_categorical(raw_data["plb0304_v13"])
    out["letzte_stelle_grund"] = object_to_str_categorical(raw_data["plb0304_v14"])
    out["actv_work_search"] = object_to_bool_categorical(
        raw_data["plb0424_v2"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_art"] = object_to_str_categorical(raw_data["plb0460"])
    out["wage_employee_m_prev"] = object_to_float(
        raw_data["plb0471_h"],
    )
    out["mutterschaftsgeld_prev"] = object_to_bool_categorical(
        raw_data["plc0126_v1"],
        renaming={"[1] Ja": True},
    )
    out["arbeitslosengeld_empf"] = object_to_bool_categorical(
        raw_data["plc0130_v1"],
        renaming={"[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_m3_m5_empf"] = object_to_str_categorical(
        raw_data["plc0130_v2"],
    )
    out["mutterschaftsgeld_vorm_pl"] = object_to_bool_categorical(
        raw_data["plc0152_v1"],
        renaming={"[1] Ja": True},
    )
    out["mutterschaftsgeld_brutto_m"] = object_to_float(
        raw_data["plc0153_h"],
    )
    out["mutterschaftsgeld_betrag_pl_prev"] = object_to_float(
        raw_data["plc0155_h"],
    )
    out["child_alimony_before_2016"] = object_to_int(
        raw_data["plc0178"],
    )
    out["in_priv_rente_eingezahlt"] = object_to_bool_categorical(
        raw_data["plc0437"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["in_priv_rente_eingezahlt_monate"] = object_to_int(
        raw_data["plc0438"],
    )
    out["priv_rente_beitr_2013_m"] = _priv_rente_beitr_year(
        raw_data["plc0439_v1"],
        out["in_priv_rente_eingezahlt_monate"],
        out["in_priv_rente_eingezahlt"],
        2013,
    )
    out["priv_rente_beitr_2018_m"] = _priv_rente_beitr_year(
        raw_data["plc0439_v2"],
        out["in_priv_rente_eingezahlt_monate"],
        out["in_priv_rente_eingezahlt"],
        2018,
    )

    out["med_pl_schw_treppen"] = object_to_int_categorical(
        raw_data["ple0004"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_schw_taten"] = object_to_int_categorical(
        raw_data["ple0005"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_groesse"] = object_to_int(
        raw_data["ple0006"],
    )
    out["med_pl_gewicht"] = object_to_int(
        raw_data["ple0007"],
    )
    out["med_pl_subj_status"] = object_to_int_categorical(
        raw_data["ple0008"],
        renaming={
            "[1] Sehr gut": 1,
            "[2] Gut": 2,
            "[3] Zufriedenstellend": 3,
            "[4] Weniger gut": 4,
            "[5] Schlecht": 5,
        },
        ordered=True,
    )
    out["med_pl_schlaf"] = object_to_bool_categorical(
        raw_data["ple0011"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_diabetes"] = object_to_bool_categorical(
        raw_data["ple0012"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_asthma"] = object_to_bool_categorical(
        raw_data["ple0013"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_herzkr"] = object_to_bool_categorical(
        raw_data["ple0014"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_krebs"] = object_to_bool_categorical(
        raw_data["ple0015"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_schlaganf"] = object_to_bool_categorical(
        raw_data["ple0016"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_migraene"] = object_to_bool_categorical(
        raw_data["ple0017"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_bluthdrck"] = object_to_bool_categorical(
        raw_data["ple0018"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_depressiv"] = object_to_bool_categorical(
        raw_data["ple0019"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_demenz"] = object_to_bool_categorical(
        raw_data["ple0020"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_gelenk"] = object_to_bool_categorical(
        raw_data["ple0021"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_ruecken"] = object_to_bool_categorical(
        raw_data["ple0022"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_sonst"] = object_to_bool_categorical(
        raw_data["ple0023"],
        renaming={"[1] Ja": True},
    )
    out["disability_degree"] = object_to_int(raw_data["ple0041"]).fillna(0)
    out["med_pl_raucher"] = object_to_bool_categorical(
        raw_data["ple0081_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
    )
    out["art_kv"] = object_to_str_categorical(raw_data["ple0097"])
    out["politics_left_right"] = object_to_int_categorical(
        raw_data["plh0004"],
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
    out["interest_politics"] = object_to_int_categorical(
        raw_data["plh0007"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Stark": 2,
            "[3] Nicht so stark": 3,
            "[4] Ueberhaupt nicht": 4,
        },
    )
    out["party_affiliation_any"] = object_to_str_categorical(raw_data["plh0011_h"])
    out["party_affiliation"] = object_to_str_categorical(raw_data["plh0012_h"])
    out["party_affiliation_intensity"] = object_to_int_categorical(
        raw_data["plh0013_h"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Ziemlich stark": 2,
            "[3] Maessig": 3,
            "[4] Ziemlich schwach": 4,
            "[5] Sehr schwach": 5,
        },
    )
    out["importance_career"] = object_to_int_categorical(
        raw_data["plh0107"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["importance_children"] = object_to_int_categorical(
        raw_data["plh0110"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["lebenszufriedenheit"] = object_to_int_categorical(
        raw_data["plh0182"],
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
    out["general_trust"] = object_to_int_categorical(
        raw_data["plh0192"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Lehne eher ab": 3,
            "[4] Lehne voll ab": 4,
        },
    )
    out["confession"] = object_to_str_categorical(raw_data["plh0258_h"])
    out["confession_any"] = object_to_str_categorical(raw_data["plh0258_v9"])
    out["norm_child_suffers_under_6"] = object_to_int_categorical(
        raw_data["plh0298_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_6_2018"] = object_to_int_categorical(
        raw_data["plh0298_v2"],
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
    out["norm_marry_when_together"] = object_to_int_categorical(
        raw_data["plh0300_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_marry_when_together_2018"] = object_to_int_categorical(
        raw_data["plh0300_v2"],
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
    out["norm_women_family_priority"] = object_to_int_categorical(
        raw_data["plh0301"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3"] = object_to_int_categorical(
        raw_data["plh0302_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3_2018"] = object_to_int_categorical(
        raw_data["plh0302_v2"],
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
    out["norm_men_chores"] = object_to_int_categorical(
        raw_data["plh0303"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_ch_suffers_father_career"] = object_to_int_categorical(
        raw_data["plh0304"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar"] = object_to_int_categorical(
        raw_data["plh0308_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar_2018"] = object_to_int_categorical(
        raw_data["plh0308_v2"],
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
    out["norm_career_mothers_same_warmth"] = object_to_int_categorical(
        raw_data["plh0309"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["importance_faith"] = object_to_int_categorical(
        raw_data["plh0343_v1"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["importance_faith_v2"] = object_to_int_categorical(
        raw_data["plh0343_v2"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["h_wage_pl"] = object_to_float(raw_data["plh0354_h"])
    out["hours_work_sat"] = object_to_float(raw_data["pli0003_h"])
    out["hours_work_sun"] = object_to_float(raw_data["pli0007_h"])
    out["hours_hobbies_sun"] = object_to_float(raw_data["pli0010"])
    out["hours_errands_sun"] = object_to_int(raw_data["pli0011"])
    out["hours_housework_sat"] = object_to_float(raw_data["pli0012_h"])
    out["hours_housework_sun"] = object_to_float(raw_data["pli0016_h"])
    out["hours_childcare_sat"] = object_to_float(raw_data["pli0019_h"])
    out["hours_childcare_sun"] = object_to_float(raw_data["pli0022_h"])
    out["hours_repairs_sat"] = object_to_float(raw_data["pli0031_h"])
    out["hours_repairs_sun"] = object_to_float(raw_data["pli0034_v4"])
    out["hours_hobbies_sat"] = object_to_float(raw_data["pli0036"])
    out["hours_work_workday"] = object_to_float(raw_data["pli0038_h"])
    out["hours_errands_workday"] = object_to_float(raw_data["pli0040"])
    out["hours_housework_workday"] = object_to_float(raw_data["pli0043_h"])
    out["hours_childcare_workday"] = object_to_float(raw_data["pli0044_h"])
    out["hours_care_workday"] = object_to_float(raw_data["pli0046"])
    out["hours_repairs_workday"] = object_to_float(raw_data["pli0049_h"])
    out["hours_hobbies_workday"] = object_to_float(raw_data["pli0051"])
    out["hours_errands_sat"] = object_to_float(raw_data["pli0054"])
    out["hours_care_sat"] = object_to_float(raw_data["pli0055"])
    out["hours_care_sun"] = object_to_float(raw_data["pli0057"])
    out["hours_sleep_workday"] = object_to_float(raw_data["pli0059"])
    out["hours_sleep_weekend"] = object_to_float(raw_data["pli0060"])
    out["motor_disability"] = object_to_bool_categorical(
        raw_data["plj0582"],
        renaming={1: True},
    )
    out["trust_public_admin"] = object_to_int_categorical(
        raw_data["plm0672"],
        renaming={
            "[0] UEberhaupt kein Vertrauen": 0,
            "[1] Skala von0-10": 1,
            "[2] Skala von0-10": 2,
            "[3] Skala von0-10": 3,
            "[4] Skala von0-10": 4,
            "[5] Skala von0-10": 5,
            "[6] Skala von0-10": 6,
            "[7] Skala von0-10": 7,
            "[8] Skala von0-10": 8,
            "[9] Skala von0-10": 9,
            "[10] Volles Vertrauen": 10,
        },
        ordered=True,
    )
    out["trust_government"] = object_to_int_categorical(
        raw_data["plm0673"],
        renaming={
            "[0] UEberhaupt kein Vertrauen": 0,
            "[1] Skala von0-10": 1,
            "[2] Skala von0-10": 2,
            "[3] Skala von0-10": 3,
            "[4] Skala von0-10": 4,
            "[5] Skala von0-10": 5,
            "[6] Skala von0-10": 6,
            "[7] Skala von0-10": 7,
            "[8] Skala von0-10": 8,
            "[9] Skala von0-10": 9,
            "[10] Volles Vertrauen": 10,
        },
        ordered=True,
    )

    return out
