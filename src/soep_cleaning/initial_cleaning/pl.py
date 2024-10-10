from soep_cleaning.config import pd
from soep_cleaning.utilities import (
    apply_lowest_int_dtype,
    bool_categorical,
    float_categorical_to_float,
    int_categorical_to_int,
    str_categorical,
    str_categorical_to_int_categorical,
)


def _prv_rente_beitr_year(
    prv_rente_beitr_year: "pd.Series[int]",
    eingezahlte_monate: "pd.Series[int]",
    eingezahlt: "pd.Series[pd.Categorical]",
    year: int,
) -> "pd.Series[int]":
    if year == 2018:
        out = int_categorical_to_int(prv_rente_beitr_year)
        out /= 100
    else:
        out = float_categorical_to_float(prv_rente_beitr_year)
    out *= eingezahlte_monate / 12
    return out.where(~eingezahlt.astype("bool[pyarrow]"), 0)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pl dataset."""
    out = pd.DataFrame()
    out["p_id"] = int_categorical_to_int(raw["pid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])
    out["lfd_pnr"] = int_categorical_to_int(raw["pnr"])
    out["year"] = int_categorical_to_int(raw["syear"])
    out["altersteilzeit_02_14"] = bool_categorical(
        raw["plb0103"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_2001"] = bool_categorical(
        raw["plb0179"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["dauer_letzte_stelle_j"] = int_categorical_to_int(
        raw["plb0301"],
    )
    out["dauer_letzte_stelle_m"] = float_categorical_to_float(
        raw["plb0302"],
    )
    out["letzte_stelle_betriebsstilll"] = str_categorical(raw["plb0304_v11"])
    out["letzte_stelle_grund_1999"] = str_categorical(raw["plb0304_v13"])
    out["letzte_stelle_grund"] = str_categorical(raw["plb0304_v14"])
    out["actv_work_search"] = bool_categorical(
        raw["plb0424_v2"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_art"] = str_categorical(raw["plb0460"])
    out["wage_employee_m_prev"] = float_categorical_to_float(
        raw["plb0471_h"],
    )
    out["mutterschaftsgeld_prev"] = bool_categorical(
        raw["plc0126_v1"],
        renaming={"[1] Ja": True},
    )
    out["arbeitslosengeld_empf"] = bool_categorical(
        raw["plc0130_v1"],
        renaming={"[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_m3_m5_empf"] = str_categorical(raw["plc0130_v2"])
    out["mutterschaftsgeld_vorm_pl"] = bool_categorical(
        raw["plc0152_v1"],
        renaming={"[1] Ja": True},
    )
    out["mutterschaftsgeld_brutto_m"] = float_categorical_to_float(
        raw["plc0153_h"],
    )
    out["mutterschaftsgeld_betrag_pl_prev"] = float_categorical_to_float(
        raw["plc0155_h"],
    )
    out["child_alimony_before_2016"] = int_categorical_to_int(
        raw["plc0178"],
    )
    out["in_priv_rente_eingezahlt"] = bool_categorical(
        raw["plc0437"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["in_priv_rente_eingezahlt_monate"] = int_categorical_to_int(
        raw["plc0438"],
    )
    out["prv_rente_beitr_2013_m"] = _prv_rente_beitr_year(
        raw["plc0439_v1"],
        out["in_priv_rente_eingezahlt_monate"],
        out["in_priv_rente_eingezahlt"],
        2013,
    )
    out["prv_rente_beitr_2018_m"] = _prv_rente_beitr_year(
        raw["plc0439_v2"],
        out["in_priv_rente_eingezahlt_monate"],
        out["in_priv_rente_eingezahlt"],
        2018,
    )
    out["med_pl_schw_treppen"] = str_categorical_to_int_categorical(
        raw["ple0004"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_schw_taten"] = str_categorical_to_int_categorical(
        raw["ple0005"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_groesse"] = int_categorical_to_int(
        raw["ple0006"],
    )
    out["med_pl_gewicht"] = int_categorical_to_int(
        raw["ple0007"],
    )
    out["med_pl_subj_status"] = str_categorical_to_int_categorical(
        raw["ple0008"],
        renaming={
            "[1] Sehr gut": 1,
            "[2] Gut": 2,
            "[3] Zufriedenstellend": 3,
            "[4] Weniger gut": 4,
            "[5] Schlecht": 5,
        },
        ordered=True,
    )
    out["med_pl_schlaf"] = bool_categorical(raw["ple0011"], renaming={"[1] Ja": True})
    out["med_pl_diabetes"] = bool_categorical(raw["ple0012"], renaming={"[1] Ja": True})
    out["med_pl_asthma"] = bool_categorical(raw["ple0013"], renaming={"[1] Ja": True})
    out["med_pl_herzkr"] = bool_categorical(raw["ple0014"], renaming={"[1] Ja": True})
    out["med_pl_krebs"] = bool_categorical(raw["ple0015"], renaming={"[1] Ja": True})
    out["med_pl_schlaganf"] = bool_categorical(
        raw["ple0016"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_migraene"] = bool_categorical(raw["ple0017"], renaming={"[1] Ja": True})
    out["med_pl_bluthdrck"] = bool_categorical(
        raw["ple0018"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_depressiv"] = bool_categorical(
        raw["ple0019"],
        renaming={"[1] Ja": True},
    )
    out["med_pl_demenz"] = bool_categorical(raw["ple0020"], renaming={"[1] Ja": True})
    out["med_pl_gelenk"] = bool_categorical(raw["ple0021"], renaming={"[1] Ja": True})
    out["med_pl_ruecken"] = bool_categorical(raw["ple0022"], renaming={"[1] Ja": True})
    out["med_pl_sonst"] = bool_categorical(raw["ple0023"], renaming={"[1] Ja": True})
    out["disability_degree"] = int_categorical_to_int(raw["ple0041"]).fillna(0)
    out["med_pl_raucher"] = bool_categorical(
        raw["ple0081_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
    )
    out["art_kv"] = str_categorical(raw["ple0097"])
    out["politics_left_right"] = str_categorical_to_int_categorical(
        raw["plh0004"],
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
    out["interest_politics"] = str_categorical_to_int_categorical(
        raw["plh0007"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Stark": 2,
            "[3] Nicht so stark": 3,
            "[4] Ueberhaupt nicht": 4,
        },
    )
    out["party_affiliation_any"] = str_categorical(raw["plh0011_h"])
    out["party_affiliation"] = str_categorical(raw["plh0012_h"])
    out["party_affiliation_intensity"] = str_categorical_to_int_categorical(
        raw["plh0013_h"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Ziemlich stark": 2,
            "[3] Maessig": 3,
            "[4] Ziemlich schwach": 4,
            "[5] Sehr schwach": 5,
        },
    )
    out["importance_career"] = str_categorical_to_int_categorical(
        raw["plh0107"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["importance_children"] = str_categorical_to_int_categorical(
        raw["plh0110"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["lebenszufriedenheit"] = str_categorical_to_int_categorical(
        raw["plh0182"],
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
    out["general_trust"] = str_categorical_to_int_categorical(
        raw["plh0192"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Lehne eher ab": 3,
            "[4] Lehne voll ab": 4,
        },
    )
    out["confession"] = str_categorical(raw["plh0258_h"])
    out["confession_any"] = str_categorical(raw["plh0258_v9"])
    out["norm_child_suffers_under_6"] = str_categorical_to_int_categorical(
        raw["plh0298_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_6_2018"] = str_categorical_to_int_categorical(
        raw["plh0298_v2"],
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
    out["norm_marry_when_together"] = str_categorical_to_int_categorical(
        raw["plh0300_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_marry_when_together_2018"] = str_categorical_to_int_categorical(
        raw["plh0300_v2"],
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
    out["norm_women_family_priority"] = str_categorical_to_int_categorical(
        raw["plh0301"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3"] = str_categorical_to_int_categorical(
        raw["plh0302_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3_2018"] = str_categorical_to_int_categorical(
        raw["plh0302_v2"],
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
    out["norm_men_chores"] = str_categorical_to_int_categorical(
        raw["plh0303"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_ch_suffers_father_career"] = str_categorical_to_int_categorical(
        raw["plh0304"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar"] = str_categorical_to_int_categorical(
        raw["plh0308_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar_2018"] = str_categorical_to_int_categorical(
        raw["plh0308_v2"],
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
    out["norm_career_mothers_same_warmth"] = str_categorical_to_int_categorical(
        raw["plh0309"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["importance_faith"] = str_categorical_to_int_categorical(
        raw["plh0343_v1"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["importance_faith_v2"] = str_categorical_to_int_categorical(
        raw["plh0343_v2"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["h_wage_pl"] = float_categorical_to_float(raw["plh0354_h"])
    out["hours_work_sat"] = float_categorical_to_float(raw["pli0003_h"])
    out["hours_work_sun"] = float_categorical_to_float(raw["pli0007_h"])
    out["hours_hobbies_sun"] = float_categorical_to_float(raw["pli0010"])
    out["hours_errands_sun"] = int_categorical_to_int(raw["pli0011"])
    out["hours_housework_sat"] = float_categorical_to_float(raw["pli0012_h"])
    out["hours_housework_sun"] = float_categorical_to_float(raw["pli0016_h"])
    out["hours_childcare_sat"] = float_categorical_to_float(raw["pli0019_h"])
    out["hours_childcare_sun"] = float_categorical_to_float(raw["pli0022_h"])
    out["hours_repairs_sat"] = float_categorical_to_float(raw["pli0031_h"])
    out["hours_repairs_sun"] = float_categorical_to_float(raw["pli0034_v4"])
    out["hours_hobbies_sat"] = float_categorical_to_float(raw["pli0036"])
    out["hours_work_workday"] = float_categorical_to_float(raw["pli0038_h"])
    out["hours_errands_workday"] = float_categorical_to_float(raw["pli0040"])
    out["hours_housework_workday"] = float_categorical_to_float(raw["pli0043_h"])
    out["hours_childcare_workday"] = float_categorical_to_float(raw["pli0044_h"])
    out["hours_care_workday"] = float_categorical_to_float(raw["pli0046"])
    out["hours_repairs_workday"] = float_categorical_to_float(raw["pli0049_h"])
    out["hours_hobbies_workday"] = float_categorical_to_float(raw["pli0051"])
    out["hours_errands_sat"] = float_categorical_to_float(raw["pli0054"])
    out["hours_care_sat"] = float_categorical_to_float(raw["pli0055"])
    out["hours_care_sun"] = float_categorical_to_float(raw["pli0057"])
    out["hours_sleep_workday"] = float_categorical_to_float(raw["pli0059"])
    out["hours_sleep_weekend"] = float_categorical_to_float(raw["pli0060"])
    out["motor_disability"] = bool_categorical(raw["plj0582"], renaming={1: True})
    out["trust_public_admin"] = str_categorical_to_int_categorical(
        raw["plm0672"],
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
    out["trust_government"] = str_categorical_to_int_categorical(
        raw["plm0673"],
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
