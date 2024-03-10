import pandas as pd

from soep_cleaning.initial_preprocessing.helper import (
    int_categorical_to_int,
    str_categorical,
)
from soep_cleaning.utilities import apply_lowest_int_dtype


def pbrutto(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pbrutto dataset."""
    out = pd.DataFrame()
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["soep_initial_hh_id"] = int_categorical_to_int(raw_data["cid"])
    out["soep_hh_id"] = int_categorical_to_int(raw_data["hid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["birth_year"] = int_categorical_to_int(raw_data["geburt_v2"])
    out["befragungs_status"] = str_categorical(raw_data["befstat_h"])
    out["hh_position_raw"] = str_categorical(raw_data["stell_h"])
    out["bearbeitungserg"] = raw_data[
        "perg"
    ]  # categories [29] and [39] have identical labels, should by cleaned using str_categorical
    out["bearbeitungserg_ausf"] = str_categorical(raw_data["pergz"])
    out["hh_position_raw_last_year"] = str_categorical(raw_data["pzugv"])
    out["teilnahmebereitchaft"] = str_categorical(raw_data["ber"])
    out["bearbeitungserg_alt"] = str_categorical(raw_data["hergs"])

    return out


def pequiv(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pequiv dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = int_categorical_to_int(raw_data["cid"])
    out["soep_hh_id"] = raw_data["hid"].astype(apply_lowest_int_dtype(raw_data["hid"]))
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["gender"] = raw_data["d11102ll"]
    out["age"] = raw_data["d11101"]
    out["hh_size"] = raw_data["d11106"]
    out["anz_minderj_hh1"] = raw_data["d11107"]
    out["annual_work_h_prev"] = raw_data["e11101"]
    out["employment_status_pequiv"] = raw_data["e11102"]
    out["employment_level_pequiv"] = raw_data["e11103"]
    out["einkommen_vor_steuer_hh_prev"] = raw_data["i11101"]
    out["einkommen_nach_steuer_hh_prev"] = raw_data["i11102"]
    out["labor_earnings_prev"] = raw_data["i11110"]
    out["bundesland"] = raw_data["l11101"]
    out["earnings_job_1_prev"] = raw_data["ijob1"]
    out["earnings_job_2_prev"] = raw_data["ijob2"]
    out["self_empl_earnings_prev"] = raw_data["iself"]
    out["arbeitsl_geld_soep_prev"] = raw_data["iunby"]
    out["arbeitsl_hilfe_soep_prev"] = raw_data["iunay"]
    out["unterhaltsgeld_soep_prev"] = raw_data["isuby"]
    out["übergangsgeld_soep_prev"] = raw_data["ieret"]
    out["mschaftsgeld_soep_prev"] = raw_data["imaty"]
    out["student_grants_prev"] = raw_data["istuy"]
    out["alimony_prev"] = raw_data["ialim"]
    out["other_transfers_prev"] = raw_data["ielse"]
    out["christmas_bonus_prev"] = raw_data["ixmas"]
    out["vacation_bonus_prev"] = raw_data["iholy"]
    out["profit_share_prev"] = raw_data["igray"]
    out["other_bonuses_prev"] = raw_data["iothy"]
    out["gov_ret_pay_prev"] = raw_data["igrv1"]
    out["gov_ret_pay_widow_prev"] = raw_data["igrv2"]
    out["rental_income_y_hh_prev"] = raw_data["renty"]
    out["maintenance_c_hh_prev"] = raw_data["opery"]
    out["capital_income_hh_prev"] = raw_data["divdy"]
    out["kindergeld_pequiv_hh_prev"] = raw_data["chspt"]
    out["wohngeld_soep_hh_prev"] = raw_data["house"]
    out["pflegegeld_hh_prev"] = raw_data["nursh"]
    out["soc_assist_hh_prev"] = raw_data["subst"]
    out["soc_assist_spec_hh_prev"] = raw_data["sphlp"]
    out["housing_sup_hh_prev"] = raw_data["hsup"]
    out["soc_miners_pension_prev"] = raw_data["ismp1"]
    out["civ_serv_pension_prev"] = raw_data["iciv1"]
    out["warvictim_pension_prev"] = raw_data["iwar1"]
    out["farmer_pension_prev"] = raw_data["iagr1"]
    out["stat_accident_insurance_prev"] = raw_data["iguv1"]
    out["civ_supp_benefits_prev"] = raw_data["ivbl1"]
    out["company_pen_prev"] = raw_data["icom1"]
    out["private_pen_prev"] = raw_data["iprv1"]
    out["other_pen_prev"] = raw_data["ison1"]
    out["soc_miners_pension_widow_prev"] = raw_data["ismp2"]
    out["civ_serv_pension_widow_prev"] = raw_data["iciv2"]
    out["warvictim_pension_widow_prev"] = raw_data["iwar2"]
    out["farmer_pension_widow_prev"] = raw_data["iagr2"]
    out["stat_accident_insur_widow_prev"] = raw_data["iguv2"]
    out["civ_supp_benefits_widow_prev"] = raw_data["ivbl2"]
    out["company_pen_widow_prev"] = raw_data["icom2"]
    out["other_pen_widow_prev"] = raw_data["ison2"]
    out["private_pen_widow_prev"] = raw_data["iprv2"]
    out["med_pe_krnkhaus"] = raw_data["m11101"]
    out["med_pe_schlaganf"] = raw_data["m11105"]
    out["med_pe_bluthdrck"] = raw_data["m11106"]
    out["med_pe_diabetes"] = raw_data["m11107"]
    out["med_pe_krebs"] = raw_data["m11108"]
    out["med_pe_psych"] = raw_data["m11109"]
    out["med_pe_gelenk"] = raw_data["m11110"]
    out["med_pe_herzkr"] = raw_data["m11111"]
    out["med_pe_schw_treppen"] = raw_data["m11113"]
    out["med_pe_schw_anziehen"] = raw_data["m11115"]
    out["med_pe_schw_bett"] = raw_data["m11116"]
    out["med_pe_schw_einkauf"] = raw_data["m11117"]
    out["med_pe_schw_hausarb"] = raw_data["m11119"]
    out["med_pe_groesse"] = raw_data["m11122"]
    out["med_pe_gewicht"] = raw_data["m11123"]
    out["med_pe_zufrieden"] = raw_data["m11125"]
    out["med_pe_subj_status"] = raw_data["m11126"]
    out["soc_assist_eld_hh_prev"] = raw_data["ssold"]
    out["arbeitsl_geld_2_soep_hh_prev"] = raw_data["alg2"]
    out["kinderzuschlag_pequiv_hh_prev"] = raw_data["adchb"]
    out["adv_child_maint_payment_prev"] = raw_data["iachm"]
    out["childcare_subsidy_hh_prev"] = raw_data["chsub"]
    out["caregiver_alimony_prev"] = raw_data["ichsu"]
    out["divorce_alimony_prev"] = raw_data["ispou"]
    out["riester_prev"] = raw_data["irie1"]
    out["riester_widow_prev"] = raw_data["irie2"]

    return out


def pgen(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pgen dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = int_categorical_to_int(raw_data["cid"])
    out["soep_hh_id"] = raw_data["hid"].astype(apply_lowest_int_dtype(raw_data["hid"]))
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["nationality_first"] = raw_data["pgnation"]
    out["status_refugee"] = raw_data["pgstatus_refu"]
    out["marital_status"] = raw_data["pgfamstd"]
    out["curr_earnings_m"] = raw_data["pglabgro"]
    out["net_wage_m"] = raw_data["pglabnet"]
    out["occupation_status"] = raw_data["pgstib"]
    out["employment_status"] = raw_data["pgemplst"]
    out["laborf_status"] = raw_data["pglfs"]
    out["dauer_im_betrieb"] = raw_data["pgerwzeit"]
    out["weekly_working_hours_actual"] = raw_data["pgtatzeit"]
    out["weekly_working_hours_contract"] = raw_data["pgvebzeit"]
    out["public_service"] = raw_data["pgoeffd"]
    out["size_company_raw"] = raw_data["pgbetr"]
    out["size_company"] = raw_data["pgallbet"]
    out["pgen_grund_beschäftigungsende"] = raw_data["pgjobend"]
    out["exp_full_time"] = raw_data["pgexpft"]
    out["exp_part_time"] = raw_data["pgexppt"]
    out["exp_unempl"] = raw_data["pgexpue"]
    out["education_isced_alt"] = raw_data["pgisced97"]
    out["education_isced"] = raw_data["pgisced11"]
    out["education_casmin"] = raw_data["pgcasmin"]
    out["month_interview"] = raw_data["pgmonth"]

    return out


def pkal(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pkal dataset."""
    out = pd.DataFrame()
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["soep_hh_id"] = raw_data["hid"].astype(apply_lowest_int_dtype(raw_data["hid"]))
    out["soep_initial_hh_id"] = int_categorical_to_int(raw_data["cid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["full_empl_v1_prev_1"] = raw_data["kal1a001_v1"]
    out["full_empl_v2_prev_1"] = raw_data["kal1a001_v2"]
    out["full_empl_v1_prev_2"] = raw_data["kal1a002_v1"]
    out["full_empl_v2_prev_2"] = raw_data["kal1a002_v2"]
    out["full_empl_v1_prev_3"] = raw_data["kal1a003_v1"]
    out["full_empl_v2_prev_3"] = raw_data["kal1a003_v2"]
    out["full_empl_v1_prev_4"] = raw_data["kal1a004_v1"]
    out["full_empl_v2_prev_4"] = raw_data["kal1a004_v2"]
    out["full_empl_v1_prev_5"] = raw_data["kal1a005_v1"]
    out["full_empl_v2_prev_5"] = raw_data["kal1a005_v2"]
    out["full_empl_v1_prev_6"] = raw_data["kal1a006_v1"]
    out["full_empl_v2_prev_6"] = raw_data["kal1a006_v2"]
    out["full_empl_v1_prev_7"] = raw_data["kal1a007_v1"]
    out["full_empl_v2_prev_7"] = raw_data["kal1a007_v2"]
    out["full_empl_v1_prev_8"] = raw_data["kal1a008_v1"]
    out["full_empl_v2_prev_8"] = raw_data["kal1a008_v2"]
    out["full_empl_v1_prev_9"] = raw_data["kal1a009_v1"]
    out["full_empl_v2_prev_9"] = raw_data["kal1a009_v2"]
    out["full_empl_v1_prev_10"] = raw_data["kal1a010_v1"]
    out["full_empl_v2_prev_10"] = raw_data["kal1a010_v2"]
    out["full_empl_v1_prev_11"] = raw_data["kal1a011_v1"]
    out["full_empl_v2_prev_11"] = raw_data["kal1a011_v2"]
    out["full_empl_v1_prev_12"] = raw_data["kal1a012_v1"]
    out["full_empl_v2_prev_12"] = raw_data["kal1a012_v2"]
    out["half_empl_prev_1"] = raw_data["kal1b001"]
    out["half_empl_prev_2"] = raw_data["kal1b002"]
    out["half_empl_prev_3"] = raw_data["kal1b003"]
    out["half_empl_prev_4"] = raw_data["kal1b004"]
    out["half_empl_prev_5"] = raw_data["kal1b005"]
    out["half_empl_prev_6"] = raw_data["kal1b006"]
    out["half_empl_prev_7"] = raw_data["kal1b007"]
    out["half_empl_prev_8"] = raw_data["kal1b008"]
    out["half_empl_prev_9"] = raw_data["kal1b009"]
    out["half_empl_prev_10"] = raw_data["kal1b010"]
    out["half_empl_prev_11"] = raw_data["kal1b011"]
    out["half_empl_prev_12"] = raw_data["kal1b012"]
    out["unempl_months_prev"] = raw_data["kal1d02"]
    out["rente_monate_prev"] = raw_data["kal1e02"]
    out["mini_job_prev_1"] = raw_data["kal1n001"]
    out["mini_job_prev_2"] = raw_data["kal1n002"]
    out["mini_job_prev_3"] = raw_data["kal1n003"]
    out["mini_job_prev_4"] = raw_data["kal1n004"]
    out["mini_job_prev_5"] = raw_data["kal1n005"]
    out["mini_job_prev_6"] = raw_data["kal1n006"]
    out["mini_job_prev_7"] = raw_data["kal1n007"]
    out["mini_job_prev_8"] = raw_data["kal1n009"]
    out["mini_job_prev_9"] = raw_data["kal1n008"]
    out["mini_job_prev_10"] = raw_data["kal1n010"]
    out["mini_job_prev_11"] = raw_data["kal1n011"]
    out["mini_job_prev_12"] = raw_data["kal1n012"]
    out["m_alg_prev"] = raw_data["kal2f02"]
    out["mschaftsgeld_bezogen_prev"] = raw_data["kal2j01"]
    out["mschaftsgeld_monate_prev"] = raw_data["kal2j02"]

    return out


def pl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pl dataset."""
    out = pd.DataFrame()
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["soep_hh_id"] = raw_data["hid"].astype(apply_lowest_int_dtype(raw_data["hid"]))
    out["lfd_pnr"] = raw_data["pnr"]
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["altersteilzeit_02_14"] = raw_data["plb0103"]
    out["altersteilzeit_2001"] = raw_data["plb0179"]
    out["dauer_letzte_stelle_j"] = raw_data["plb0301"]
    out["dauer_letzte_stelle_m"] = raw_data["plb0302"]
    out["letzte_stelle_betriebsstilll"] = raw_data["plb0304_v11"]
    out["letzte_stelle_grund_1999"] = raw_data["plb0304_v13"]
    out["letzte_stelle_grund"] = raw_data["plb0304_v14"]
    out["actv_work_search"] = raw_data["plb0424_v2"]
    out["altersteilzeit_art"] = raw_data["plb0460"]
    out["wage_employee_m_prev"] = raw_data["plb0471_h"]
    out["mschaftsgeld_prev"] = raw_data["plc0126_v1"]
    out["arbeitslosengeld_empf"] = raw_data["plc0130_v1"]
    out["arbeitslosengeld_m3_m5_empf"] = raw_data["plc0130_v2"]
    out["mschaftsgeld_vorm_pl"] = raw_data["plc0152_v1"]
    out["mschaftsgeld_brutto_m"] = raw_data["plc0153_h"]
    out["mschaftsgeld_betrag_pl_prev"] = raw_data["plc0155_h"]
    out["child_alimony_before_2016"] = raw_data["plc0178"]
    out["in_priv_rente_eingezahlt"] = raw_data["plc0437"]
    out["in_priv_rente_eingezahlt_monate"] = raw_data["plc0438"]
    out["prv_rente_beitr_2013_m"] = raw_data["plc0439_v1"]
    out["prv_rente_beitr_2018_m"] = raw_data["plc0439_v2"]
    out["med_pl_schw_treppen"] = raw_data["ple0004"]
    out["med_pl_schw_taten"] = raw_data["ple0005"]
    out["med_pl_groesse"] = raw_data["ple0006"]
    out["med_pl_gewicht"] = raw_data["ple0007"]
    out["med_pl_subj_status"] = raw_data["ple0008"]
    out["med_pl_schlaf"] = raw_data["ple0011"]
    out["med_pl_diabetes"] = raw_data["ple0012"]
    out["med_pl_asthma"] = raw_data["ple0013"]
    out["med_pl_herzkr"] = raw_data["ple0014"]
    out["med_pl_krebs"] = raw_data["ple0015"]
    out["med_pl_schlaganf"] = raw_data["ple0016"]
    out["med_pl_migraene"] = raw_data["ple0017"]
    out["med_pl_bluthdrck"] = raw_data["ple0018"]
    out["med_pl_depressiv"] = raw_data["ple0019"]
    out["med_pl_demenz"] = raw_data["ple0020"]
    out["med_pl_gelenk"] = raw_data["ple0021"]
    out["med_pl_ruecken"] = raw_data["ple0022"]
    out["med_pl_sonst"] = raw_data["ple0023"]
    out["disability_degree"] = raw_data["ple0041"]
    out["med_pl_raucher"] = raw_data["ple0081_h"]
    out["art_kv"] = raw_data["ple0097"]
    out["politics_left_right"] = raw_data["plh0004"]
    out["interest_politics"] = raw_data["plh0007"]
    out["party_affiliation_any"] = raw_data["plh0011_h"]
    out["party_affiliation"] = raw_data["plh0012_h"]
    out["party_affiliation_intensity"] = raw_data["plh0013_h"]
    out["importance_career"] = raw_data["plh0107"]
    out["importance_children"] = raw_data["plh0110"]
    out["lebenszufriedenheit"] = raw_data["plh0182"]
    out["general_trust"] = raw_data["plh0192"]
    out["confession"] = raw_data["plh0258_h"]
    out["confession_any"] = raw_data["plh0258_v9"]
    out["norm_child_suffers_under_6"] = raw_data["plh0298_v1"]
    out["norm_child_suffers_under_6_2018"] = raw_data["plh0298_v2"]
    out["norm_marry_when_together"] = raw_data["plh0300_v1"]
    out["norm_marry_when_together_2018"] = raw_data["plh0300_v2"]
    out["norm_women_family_priority"] = raw_data["plh0301"]
    out["norm_child_suffers_under_3"] = raw_data["plh0302_v1"]
    out["norm_child_suffers_under_3_2018"] = raw_data["plh0302_v2"]
    out["norm_men_chores"] = raw_data["plh0303"]
    out["norm_ch_suffers_father_career"] = raw_data["plh0304"]
    out["norm_genders_similar"] = raw_data["plh0308_v1"]
    out["norm_genders_similar_2018"] = raw_data["plh0308_v2"]
    out["norm_career_mothers_same_warmth"] = raw_data["plh0309"]
    out["importance_faith"] = raw_data["plh0343_v1"]
    out["importance_faith_v2"] = raw_data["plh0343_v2"]
    out["h_wage_pl"] = raw_data["plh0354_h"]
    out["hours_work_sat"] = raw_data["pli0003_h"]
    out["hours_work_sun"] = raw_data["pli0007_h"]
    out["hours_hobbies_sun"] = raw_data["pli0010"]
    out["hours_errands_sun"] = raw_data["pli0011"]
    out["hours_housework_sat"] = raw_data["pli0012_h"]
    out["hours_housework_sun"] = raw_data["pli0016_h"]
    out["hours_childcare_sat"] = raw_data["pli0019_h"]
    out["hours_childcare_sun"] = raw_data["pli0022_h"]
    out["hours_repairs_sat"] = raw_data["pli0031_h"]
    out["hours_repairs_sun"] = raw_data["pli0034_v4"]
    out["hours_hobbies_sat"] = raw_data["pli0036"]
    out["hours_work_workday"] = raw_data["pli0038_h"]
    out["hours_errands_workday"] = raw_data["pli0040"]
    out["hours_housework_workday"] = raw_data["pli0043_h"]
    out["hours_childcare_workday"] = raw_data["pli0044_h"]
    out["hours_care_workday"] = raw_data["pli0046"]
    out["hours_repairs_workday"] = raw_data["pli0049_h"]
    out["hours_hobbies_workday"] = raw_data["pli0051"]
    out["hours_errands_sat"] = raw_data["pli0054"]
    out["hours_care_sat"] = raw_data["pli0055"]
    out["hours_care_sun"] = raw_data["pli0057"]
    out["hours_sleep_workday"] = raw_data["pli0059"]
    out["hours_sleep_weekend"] = raw_data["pli0060"]
    out["motor_disability"] = raw_data["plj0582"]
    out["trust_public_admin"] = raw_data["plm0672"]
    out["trust_government"] = raw_data["plm0673"]

    return out


def ppathl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the ppathl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = raw_data["hid"].astype(apply_lowest_int_dtype(raw_data["hid"]))
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["current_east_west"] = raw_data["sampreg"]
    out["befragungsstatus"] = raw_data["netto"]
    out["year_immigration"] = raw_data["immiyear"]
    out["born_in_germany"] = raw_data["germborn"]
    out["country_of_birth"] = raw_data["corigin"]
    out["birth_month_ppathl"] = raw_data["gebmonat"]
    out["east_west_1989"] = raw_data["loc1989"]
    out["migrationshintergrund"] = raw_data["migback"]
    out["sexual_orientation"] = raw_data["sexor"]
    out["birth_bundesland"] = raw_data["birthregion"]
    out["p_bleibe_wkeit"] = raw_data["pbleib"]
    out["p_gewicht"] = raw_data["phrf"]
    out["p_gewicht_nur_neue"] = raw_data["phrf0"]
    out["p_gewicht_ohne_neue"] = raw_data["phrf1"]
    out["pointer_partner"] = raw_data["parid"]
    out["has_partner"] = raw_data["partner"]

    return out
