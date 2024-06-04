import pandas as pd

from soep_cleaning.initial_preprocessing.helper import (
    agreement_int_categorical,
    bool_categorical,
    float_categorical_to_float,
    int_categorical_to_int,
    str_categorical,
)
from soep_cleaning.utilities import apply_lowest_float_dtype, apply_lowest_int_dtype


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
    ]  # TODO: categories [29] and [39] have identical labels, should be cleaned using str_categorical
    out["bearbeitungserg_ausf"] = raw_data[
        "pergz"
    ]  # TODO: categories [29] and [39] have identical labels, should be cleaned using str_categorical
    out["hh_position_raw_last_year"] = raw_data[
        "pzugv"
    ]  # TODO: categories [29] and [39] have identical labels, should be cleaned using str_categorical
    out["teilnahmebereitchaft"] = str_categorical(raw_data["ber"])
    out["bearbeitungserg_alt"] = str_categorical(raw_data["hergs"])

    return out


def pequiv(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pequiv dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["gender"] = str_categorical(
        raw_data["d11102ll"],
        renaming={"[1] Male": "male", "[2] Female": "female"},
    )
    out["age"] = int_categorical_to_int(raw_data["d11101"])
    out["hh_size"] = apply_lowest_int_dtype(raw_data["d11106"])
    out["anz_minderj_hh1"] = apply_lowest_int_dtype(raw_data["d11107"])
    out["annual_work_h_prev"] = int_categorical_to_int(raw_data["e11101"])
    out["employment_status_pequiv"] = str_categorical(raw_data["e11102"], ordered=False)
    out["employment_level_pequiv"] = str_categorical(raw_data["e11103"], ordered=False)
    out["einkommen_vor_steuer_hh_prev"] = int_categorical_to_int(raw_data["i11101"])
    out["einkommen_nach_steuer_hh_prev"] = int_categorical_to_int(raw_data["i11102"])
    out["labor_earnings_prev"] = int_categorical_to_int(raw_data["i11110"])
    out["bundesland"] = str_categorical(raw_data["l11101"], ordered=False)
    out["earnings_job_1_prev"] = int_categorical_to_int(raw_data["ijob1"])
    out["earnings_job_2_prev"] = int_categorical_to_int(raw_data["ijob2"])
    out["self_empl_earnings_prev"] = int_categorical_to_int(raw_data["iself"])
    out["arbeitsl_geld_soep_prev"] = int_categorical_to_int(raw_data["iunby"])
    out["arbeitsl_hilfe_soep_prev"] = int_categorical_to_int(raw_data["iunay"])
    out["unterhaltsgeld_soep_prev"] = int_categorical_to_int(raw_data["isuby"])
    out["übergangsgeld_soep_prev"] = int_categorical_to_int(raw_data["ieret"])
    out["mschaftsgeld_soep_prev"] = int_categorical_to_int(raw_data["imaty"])
    out["student_grants_prev"] = int_categorical_to_int(raw_data["istuy"])
    out["alimony_prev"] = int_categorical_to_int(raw_data["ialim"])
    out["other_transfers_prev"] = int_categorical_to_int(raw_data["ielse"])
    out["christmas_bonus_prev"] = int_categorical_to_int(raw_data["ixmas"])
    out["vacation_bonus_prev"] = int_categorical_to_int(raw_data["iholy"])
    out["profit_share_prev"] = int_categorical_to_int(raw_data["igray"])
    out["other_bonuses_prev"] = int_categorical_to_int(raw_data["iothy"])
    out["gov_ret_pay_prev"] = int_categorical_to_int(raw_data["igrv1"])
    out["gov_ret_pay_widow_prev"] = int_categorical_to_int(raw_data["igrv2"])
    out["rental_income_y_hh_prev"] = int_categorical_to_int(raw_data["renty"])
    out["maintenance_c_hh_prev"] = int_categorical_to_int(raw_data["opery"])
    out["capital_income_hh_prev"] = int_categorical_to_int(raw_data["divdy"])
    out["kindergeld_pequiv_hh_prev"] = int_categorical_to_int(raw_data["chspt"])
    out["wohngeld_soep_hh_prev"] = int_categorical_to_int(raw_data["house"])
    out["pflegegeld_hh_prev"] = int_categorical_to_int(raw_data["nursh"])
    out["soc_assist_hh_prev"] = int_categorical_to_int(raw_data["subst"])
    out["soc_assist_spec_hh_prev"] = int_categorical_to_int(raw_data["sphlp"])
    out["housing_sup_hh_prev"] = int_categorical_to_int(raw_data["hsup"])
    out["soc_miners_pension_prev"] = int_categorical_to_int(raw_data["ismp1"])
    out["civ_serv_pension_prev"] = int_categorical_to_int(raw_data["iciv1"])
    out["warvictim_pension_prev"] = int_categorical_to_int(raw_data["iwar1"])
    out["farmer_pension_prev"] = int_categorical_to_int(raw_data["iagr1"])
    out["stat_accident_insurance_prev"] = int_categorical_to_int(raw_data["iguv1"])
    out["civ_supp_benefits_prev"] = int_categorical_to_int(raw_data["ivbl1"])
    out["company_pen_prev"] = int_categorical_to_int(raw_data["icom1"])
    out["private_pen_prev"] = int_categorical_to_int(raw_data["iprv1"])
    out["other_pen_prev"] = int_categorical_to_int(raw_data["ison1"])
    out["soc_miners_pension_widow_prev"] = int_categorical_to_int(raw_data["ismp2"])
    out["civ_serv_pension_widow_prev"] = int_categorical_to_int(raw_data["iciv2"])
    out["warvictim_pension_widow_prev"] = int_categorical_to_int(raw_data["iwar2"])
    out["farmer_pension_widow_prev"] = int_categorical_to_int(raw_data["iagr2"])
    out["stat_accident_insur_widow_prev"] = int_categorical_to_int(raw_data["iguv2"])
    out["civ_supp_benefits_widow_prev"] = int_categorical_to_int(raw_data["ivbl2"])
    out["company_pen_widow_prev"] = int_categorical_to_int(raw_data["icom2"])
    out["other_pen_widow_prev"] = int_categorical_to_int(raw_data["ison2"])
    out["private_pen_widow_prev"] = int_categorical_to_int(raw_data["iprv2"])

    out["med_pe_krnkhaus"] = bool_categorical(
        raw_data["m11101"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schlaganf"] = bool_categorical(
        raw_data["m11105"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_bluthdrck"] = bool_categorical(
        raw_data["m11106"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_diabetes"] = bool_categorical(
        raw_data["m11107"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_krebs"] = bool_categorical(
        raw_data["m11108"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_psych"] = bool_categorical(
        raw_data["m11109"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_gelenk"] = bool_categorical(
        raw_data["m11110"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_herzkr"] = bool_categorical(
        raw_data["m11111"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_schw_treppen"] = bool_categorical(
        raw_data["m11113"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_anziehen"] = bool_categorical(
        raw_data["m11115"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_bett"] = bool_categorical(
        raw_data["m11116"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_einkauf"] = bool_categorical(
        raw_data["m11117"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_hausarb"] = bool_categorical(
        raw_data["m11119"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_groesse"] = int_categorical_to_int(raw_data["m11122"])
    out["med_pe_gewicht"] = int_categorical_to_int(raw_data["m11123"])
    out["med_pe_zufrieden"] = str_categorical(
        raw_data["m11125"],
        renaming={
            "[0] Completely dissatisfied": 0,
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
            "5": 5,
            "6": 6,
            "7": 7,
            "8": 8,
            "9": 9,
            "[10] Completely satisfied": 10,
        },
    )
    # TODO: renaming to be deleted here
    out["med_pe_subj_status"] = str_categorical(
        raw_data["m11126"],
        renaming={
            "[1] Very good": 1,
            "[2] Good": 2,
            "[3] Satisfactory": 3,
            "[4] Poor": 4,
            "[5] Bad": 5,
        },
    )
    out["soc_assist_eld_hh_prev"] = int_categorical_to_int(raw_data["ssold"])
    out["arbeitsl_geld_2_soep_hh_prev"] = int_categorical_to_int(raw_data["alg2"])
    out["kinderzuschlag_pequiv_hh_prev"] = int_categorical_to_int(raw_data["adchb"])
    out["adv_child_maint_payment_prev"] = int_categorical_to_int(raw_data["iachm"])
    out["childcare_subsidy_hh_prev"] = int_categorical_to_int(raw_data["chsub"])
    out["caregiver_alimony_prev"] = int_categorical_to_int(raw_data["ichsu"])
    out["divorce_alimony_prev"] = int_categorical_to_int(raw_data["ispou"])
    out["riester_prev"] = int_categorical_to_int(raw_data["irie1"])
    out["riester_widow_prev"] = int_categorical_to_int(raw_data["irie2"])

    return out


def pgen(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pgen dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["nationality_first"] = str_categorical(raw_data["pgnation"], ordered=False)
    out["status_refugee"] = str_categorical(raw_data["pgstatus_refu"], ordered=False)
    out["marital_status"] = str_categorical(raw_data["pgfamstd"], ordered=False)
    out["curr_earnings_m"] = int_categorical_to_int(raw_data["pglabgro"])
    out["net_wage_m"] = int_categorical_to_int(raw_data["pglabnet"])
    out["occupation_status"] = str_categorical(raw_data["pgstib"], ordered=False)
    out["employment_status"] = str_categorical(
        raw_data["pgemplst"],
        ordered=False,
        renaming={
            "[1] Voll erwerbstätig": "[1] Voll erwerbstätig",
            "[2] Teilzeitbeschäftigung": "[2] Teilzeitbeschäftigung",
            "[3] Ausbildung, Lehre": "[3] Ausbildung, Lehre",
            "[4] Unregelmässig, geringfügig erwerbstät.": "[4] Unregelmässig, geringfügig erwerbstät.",
            "[5] Nicht erwerbstaetig": "[5] Nicht erwerbstätig",
            "[6] Werkstatt für behinderte Menschen": "[6] Werkstatt für behinderte Menschen",
        },
    )
    out["laborf_status"] = str_categorical(raw_data["pglfs"], ordered=False)
    out["dauer_im_betrieb"] = int_categorical_to_int(raw_data["pgerwzeit"])
    out["weekly_working_hours_actual"] = int_categorical_to_int(raw_data["pgtatzeit"])
    out["weekly_working_hours_contract"] = int_categorical_to_int(raw_data["pgvebzeit"])
    out["public_service"] = bool_categorical(
        raw_data["pgoeffd"],
        renaming={"[1] ja": True, "[2] nein": False},
    )
    out["size_company_raw"] = str_categorical(raw_data["pgbetr"])
    out["size_company"] = str_categorical(raw_data["pgallbet"])
    out["pgen_grund_beschäftigungsende"] = str_categorical(
        raw_data["pgjobend"],
        ordered=False,
    )
    out["exp_full_time"] = float_categorical_to_float(raw_data["pgexpft"])
    out["exp_part_time"] = float_categorical_to_float(raw_data["pgexppt"])
    out["exp_unempl"] = float_categorical_to_float(raw_data["pgexpue"])
    out["education_isced_alt"] = str_categorical(raw_data["pgisced97"])
    out["education_isced"] = str_categorical(
        raw_data["pgisced11"],
        renaming={
            "[0] in school": "primary_and_lower_secondary",
            "[1] Primary education": "primary_and_lower_secondary",
            "[2] Lower secondary education": "primary_and_lower_secondary",
            "[3] Upper secondary education": "upper_secondary",
            "[4] Post-secondary non-tertiary education": "upper_secondary",
            "[5] Short-cycle tertiary education": "upper_secondary",
            "[6] Bachelor s or equivalent level": "tertiary",
            "[7] Master s or equivalent level": "tertiary",
            "[8] Doctoral or equivalent level": "tertiary",
        },
    )
    out["education_casmin"] = str_categorical(
        raw_data["pgcasmin"],
        renaming={
            "[0] (0) in school": "primary_and_lower_secondary",
            "[1] (1a) inadequately completed": "primary_and_lower_secondary",
            "[2] (1b) general elementary school": "primary_and_lower_secondary",
            "[3] (1c) basic vocational qualification": "primary_and_lower_secondary",
            "[4] (2b) intermediate general qualification": "upper_secondary",
            "[5] (2a) intermediate vocational": "upper_secondary",
            "[6] (2c_gen) general maturity certificate": "upper_secondary",
            "[7] (2c_voc) vocational maturity certificate": "upper_secondary",
            "[8] (3a) lower tertiary education": "tertiary",
            "[9] (3b) higher tertiary education": "tertiary",
        },
    )
    out["month_interview"] = str_categorical(
        raw_data["pgmonth"],
        renaming={
            "[1] Januar": 1,
            "[2] Februar": 2,
            "[3] Maerz": 3,
            "[4] April": 4,
            "[5] Mai": 5,
            "[6] Juni": 6,
            "[7] Juli": 7,
            "[8] August": 8,
            "[9] September": 9,
            "[10] Oktober": 10,
            "[11] November": 11,
            "[12] Dezember": 12,
        },
    )

    return out


def pkal(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pkal dataset."""
    out = pd.DataFrame()
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["full_empl_v1_prev_1"] = bool_categorical(
        raw_data["kal1a001_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_1"] = str_categorical(raw_data["kal1a001_v2"])
    out["full_empl_v1_prev_2"] = bool_categorical(
        raw_data["kal1a002_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_2"] = str_categorical(raw_data["kal1a002_v2"])
    out["full_empl_v1_prev_3"] = bool_categorical(
        raw_data["kal1a003_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_3"] = str_categorical(raw_data["kal1a003_v2"])
    out["full_empl_v1_prev_4"] = bool_categorical(
        raw_data["kal1a004_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_4"] = str_categorical(raw_data["kal1a004_v2"])
    out["full_empl_v1_prev_5"] = bool_categorical(
        raw_data["kal1a005_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_5"] = str_categorical(raw_data["kal1a005_v2"])
    out["full_empl_v1_prev_6"] = bool_categorical(
        raw_data["kal1a006_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_6"] = str_categorical(raw_data["kal1a006_v2"])
    out["full_empl_v1_prev_7"] = bool_categorical(
        raw_data["kal1a007_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_7"] = str_categorical(raw_data["kal1a007_v2"])
    out["full_empl_v1_prev_8"] = bool_categorical(
        raw_data["kal1a008_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_8"] = str_categorical(raw_data["kal1a008_v2"])
    out["full_empl_v1_prev_9"] = bool_categorical(
        raw_data["kal1a009_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_9"] = str_categorical(raw_data["kal1a009_v2"])
    out["full_empl_v1_prev_10"] = bool_categorical(
        raw_data["kal1a010_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_10"] = str_categorical(raw_data["kal1a010_v2"])
    out["full_empl_v1_prev_11"] = bool_categorical(
        raw_data["kal1a011_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_11"] = str_categorical(raw_data["kal1a011_v2"])
    out["full_empl_v1_prev_12"] = bool_categorical(
        raw_data["kal1a012_v1"],
        renaming={"[1] Ja": True},
    )
    out["full_empl_v2_prev_12"] = str_categorical(raw_data["kal1a012_v2"])
    out["half_empl_prev_1"] = str_categorical(raw_data["kal1b001"])
    out["half_empl_prev_2"] = str_categorical(raw_data["kal1b002"])
    out["half_empl_prev_3"] = str_categorical(raw_data["kal1b003"])
    out["half_empl_prev_4"] = str_categorical(raw_data["kal1b004"])
    out["half_empl_prev_5"] = str_categorical(raw_data["kal1b005"])
    out["half_empl_prev_6"] = str_categorical(raw_data["kal1b006"])
    out["half_empl_prev_7"] = str_categorical(raw_data["kal1b007"])
    out["half_empl_prev_8"] = str_categorical(raw_data["kal1b008"])
    out["half_empl_prev_9"] = str_categorical(raw_data["kal1b009"])
    out["half_empl_prev_10"] = str_categorical(raw_data["kal1b010"])
    out["half_empl_prev_11"] = str_categorical(raw_data["kal1b011"])
    out["half_empl_prev_12"] = str_categorical(raw_data["kal1b012"])
    out["unempl_months_prev"] = int_categorical_to_int(raw_data["kal1d02"])
    out["rente_monate_prev"] = int_categorical_to_int(raw_data["kal1e02"])
    out["mini_job_prev_1"] = bool_categorical(raw_data["kal1n001"])
    out["mini_job_prev_3"] = bool_categorical(raw_data["kal1n003"])
    out["mini_job_prev_4"] = bool_categorical(raw_data["kal1n004"])
    out["mini_job_prev_5"] = bool_categorical(raw_data["kal1n005"])
    out["mini_job_prev_6"] = bool_categorical(raw_data["kal1n006"])
    out["mini_job_prev_7"] = bool_categorical(raw_data["kal1n007"])
    out["mini_job_prev_8"] = bool_categorical(raw_data["kal1n009"])
    out["mini_job_prev_9"] = bool_categorical(raw_data["kal1n008"])
    out["mini_job_prev_2"] = bool_categorical(raw_data["kal1n002"])
    out["mini_job_prev_10"] = bool_categorical(raw_data["kal1n010"])
    out["mini_job_prev_11"] = bool_categorical(raw_data["kal1n011"])
    out["mini_job_prev_12"] = bool_categorical(raw_data["kal1n012"])
    out["m_alg_prev"] = int_categorical_to_int(raw_data["kal2f02"])
    out["mschaftsgeld_bezogen_prev"] = bool_categorical(
        raw_data["kal2j01"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["mschaftsgeld_monate_prev"] = int_categorical_to_int(raw_data["kal2j02"])
    # TODO: transform full_empl, half_empl_prev mini_job_prev into long format?
    return out


def pl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pl dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["lfd_pnr"] = int_categorical_to_int(raw_data["pnr"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["altersteilzeit_02_14"] = bool_categorical(
        raw_data["plb0103"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_2001"] = bool_categorical(
        raw_data["plb0179"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["dauer_letzte_stelle_j"] = int_categorical_to_int(
        raw_data["plb0301"],
    )  # TODO: Check on full dataset
    out["dauer_letzte_stelle_m"] = bool_categorical(
        raw_data["plb0302"],
    )  # TODO: Check on full dataset
    out["letzte_stelle_betriebsstilll"] = bool_categorical(
        raw_data["plb0304_v11"],
    )  # TODO: Check on full dataset
    out["letzte_stelle_grund_1999"] = bool_categorical(
        raw_data["plb0304_v13"],
    )  # TODO: Check on full dataset
    out["letzte_stelle_grund"] = bool_categorical(
        raw_data["plb0304_v14"],
    )  # TODO: Check on full dataset
    out["actv_work_search"] = bool_categorical(
        raw_data["plb0424_v2"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_art"] = bool_categorical(
        raw_data["plb0460"],
    )  # TODO: Check on full dataset
    out["wage_employee_m_prev"] = bool_categorical(
        raw_data["plb0471_h"],
    )  # TODO: Check on full dataset
    out["mschaftsgeld_prev"] = bool_categorical(
        raw_data["plc0126_v1"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_empf"] = bool_categorical(
        raw_data["plc0130_v1"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_m3_m5_empf"] = bool_categorical(
        raw_data["plc0130_v2"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )  # TODO: Fix ordering and category "[3] Weiss nicht"
    out["mschaftsgeld_vorm_pl"] = bool_categorical(
        raw_data["plc0152_v1"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["mschaftsgeld_brutto_m"] = bool_categorical(
        raw_data["plc0153_h"],
    )  # TODO: Check on full dataset
    out["mschaftsgeld_betrag_pl_prev"] = bool_categorical(
        raw_data["plc0155_h"],
    )  # TODO: Check on full dataset
    out["child_alimony_before_2016"] = bool_categorical(
        raw_data["plc0178"],
    )  # TODO: Check on full dataset
    out["in_priv_rente_eingezahlt"] = bool_categorical(
        raw_data["plc0437"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["in_priv_rente_eingezahlt_monate"] = bool_categorical(
        raw_data["plc0438"],
    )  # TODO: Check on full dataset
    out["prv_rente_beitr_2013_m"] = bool_categorical(
        raw_data["plc0439_v1"],
    )  # TODO: Check on full dataset
    out["prv_rente_beitr_2018_m"] = bool_categorical(
        raw_data["plc0439_v2"],
    )  # TODO: Check on full dataset
    out["med_pl_schw_treppen"] = raw_data[
        "ple0004"
    ]  # TODO: check with pl_replacing.yaml
    out["med_pl_schw_taten"] = raw_data["ple0005"]  # TODO: check with pl_replacing.yaml
    out["med_pl_groesse"] = bool_categorical(
        raw_data["ple0006"],
    )  # TODO: Check on full dataset
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
    out["politics_left_right"] = agreement_int_categorical(
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
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"], remove_negatives=True)
    out["p_id"] = int_categorical_to_int(raw_data["pid"])
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["current_east_west"] = str_categorical(raw_data["sampreg"], ordered=False)
    out["befragungsstatus"] = str_categorical(raw_data["netto"], ordered=False)
    out["year_immigration"] = apply_lowest_int_dtype(raw_data["immiyear"])
    out["born_in_germany"] = str_categorical(raw_data["germborn"], ordered=False)
    out["country_of_birth"] = str_categorical(raw_data["corigin"], ordered=False)
    out["birth_month_ppathl"] = str_categorical(
        raw_data["gebmonat"],
        ordered=False,
        renaming={
            "[1] Januar": 1,
            "[2] Februar": 2,
            "[3] Maerz": 3,
            "[4] April": 4,
            "[5] Mai": 5,
            "[6] Juni": 6,
            "[7] Juli": 7,
            "[8] August": 8,
            "[9] September": 9,
            "[10] Oktober": 10,
            "[11] November": 11,
            "[12] Dezember": 12,
        },
    )
    out["east_west_1989"] = str_categorical(raw_data["loc1989"], ordered=False)
    out["migrationshintergrund"] = str_categorical(raw_data["migback"], ordered=False)
    out["sexual_orientation"] = str_categorical(raw_data["sexor"], ordered=False)
    out["birth_bundesland"] = str_categorical(raw_data["birthregion"], ordered=False)
    out["p_bleibe_wkeit"] = apply_lowest_float_dtype(raw_data["pbleib"])
    out["p_gewicht"] = apply_lowest_float_dtype(raw_data["phrf"])
    out["p_gewicht_nur_neue"] = apply_lowest_float_dtype(raw_data["phrf0"])
    out["p_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw_data["phrf1"])
    out["pointer_partner"] = apply_lowest_int_dtype(
        raw_data["parid"],
        remove_negatives=True,
    )
    out["has_partner"] = str_categorical(raw_data["partner"], ordered=False)

    return out
