from soep_cleaning.config import pd
from soep_cleaning.initial_preprocessing import month_mapping
from soep_cleaning.initial_preprocessing.helper import (
    bool_categorical,
    categorical_to_int_categorical,
    float_categorical_to_float,
    float_categorical_to_int,
    int_categorical_to_int,
    int_to_int_categorical,
    replace_conditionally,
    str_categorical,
    str_categorical_to_int_categorical,
)
from soep_cleaning.utilities import apply_lowest_float_dtype, apply_lowest_int_dtype


def pbrutto(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pbrutto dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["soep_initial_hh_id"] = int_categorical_to_int(raw_data["cid"])
    out["soep_hh_id"] = int_categorical_to_int(raw_data["hid"])
    out["year"] = int_categorical_to_int(raw_data["syear"])
    out["birth_year"] = int_categorical_to_int(raw_data["geburt_v2"])
    out["befragungs_status"] = str_categorical(raw_data["befstat_h"])
    out["hh_position"] = str_categorical(
        raw_data["stell_h"],
        ordered=False,
        renaming={
            "[0] Haushaltsvorstand,Bezugsperson": "Household head",
            "[11] Ehegatte/in": "Spouse",
            "[12] gleichgeschl.Partner/in": "Spouse",
            "[13] Lebenspartner/in": "Spouse",
            "[20] Kind/Adoptivkind [bis 2011]": "Child",
            "[21] Leibliches Kind": "Child",
            "[22] Stiefkind(Kind d.Ehe-/LPartners)": "Child",
            "[23] Adoptivkind": "Child",
            "[24] Pflegekind": "Other",
            "[25] Enkelkind": "Other relative",
            "[26] Urenkelkind": "Other",
            "[27] Schwsohn,-tocher (Ehe-/LPartner v.Kind)": "Other relative",
            "[30] Eltern [bis 2011]": "Parent",
            "[31] Leibliche/r Vater,Mutter": "Parent",
            "[32] Stiefvater,-mutter/Partner v.Vater,Mutter": "Parent",
            "[33] Adoptivvater,-mutter": "Parent",
            "[35] Schwiegervater,-mutter,(Ehe-/LPartner v.Eltern)": "Parent",
            "[36] Grossvater,-mutter": "Other relative",
            "[40] Geschwister, Schwager/Schwaegerin [bis 2011]": "Other relative",
            "[41] Leibliche/r Bruder,Schwester": "Other relative",
            "[42] Halbbruder,-schwester": "Other relative",
            "[43] Stiefschwester,-bruder(von Elternteilen.verh/lpar)": "Other relative",
            "[45] Pflegebruder,-schwester": "Other relative",
            "[51] Schwa(e)ger/in (Ehe-/LPartner v.Geschwistern)": "Other relative",
            "[52] Schwa(e)ger/in (Geschwister v.Ehe-/LPartner)": "Other relative",
            "[60] Tante/Onkel, Nichte/Neffe [bis 2011]": "Other relative",
            "[61] Tante,Onkel": "Other relative",
            "[62] Nichte/Neffe": "Other relative",
            "[63] Cousin/Cousine": "Other relative",
            "[64] Andere Verwandte": "Other relative",
            "[70] Nicht verwandte/verschwaegerte Person [bis 2011]": "Other",
            "[71] Sonstige": "Other",
            "[71] keine Angabe": "Other",
            "[99] Stellung zu HV unbekannt": "Other",
        },
        reduce=True,
    )
    out["bearbeitungserg"] = str_categorical(raw_data["perg"], ordered=False)
    out["bearbeitungserg_ausf"] = str_categorical(
        raw_data["pergz"],
        ordered=False,
        reduce=True,
    )  # TODO: categories [29] and [39] have identical labels, by reducing we loose some information (seems most in line with old code)
    out["hh_position_raw_last_year"] = str_categorical(
        raw_data["pzugv"],
        ordered=False,
        reduce=True,
    )  # TODO: categories [19] and [39] have identical labels, by reducing we loose some information (seems most in line with old code)
    out["teilnahmebereitschaft"] = str_categorical(
        raw_data["ber"],
        ordered=True,
        renaming={
            "[4] sehr schlecht": "sehr schlecht",
            "[3] schlecht": "schlecht",
            "[2] gut": "gut",
            "[1] sehr gut": "sehr gut",
        },
    )
    out["bearbeitungserg_alt"] = str_categorical(raw_data["hergs"])
    return out


def pequiv(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pequiv dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["gender"] = str_categorical(
        raw_data["d11102ll"],
        ordered=False,
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
    out["uebergangsgeld_soep_prev"] = int_categorical_to_int(raw_data["ieret"])
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
    out["med_pe_zufrieden"] = categorical_to_int_categorical(
        raw_data["m11125"],
        renaming={
            "[0] Completely dissatisfied": 0,
            1: 1,
            2: 2,
            3: 3,
            4: 4,
            5: 5,
            6: 6,
            7: 7,
            8: 8,
            9: 9,
            "[10] Completely satisfied": 10,
        },
    )
    out["med_pe_subj_status"] = categorical_to_int_categorical(
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
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["nationality_first"] = str_categorical(raw_data["pgnation"], ordered=False)
    out["german"] = out["nationality_first"].dropna() == "Deutschland"
    out["status_refugee"] = str_categorical(raw_data["pgstatus_refu"], ordered=False)
    out["marital_status"] = str_categorical(raw_data["pgfamstd"], ordered=False)
    out["curr_earnings_m"] = float_categorical_to_float(raw_data["pglabgro"])
    out["net_wage_m"] = float_categorical_to_float(raw_data["pglabnet"])
    out["occupation_status"] = str_categorical(raw_data["pgstib"], ordered=False)
    out["employment_status"] = str_categorical(
        raw_data["pgemplst"],
        ordered=False,
    )
    out["laborf_status"] = str_categorical(raw_data["pglfs"], ordered=False)
    out["dauer_im_betrieb"] = float_categorical_to_float(raw_data["pgerwzeit"])
    out["weekly_working_hours_actual"] = replace_conditionally(
        float_categorical_to_float(
            raw_data["pgtatzeit"],
        ),
        out["employment_status"],
        "Nicht erwerbstätig",
        0,
    )
    out["weekly_working_hours_contract"] = replace_conditionally(
        float_categorical_to_float(
            raw_data["pgvebzeit"],
        ),
        out["employment_status"],
        "Nicht erwerbstätig",
        0,
    )
    out["public_service"] = bool_categorical(
        raw_data["pgoeffd"],
        renaming={"[2] nein": False, "[1] ja": True},
        ordered=True,
    )
    out["size_company_raw"] = str_categorical(
        raw_data["pgbetr"].replace({-5: "[-5] in Fragebogenversion nicht enthalten"}),
        ordered=False,
    )  # TODO: report missing encoding here
    out["size_company"] = str_categorical(raw_data["pgallbet"], ordered=False)
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
        ordered=True,
    )
    out["education_isced_cat"] = str_categorical_to_int_categorical(
        out["education_isced"],
        ordered=True,
    )
    out["education_casmin"] = str_categorical(
        raw_data["pgcasmin"],
        nr_identifiers=2,
        ordered=True,
    )
    out["education_casmin_cat"] = str_categorical_to_int_categorical(
        out["education_casmin"],
        ordered=True,
    )
    out["month_interview"] = categorical_to_int_categorical(
        raw_data["pgmonth"],
        renaming=month_mapping.de,
    )
    return out


def pkal(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pkal dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw_data["cid"])
    out["year"] = apply_lowest_int_dtype(float_categorical_to_int(raw_data["syear"]))

    three_letter_m = [i.split(" ", 1)[1][:3] for i in month_mapping.de]
    for m in range(1, 13):
        two_digit = f"{m:02d}"
        out[f"full_empl_v1_prev_{m}"] = str_categorical(
            raw_data[f"kal1a0{two_digit}_v1"],
            renaming={"[1] Ja": f"{three_letter_m[m-1]} Vollzeit erwerbst."},
            ordered=False,
        )
        out[f"full_empl_v2_prev_{m}"] = str_categorical(
            raw_data[f"kal1a0{two_digit}_v2"],
            ordered=False,
        )
        out[f"half_empl_prev_{m}"] = str_categorical(
            raw_data[f"kal1b0{two_digit}"],
            ordered=False,
        )
        out[f"mini_job_prev_{m}"] = int_categorical_to_int(
            raw_data[f"kal1n0{two_digit}"],
        )

    out["unempl_months_prev"] = int_categorical_to_int(raw_data["kal1d02"])
    out["rente_monate_prev"] = int_categorical_to_int(raw_data["kal1e02"])
    out["m_alg_prev"] = int_to_int_categorical(
        float_categorical_to_int(raw_data["kal2f02"]),
    )
    out["mschaftsgeld_bezogen_prev"] = str_categorical(
        raw_data["kal2j01_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )  # TODO: replaced var kal2j01 by kal2j01_h
    out["mschaftsgeld_monate_prev"] = int_to_int_categorical(
        float_categorical_to_int(raw_data["kal2j02"]),
    )
    # TODO: discuss/transform full_empl, half_empl_prev mini_job_prev into long format?
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
    )
    out["dauer_letzte_stelle_m"] = int_categorical_to_int(
        raw_data["plb0302"],
    )
    out["letzte_stelle_betriebsstilll"] = str_categorical(
        raw_data["plb0304_v11"],
        ordered=False,
    )
    out["letzte_stelle_grund_1999"] = str_categorical(
        raw_data["plb0304_v13"],
        ordered=False,
    )
    out["letzte_stelle_grund"] = str_categorical(
        raw_data["plb0304_v14"],
        ordered=False,
    )
    out["actv_work_search"] = bool_categorical(
        raw_data["plb0424_v2"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_art"] = str_categorical(
        raw_data["plb0460"],
        ordered=False,
    )
    out["wage_employee_m_prev"] = float_categorical_to_float(
        raw_data["plb0471_h"],
    )
    out["mschaftsgeld_prev"] = bool_categorical(
        raw_data["plc0126_v1"],
        renaming={"[1] Ja": True},
    )
    out["arbeitslosengeld_empf"] = bool_categorical(
        raw_data["plc0130_v1"],
        renaming={"[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_m3_m5_empf"] = str_categorical(
        raw_data["plc0130_v2"],
        ordered=False,
    )
    out["mschaftsgeld_vorm_pl"] = bool_categorical(
        raw_data["plc0152_v1"],
        renaming={"[1] Ja": True},
    )
    out["mschaftsgeld_brutto_m"] = int_categorical_to_int(
        raw_data["plc0153_h"],
    )  # TODO: Check on full dataset
    out["mschaftsgeld_betrag_pl_prev"] = int_categorical_to_int(
        raw_data["plc0155_h"],
    )
    out["child_alimony_before_2016"] = int_categorical_to_int(
        raw_data["plc0178"],
    )  # TODO: Check on full dataset
    out["in_priv_rente_eingezahlt"] = bool_categorical(
        raw_data["plc0437"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["in_priv_rente_eingezahlt_monate"] = int_categorical_to_int(
        raw_data["plc0438"],
    )  # TODO: Check on full dataset
    out["prv_rente_beitr_2013_m"] = int_categorical_to_int(
        raw_data["plc0439_v1"],
    )  # TODO: Check on full dataset
    out["prv_rente_beitr_2018_m"] = int_categorical_to_int(
        raw_data["plc0439_v2"],
    )  # TODO: Check on full dataset
    out["med_pl_schw_treppen"] = agreement_to_int_categorical(
        raw_data["ple0004"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_schw_taten"] = agreement_to_int_categorical(
        raw_data["ple0005"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_groesse"] = float_categorical_to_float(
        raw_data["ple0006"],
    )  # TODO: Check on full dataset
    out["med_pl_gewicht"] = float_categorical_to_float(
        raw_data["ple0007"],
    )  # TODO: Check on full dataset
    out["med_pl_subj_status"] = agreement_to_int_categorical(
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
    out["med_pl_schlaf"] = str_categorical(
        raw_data["ple0011"],
    )
    out["med_pl_diabetes"] = str_categorical(
        raw_data["ple0012"],
    )
    out["med_pl_asthma"] = str_categorical(
        raw_data["ple0013"],
    )
    out["med_pl_herzkr"] = str_categorical(
        raw_data["ple0014"],
    )
    out["med_pl_krebs"] = str_categorical(
        raw_data["ple0015"],
    )
    out["med_pl_schlaganf"] = str_categorical(
        raw_data["ple0016"],
    )
    out["med_pl_migraene"] = str_categorical(
        raw_data["ple0017"],
    )
    out["med_pl_bluthdrck"] = str_categorical(
        raw_data["ple0018"],
    )
    out["med_pl_depressiv"] = str_categorical(
        raw_data["ple0019"],
    )
    out["med_pl_demenz"] = str_categorical(
        raw_data["ple0020"],
    )
    out["med_pl_gelenk"] = str_categorical(
        raw_data["ple0021"],
    )
    out["med_pl_ruecken"] = str_categorical(
        raw_data["ple0022"],
    )
    out["med_pl_sonst"] = str_categorical(
        raw_data["ple0023"],
    )
    out["disability_degree"] = int_categorical_to_int(raw_data["ple0041"])
    out["med_pl_raucher"] = str_categorical(
        raw_data["ple0081_h"],
    )
    out["art_kv"] = str_categorical(
        raw_data["ple0097"],
        ordered=False,
    )
    out["politics_left_right"] = agreement_to_int_categorical(
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
    out["interest_politics"] = agreement_to_int_categorical(
        raw_data["plh0007"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Stark": 2,
            "[3] Nicht so stark": 3,
            "[4] Ueberhaupt nicht": 4,
        },
    )
    out["party_affiliation_any"] = str_categorical(
        raw_data["plh0011_h"],
    )
    out["party_affiliation"] = str_categorical(
        raw_data["plh0012_h"],
        ordered=False,
    )
    out["party_affiliation_intensity"] = agreement_to_int_categorical(
        raw_data["plh0013_h"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Ziemlich stark": 2,
            "[3] Maessig": 3,
            "[4] Ziemlich schwach": 4,
            "[5] Sehr schwach": 5,
        },
    )
    out["importance_career"] = agreement_to_int_categorical(
        raw_data["plh0107"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["importance_children"] = agreement_to_int_categorical(
        raw_data["plh0110"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["lebenszufriedenheit"] = agreement_to_int_categorical(
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
    out["general_trust"] = agreement_to_int_categorical(
        raw_data["plh0192"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Lehne eher ab": 3,
            "[4] Lehne voll ab": 4,
        },
    )
    out["confession"] = str_categorical(
        raw_data["plh0258_h"],
        ordered=False,
    )
    out["confession_any"] = str_categorical(
        raw_data["plh0258_v9"],
        ordered=False,
    )
    out["norm_child_suffers_under_6"] = agreement_to_int_categorical(
        raw_data["plh0298_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_6_2018"] = agreement_to_int_categorical(
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
    out["norm_marry_when_together"] = agreement_to_int_categorical(
        raw_data["plh0300_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_marry_when_together_2018"] = agreement_to_int_categorical(
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
    out["norm_women_family_priority"] = agreement_to_int_categorical(
        raw_data["plh0301"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3"] = agreement_to_int_categorical(
        raw_data["plh0302_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3_2018"] = agreement_to_int_categorical(
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
    out["norm_men_chores"] = agreement_to_int_categorical(
        raw_data["plh0303"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_ch_suffers_father_career"] = agreement_to_int_categorical(
        raw_data["plh0304"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar"] = agreement_to_int_categorical(
        raw_data["plh0308_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar_2018"] = agreement_to_int_categorical(
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
    out["norm_career_mothers_same_warmth"] = agreement_to_int_categorical(
        raw_data["plh0309"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["importance_faith"] = agreement_to_int_categorical(
        raw_data["plh0343_v1"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["importance_faith_v2"] = agreement_to_int_categorical(
        raw_data["plh0343_v2"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["h_wage_pl"] = float_categorical_to_float(
        raw_data["plh0354_h"],
    )  # TODO: Check on whole dataset
    out["hours_work_sat"] = float_categorical_to_float(raw_data["pli0003_h"])
    out["hours_work_sun"] = float_categorical_to_float(raw_data["pli0007_h"])
    out["hours_hobbies_sun"] = int_categorical_to_int(raw_data["pli0010"])
    out["hours_errands_sun"] = int_categorical_to_int(raw_data["pli0011"])
    out["hours_housework_sat"] = float_categorical_to_float(raw_data["pli0012_h"])
    out["hours_housework_sun"] = float_categorical_to_float(raw_data["pli0016_h"])
    out["hours_childcare_sat"] = float_categorical_to_float(raw_data["pli0019_h"])
    out["hours_childcare_sun"] = float_categorical_to_float(raw_data["pli0022_h"])
    out["hours_repairs_sat"] = float_categorical_to_float(raw_data["pli0031_h"])
    out["hours_repairs_sun"] = float_categorical_to_float(raw_data["pli0034_v4"])
    out["hours_hobbies_sat"] = float_categorical_to_float(raw_data["pli0036"])
    out["hours_work_workday"] = float_categorical_to_float(raw_data["pli0038_h"])
    out["hours_errands_workday"] = int_categorical_to_int(raw_data["pli0040"])
    out["hours_housework_workday"] = int_categorical_to_int(raw_data["pli0043_h"])
    out["hours_childcare_workday"] = int_categorical_to_int(raw_data["pli0044_h"])
    out["hours_care_workday"] = float_categorical_to_float(
        raw_data["pli0046"],
    )  # TODO: Check on full dataset
    out["hours_repairs_workday"] = int_categorical_to_int(raw_data["pli0049_h"])
    out["hours_hobbies_workday"] = int_categorical_to_int(raw_data["pli0051"])
    out["hours_errands_sat"] = int_categorical_to_int(raw_data["pli0054"])
    out["hours_care_sat"] = float_categorical_to_float(
        raw_data["pli0055"],
    )  # TODO: Check on full dataset
    out["hours_care_sun"] = float_categorical_to_float(
        raw_data["pli0057"],
    )  # TODO: Check on full dataset
    out["hours_sleep_workday"] = float_categorical_to_float(
        raw_data["pli0059"],
    )  # TODO: Check on full dataset
    out["hours_sleep_weekend"] = float_categorical_to_float(
        raw_data["pli0060"],
    )  # TODO: Check on full dataset
    out["motor_disability"] = bool_categorical(
        raw_data["plj0582"],
    )  # TODO: Check on full dataset
    out["trust_public_admin"] = agreement_to_int_categorical(
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
    )  # TODO: Are all categories covered?
    out["trust_government"] = agreement_to_int_categorical(
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
    )  # TODO: Are all categories covered?

    return out


def ppathl(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the ppathl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = apply_lowest_int_dtype(raw_data["hid"], remove_negatives=True)
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["current_east_west"] = str_categorical(
        raw_data["sampreg"],
        renaming={
            "[1] Westdeutschland, alte Bundeslaender": "Westdeutschland (alte Bundeslaender)",
            "[2] Ostdeutschland, neue Bundeslaender": "Ostdeutschland (neue Bundeslaender)",
        },
        ordered=False,
    )
    out["befragungsstatus"] = str_categorical(raw_data["netto"], ordered=False)
    out["year_immigration"] = apply_lowest_int_dtype(
        raw_data["immiyear"],
        remove_negatives=True,
    )
    out["born_in_germany"] = str_categorical(raw_data["germborn"], ordered=False)
    out["country_of_birth"] = str_categorical(raw_data["corigin"], ordered=False)
    out["birth_month_ppathl"] = categorical_to_int_categorical(
        raw_data["gebmonat"],
        ordered=False,
        renaming=month_mapping.de,
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
