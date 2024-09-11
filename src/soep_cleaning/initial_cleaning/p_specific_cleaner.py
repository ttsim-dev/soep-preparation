from soep_cleaning.config import pd
from soep_cleaning.initial_cleaning import month_mapping
from soep_cleaning.initial_cleaning.helper import (
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


def pbrutto(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pbrutto dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["soep_initial_hh_id"] = int_categorical_to_int(raw["cid"])
    out["soep_hh_id"] = int_categorical_to_int(raw["hid"])
    out["year"] = int_categorical_to_int(raw["syear"])
    out["birth_year"] = int_categorical_to_int(raw["geburt_v2"])
    out["befragungs_status"] = str_categorical(raw["befstat_h"])
    out["hh_position"] = str_categorical(
        raw["stell_h"],
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
    out["bearbeitungserg"] = str_categorical(raw["perg"], ordered=False)
    out["bearbeitungserg_ausf"] = str_categorical(
        raw["pergz"],
        ordered=False,
        reduce=True,
    )  # TODO: categories [29] and [39] have identical labels, by reducing we loose some information (seems most in line with old code)
    out["hh_position_raw_last_year"] = str_categorical(
        raw["pzugv"],
        ordered=False,
        reduce=True,
    )  # TODO: categories [19] and [39] have identical labels, by reducing we loose some information (seems most in line with old code)
    out["teilnahmebereitschaft"] = str_categorical(
        raw["ber"],
        ordered=True,
        renaming={
            "[4] sehr schlecht": "sehr schlecht",
            "[3] schlecht": "schlecht",
            "[2] gut": "gut",
            "[1] sehr gut": "sehr gut",
        },
    )
    out["bearbeitungserg_alt"] = str_categorical(raw["hergs"])
    return out


def pequiv(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pequiv dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["gender"] = str_categorical(
        raw["d11102ll"],
        ordered=False,
        renaming={"[1] Male": "male", "[2] Female": "female"},
    )
    out["age"] = int_categorical_to_int(raw["d11101"])
    out["hh_size"] = apply_lowest_int_dtype(raw["d11106"])
    out["anz_minderj_hh1"] = apply_lowest_int_dtype(raw["d11107"])
    out["annual_work_h_prev"] = int_categorical_to_int(raw["e11101"])
    out["employment_status_pequiv"] = str_categorical(raw["e11102"], ordered=False)
    out["employment_level_pequiv"] = str_categorical(raw["e11103"], ordered=False)
    out["einkommen_vor_steuer_hh_prev"] = int_categorical_to_int(raw["i11101"])
    out["einkommen_nach_steuer_hh_prev"] = int_categorical_to_int(raw["i11102"])
    out["labor_earnings_prev"] = int_categorical_to_int(raw["i11110"])
    out["bundesland"] = str_categorical(raw["l11101"], ordered=False)
    out["earnings_job_1_prev"] = int_categorical_to_int(raw["ijob1"])
    out["earnings_job_2_prev"] = int_categorical_to_int(raw["ijob2"])
    out["self_empl_earnings_prev"] = int_categorical_to_int(raw["iself"])
    out["arbeitsl_geld_soep_prev"] = int_categorical_to_int(raw["iunby"])
    out["arbeitsl_hilfe_soep_prev"] = int_categorical_to_int(raw["iunay"])
    out["unterhaltsgeld_soep_prev"] = int_categorical_to_int(raw["isuby"])
    out["uebergangsgeld_soep_prev"] = int_categorical_to_int(raw["ieret"])
    out["mschaftsgeld_soep_prev"] = int_categorical_to_int(raw["imaty"])
    out["student_grants_prev"] = int_categorical_to_int(raw["istuy"])
    out["alimony_prev"] = int_categorical_to_int(raw["ialim"])
    out["other_transfers_prev"] = int_categorical_to_int(raw["ielse"])
    out["christmas_bonus_prev"] = int_categorical_to_int(raw["ixmas"])
    out["vacation_bonus_prev"] = int_categorical_to_int(raw["iholy"])
    out["profit_share_prev"] = int_categorical_to_int(raw["igray"])
    out["other_bonuses_prev"] = int_categorical_to_int(raw["iothy"])
    out["gov_ret_pay_prev"] = int_categorical_to_int(raw["igrv1"])
    out["gov_ret_pay_widow_prev"] = int_categorical_to_int(raw["igrv2"])
    out["rental_income_y_hh_prev"] = int_categorical_to_int(raw["renty"])
    out["maintenance_c_hh_prev"] = int_categorical_to_int(raw["opery"])
    out["capital_income_hh_prev"] = int_categorical_to_int(raw["divdy"])
    out["kindergeld_pequiv_hh_prev"] = int_categorical_to_int(raw["chspt"])
    out["wohngeld_soep_hh_prev"] = int_categorical_to_int(raw["house"])
    out["pflegegeld_hh_prev"] = int_categorical_to_int(raw["nursh"])
    out["soc_assist_hh_prev"] = int_categorical_to_int(raw["subst"])
    out["soc_assist_spec_hh_prev"] = int_categorical_to_int(raw["sphlp"])
    out["housing_sup_hh_prev"] = int_categorical_to_int(raw["hsup"])
    out["soc_miners_pension_prev"] = int_categorical_to_int(raw["ismp1"])
    out["civ_serv_pension_prev"] = int_categorical_to_int(raw["iciv1"])
    out["warvictim_pension_prev"] = int_categorical_to_int(raw["iwar1"])
    out["farmer_pension_prev"] = int_categorical_to_int(raw["iagr1"])
    out["stat_accident_insurance_prev"] = int_categorical_to_int(raw["iguv1"])
    out["civ_supp_benefits_prev"] = int_categorical_to_int(raw["ivbl1"])
    out["company_pen_prev"] = int_categorical_to_int(raw["icom1"])
    out["private_pen_prev"] = int_categorical_to_int(raw["iprv1"])
    out["other_pen_prev"] = int_categorical_to_int(raw["ison1"])
    out["soc_miners_pension_widow_prev"] = int_categorical_to_int(raw["ismp2"])
    out["civ_serv_pension_widow_prev"] = int_categorical_to_int(raw["iciv2"])
    out["warvictim_pension_widow_prev"] = int_categorical_to_int(raw["iwar2"])
    out["farmer_pension_widow_prev"] = int_categorical_to_int(raw["iagr2"])
    out["stat_accident_insur_widow_prev"] = int_categorical_to_int(raw["iguv2"])
    out["civ_supp_benefits_widow_prev"] = int_categorical_to_int(raw["ivbl2"])
    out["company_pen_widow_prev"] = int_categorical_to_int(raw["icom2"])
    out["other_pen_widow_prev"] = int_categorical_to_int(raw["ison2"])
    out["private_pen_widow_prev"] = int_categorical_to_int(raw["iprv2"])

    out["med_pe_krnkhaus"] = bool_categorical(
        raw["m11101"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schlaganf"] = bool_categorical(
        raw["m11105"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_bluthdrck"] = bool_categorical(
        raw["m11106"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_diabetes"] = bool_categorical(
        raw["m11107"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_krebs"] = bool_categorical(
        raw["m11108"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_psych"] = bool_categorical(
        raw["m11109"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_gelenk"] = bool_categorical(
        raw["m11110"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_herzkr"] = bool_categorical(
        raw["m11111"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_schw_treppen"] = bool_categorical(
        raw["m11113"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_anziehen"] = bool_categorical(
        raw["m11115"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_bett"] = bool_categorical(
        raw["m11116"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_einkauf"] = bool_categorical(
        raw["m11117"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_hausarb"] = bool_categorical(
        raw["m11119"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_groesse"] = int_categorical_to_int(raw["m11122"])
    out["med_pe_gewicht"] = int_categorical_to_int(raw["m11123"])
    out["med_pe_zufrieden"] = categorical_to_int_categorical(
        raw["m11125"],
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
        raw["m11126"],
        renaming={
            "[1] Very good": 1,
            "[2] Good": 2,
            "[3] Satisfactory": 3,
            "[4] Poor": 4,
            "[5] Bad": 5,
        },
    )
    out["soc_assist_eld_hh_prev"] = int_categorical_to_int(raw["ssold"])
    out["arbeitsl_geld_2_soep_hh_prev"] = int_categorical_to_int(raw["alg2"])
    out["kinderzuschlag_pequiv_hh_prev"] = int_categorical_to_int(raw["adchb"])
    out["adv_child_maint_payment_prev"] = int_categorical_to_int(raw["iachm"])
    out["childcare_subsidy_hh_prev"] = int_categorical_to_int(raw["chsub"])
    out["caregiver_alimony_prev"] = int_categorical_to_int(raw["ichsu"])
    out["divorce_alimony_prev"] = int_categorical_to_int(raw["ispou"])
    out["riester_prev"] = int_categorical_to_int(raw["irie1"])
    out["riester_widow_prev"] = int_categorical_to_int(raw["irie2"])
    return out


def pgen(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pgen dataset."""
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["nationality_first"] = str_categorical(raw["pgnation"], ordered=False)
    out["german"] = out["nationality_first"].dropna() == "Deutschland"
    out["status_refugee"] = str_categorical(raw["pgstatus_refu"], ordered=False)
    out["marital_status"] = str_categorical(raw["pgfamstd"], ordered=False)
    out["curr_earnings_m"] = float_categorical_to_float(raw["pglabgro"])
    out["net_wage_m"] = float_categorical_to_float(raw["pglabnet"])
    out["occupation_status"] = str_categorical(raw["pgstib"], ordered=False)
    out["employment_status"] = str_categorical(
        raw["pgemplst"],
        ordered=False,
    )
    out["laborf_status"] = str_categorical(raw["pglfs"], ordered=False)
    out["dauer_im_betrieb"] = float_categorical_to_float(raw["pgerwzeit"])
    out["weekly_working_hours_actual"] = replace_conditionally(
        float_categorical_to_float(
            raw["pgtatzeit"],
        ),
        out["employment_status"],
        "Nicht erwerbstätig",
        0,
    )
    out["weekly_working_hours_contract"] = replace_conditionally(
        float_categorical_to_float(
            raw["pgvebzeit"],
        ),
        out["employment_status"],
        "Nicht erwerbstätig",
        0,
    )
    out["public_service"] = bool_categorical(
        raw["pgoeffd"],
        renaming={"[2] nein": False, "[1] ja": True},
        ordered=True,
    )
    out["size_company_raw"] = str_categorical(
        raw["pgbetr"].replace({-5: "[-5] in Fragebogenversion nicht enthalten"}),
        ordered=False,
    )  # TODO: report missing encoding here
    out["size_company"] = str_categorical(raw["pgallbet"], ordered=False)
    out["pgen_grund_beschäftigungsende"] = str_categorical(
        raw["pgjobend"],
        ordered=False,
    )
    out["exp_full_time"] = float_categorical_to_float(raw["pgexpft"])
    out["exp_part_time"] = float_categorical_to_float(raw["pgexppt"])
    out["exp_unempl"] = float_categorical_to_float(raw["pgexpue"])
    out["education_isced_alt"] = str_categorical(raw["pgisced97"])
    out["education_isced"] = str_categorical(
        raw["pgisced11"],
        ordered=True,
    )
    out["education_isced_cat"] = str_categorical_to_int_categorical(
        out["education_isced"],
        ordered=True,
    )
    out["education_casmin"] = str_categorical(
        raw["pgcasmin"],
        nr_identifiers=2,
        ordered=True,
    )
    out["education_casmin_cat"] = str_categorical_to_int_categorical(
        out["education_casmin"],
        ordered=True,
    )
    out["month_interview"] = categorical_to_int_categorical(
        raw["pgmonth"],
        renaming=month_mapping.de,
    )
    return out


def pkal(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pkal dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"])
    out["soep_initial_hh_id"] = apply_lowest_int_dtype(raw["cid"])
    out["year"] = apply_lowest_int_dtype(float_categorical_to_int(raw["syear"]))

    out["full_empl_v1_prev_1"] = str_categorical(
        raw["kal1a001_v1"],
        renaming={"[1] Ja": "Jan Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_2"] = str_categorical(
        raw["kal1a002_v1"],
        renaming={"[1] Ja": "Feb Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_3"] = str_categorical(
        raw["kal1a003_v1"],
        renaming={"[1] Ja": "Mär Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_4"] = str_categorical(
        raw["kal1a004_v1"],
        renaming={"[1] Ja": "Apr Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_5"] = str_categorical(
        raw["kal1a005_v1"],
        renaming={"[1] Ja": "Mai Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_6"] = str_categorical(
        raw["kal1a006_v1"],
        renaming={"[1] Ja": "Jun Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_7"] = str_categorical(
        raw["kal1a007_v1"],
        renaming={"[1] Ja": "Jul Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_8"] = str_categorical(
        raw["kal1a008_v1"],
        renaming={"[1] Ja": "Aug Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_9"] = str_categorical(
        raw["kal1a009_v1"],
        renaming={"[1] Ja": "Sep Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_10"] = str_categorical(
        raw["kal1a010_v1"],
        renaming={"[1] Ja": "Okt Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_11"] = str_categorical(
        raw["kal1a011_v1"],
        renaming={"[1] Ja": "Nov Vollzeit erwerbst."},
        ordered=False,
    )
    out["full_empl_v1_prev_12"] = str_categorical(
        raw["kal1a012_v1"],
        renaming={"[1] Ja": "Dez Vollzeit erwerbst."},
        ordered=False,
    )

    out["full_empl_v2_prev_1"] = str_categorical(
        raw["kal1a001_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_2"] = str_categorical(
        raw["kal1a002_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_3"] = str_categorical(
        raw["kal1a003_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_4"] = str_categorical(
        raw["kal1a004_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_5"] = str_categorical(
        raw["kal1a005_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_6"] = str_categorical(
        raw["kal1a006_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_7"] = str_categorical(
        raw["kal1a007_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_8"] = str_categorical(
        raw["kal1a008_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_9"] = str_categorical(
        raw["kal1a009_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_10"] = str_categorical(
        raw["kal1a010_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_11"] = str_categorical(
        raw["kal1a011_v2"],
        ordered=False,
    )
    out["full_empl_v2_prev_12"] = str_categorical(
        raw["kal1a012_v2"],
        ordered=False,
    )

    out["half_empl_prev_1"] = str_categorical(
        raw["kal1b001"],
        ordered=False,
    )
    out["half_empl_prev_2"] = str_categorical(
        raw["kal1b002"],
        ordered=False,
    )
    out["half_empl_prev_3"] = str_categorical(
        raw["kal1b003"],
        ordered=False,
    )
    out["half_empl_prev_4"] = str_categorical(
        raw["kal1b004"],
        ordered=False,
    )
    out["half_empl_prev_5"] = str_categorical(
        raw["kal1b005"],
        ordered=False,
    )
    out["half_empl_prev_6"] = str_categorical(
        raw["kal1b006"],
        ordered=False,
    )
    out["half_empl_prev_7"] = str_categorical(
        raw["kal1b007"],
        ordered=False,
    )
    out["half_empl_prev_8"] = str_categorical(
        raw["kal1b008"],
        ordered=False,
    )
    out["half_empl_prev_9"] = str_categorical(
        raw["kal1b009"],
        ordered=False,
    )
    out["half_empl_prev_10"] = str_categorical(
        raw["kal1b010"],
        ordered=False,
    )
    out["half_empl_prev_11"] = str_categorical(
        raw["kal1b011"],
        ordered=False,
    )
    out["half_empl_prev_12"] = str_categorical(
        raw["kal1b012"],
        ordered=False,
    )

    out["mini_job_prev_1"] = int_categorical_to_int(
        raw["kal1n001"],
    )
    out["mini_job_prev_2"] = int_categorical_to_int(
        raw["kal1n002"],
    )
    out["mini_job_prev_3"] = int_categorical_to_int(
        raw["kal1n003"],
    )
    out["mini_job_prev_4"] = int_categorical_to_int(
        raw["kal1n004"],
    )
    out["mini_job_prev_5"] = int_categorical_to_int(
        raw["kal1n005"],
    )
    out["mini_job_prev_6"] = int_categorical_to_int(
        raw["kal1n006"],
    )
    out["mini_job_prev_7"] = int_categorical_to_int(
        raw["kal1n007"],
    )
    out["mini_job_prev_8"] = int_categorical_to_int(
        raw["kal1n008"],
    )
    out["mini_job_prev_9"] = int_categorical_to_int(
        raw["kal1n009"],
    )
    out["mini_job_prev_10"] = int_categorical_to_int(
        raw["kal1n010"],
    )
    out["mini_job_prev_11"] = int_categorical_to_int(
        raw["kal1n011"],
    )
    out["mini_job_prev_12"] = int_categorical_to_int(
        raw["kal1n012"],
    )

    out["unempl_months_prev"] = int_categorical_to_int(raw["kal1d02"])
    out["rente_monate_prev"] = int_categorical_to_int(raw["kal1e02"])
    out["m_alg_prev"] = int_to_int_categorical(
        float_categorical_to_int(raw["kal2f02"]),
    )
    out["mschaftsgeld_bezogen_prev"] = str_categorical(
        raw["kal2j01_h"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )  # TODO: replaced var kal2j01 by kal2j01_h
    out["mschaftsgeld_monate_prev"] = int_to_int_categorical(
        float_categorical_to_int(raw["kal2j02"]),
    )
    # TODO: discuss/transform full_empl, half_empl_prev mini_job_prev into long format?
    return out


def pl(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pl dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
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
    out["dauer_letzte_stelle_m"] = int_categorical_to_int(
        raw["plb0302"],
    )
    out["letzte_stelle_betriebsstilll"] = str_categorical(
        raw["plb0304_v11"],
        ordered=False,
    )
    out["letzte_stelle_grund_1999"] = str_categorical(
        raw["plb0304_v13"],
        ordered=False,
    )
    out["letzte_stelle_grund"] = str_categorical(
        raw["plb0304_v14"],
        ordered=False,
    )
    out["actv_work_search"] = bool_categorical(
        raw["plb0424_v2"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["altersteilzeit_art"] = str_categorical(
        raw["plb0460"],
        ordered=False,
    )
    out["wage_employee_m_prev"] = float_categorical_to_float(
        raw["plb0471_h"],
    )
    out["mschaftsgeld_prev"] = bool_categorical(
        raw["plc0126_v1"],
        renaming={"[1] Ja": True},
    )
    out["arbeitslosengeld_empf"] = bool_categorical(
        raw["plc0130_v1"],
        renaming={"[1] Ja": True},
        ordered=True,
    )
    out["arbeitslosengeld_m3_m5_empf"] = str_categorical(
        raw["plc0130_v2"],
        ordered=False,
    )
    out["mschaftsgeld_vorm_pl"] = bool_categorical(
        raw["plc0152_v1"],
        renaming={"[1] Ja": True},
    )
    out["mschaftsgeld_brutto_m"] = int_categorical_to_int(
        raw["plc0153_h"],
    )  # TODO: Check on full dataset
    out["mschaftsgeld_betrag_pl_prev"] = int_categorical_to_int(
        raw["plc0155_h"],
    )
    out["child_alimony_before_2016"] = int_categorical_to_int(
        raw["plc0178"],
    )  # TODO: Check on full dataset
    out["in_priv_rente_eingezahlt"] = bool_categorical(
        raw["plc0437"],
        renaming={"[2] Nein": False, "[1] Ja": True},
        ordered=True,
    )
    out["in_priv_rente_eingezahlt_monate"] = int_categorical_to_int(
        raw["plc0438"],
    )  # TODO: Check on full dataset
    out["prv_rente_beitr_2013_m"] = int_categorical_to_int(
        raw["plc0439_v1"],
    )  # TODO: Check on full dataset
    out["prv_rente_beitr_2018_m"] = int_categorical_to_int(
        raw["plc0439_v2"],
    )  # TODO: Check on full dataset
    out["med_pl_schw_treppen"] = agreement_to_int_categorical(
        raw["ple0004"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_schw_taten"] = agreement_to_int_categorical(
        raw["ple0005"],
        renaming={"[3] Gar nicht": 0, "[2] Ein wenig": 1, "[1] Stark": 2},
        ordered=True,
    )
    out["med_pl_groesse"] = float_categorical_to_float(
        raw["ple0006"],
    )  # TODO: Check on full dataset
    out["med_pl_gewicht"] = float_categorical_to_float(
        raw["ple0007"],
    )  # TODO: Check on full dataset
    out["med_pl_subj_status"] = agreement_to_int_categorical(
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
    out["med_pl_schlaf"] = str_categorical(
        raw["ple0011"],
    )
    out["med_pl_diabetes"] = str_categorical(
        raw["ple0012"],
    )
    out["med_pl_asthma"] = str_categorical(
        raw["ple0013"],
    )
    out["med_pl_herzkr"] = str_categorical(
        raw["ple0014"],
    )
    out["med_pl_krebs"] = str_categorical(
        raw["ple0015"],
    )
    out["med_pl_schlaganf"] = str_categorical(
        raw["ple0016"],
    )
    out["med_pl_migraene"] = str_categorical(
        raw["ple0017"],
    )
    out["med_pl_bluthdrck"] = str_categorical(
        raw["ple0018"],
    )
    out["med_pl_depressiv"] = str_categorical(
        raw["ple0019"],
    )
    out["med_pl_demenz"] = str_categorical(
        raw["ple0020"],
    )
    out["med_pl_gelenk"] = str_categorical(
        raw["ple0021"],
    )
    out["med_pl_ruecken"] = str_categorical(
        raw["ple0022"],
    )
    out["med_pl_sonst"] = str_categorical(
        raw["ple0023"],
    )
    out["disability_degree"] = int_categorical_to_int(raw["ple0041"])
    out["med_pl_raucher"] = str_categorical(
        raw["ple0081_h"],
    )
    out["art_kv"] = str_categorical(
        raw["ple0097"],
        ordered=False,
    )
    out["politics_left_right"] = agreement_to_int_categorical(
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
    out["interest_politics"] = agreement_to_int_categorical(
        raw["plh0007"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Stark": 2,
            "[3] Nicht so stark": 3,
            "[4] Ueberhaupt nicht": 4,
        },
    )
    out["party_affiliation_any"] = str_categorical(
        raw["plh0011_h"],
    )
    out["party_affiliation"] = str_categorical(
        raw["plh0012_h"],
        ordered=False,
    )
    out["party_affiliation_intensity"] = agreement_to_int_categorical(
        raw["plh0013_h"],
        renaming={
            "[1] Sehr stark": 1,
            "[2] Ziemlich stark": 2,
            "[3] Maessig": 3,
            "[4] Ziemlich schwach": 4,
            "[5] Sehr schwach": 5,
        },
    )
    out["importance_career"] = agreement_to_int_categorical(
        raw["plh0107"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["importance_children"] = agreement_to_int_categorical(
        raw["plh0110"],
        renaming={
            "[1] 1 Sehr wichtig": 1,
            "[2] 2 Wichtig": 2,
            "[3] 3 Weniger wichtig": 3,
            "[4] 4 Ganz unwichtig": 4,
        },
    )
    out["lebenszufriedenheit"] = agreement_to_int_categorical(
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
    out["general_trust"] = agreement_to_int_categorical(
        raw["plh0192"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Lehne eher ab": 3,
            "[4] Lehne voll ab": 4,
        },
    )
    out["confession"] = str_categorical(
        raw["plh0258_h"],
        ordered=False,
    )
    out["confession_any"] = str_categorical(
        raw["plh0258_v9"],
        ordered=False,
    )
    out["norm_child_suffers_under_6"] = agreement_to_int_categorical(
        raw["plh0298_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_6_2018"] = agreement_to_int_categorical(
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
    out["norm_marry_when_together"] = agreement_to_int_categorical(
        raw["plh0300_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_marry_when_together_2018"] = agreement_to_int_categorical(
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
    out["norm_women_family_priority"] = agreement_to_int_categorical(
        raw["plh0301"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3"] = agreement_to_int_categorical(
        raw["plh0302_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_child_suffers_under_3_2018"] = agreement_to_int_categorical(
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
    out["norm_men_chores"] = agreement_to_int_categorical(
        raw["plh0303"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_ch_suffers_father_career"] = agreement_to_int_categorical(
        raw["plh0304"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar"] = agreement_to_int_categorical(
        raw["plh0308_v1"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["norm_genders_similar_2018"] = agreement_to_int_categorical(
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
    out["norm_career_mothers_same_warmth"] = agreement_to_int_categorical(
        raw["plh0309"],
        renaming={
            "[1] Stimme voll zu": 1,
            "[2] Stimme eher zu": 2,
            "[3] Stimme eher nicht zu": 3,
            "[4] Stimme ueberhaupt nicht zu": 4,
        },
    )
    out["importance_faith"] = agreement_to_int_categorical(
        raw["plh0343_v1"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["importance_faith_v2"] = agreement_to_int_categorical(
        raw["plh0343_v2"],
        renaming={
            "[1] sehr wichtig": 1,
            "[2] wichtig": 2,
            "[3] weniger wichtig": 3,
            "[4] ganz unwichtig": 4,
        },
    )
    out["h_wage_pl"] = float_categorical_to_float(
        raw["plh0354_h"],
    )  # TODO: Check on whole dataset
    out["hours_work_sat"] = float_categorical_to_float(raw["pli0003_h"])
    out["hours_work_sun"] = float_categorical_to_float(raw["pli0007_h"])
    out["hours_hobbies_sun"] = int_categorical_to_int(raw["pli0010"])
    out["hours_errands_sun"] = int_categorical_to_int(raw["pli0011"])
    out["hours_housework_sat"] = float_categorical_to_float(raw["pli0012_h"])
    out["hours_housework_sun"] = float_categorical_to_float(raw["pli0016_h"])
    out["hours_childcare_sat"] = float_categorical_to_float(raw["pli0019_h"])
    out["hours_childcare_sun"] = float_categorical_to_float(raw["pli0022_h"])
    out["hours_repairs_sat"] = float_categorical_to_float(raw["pli0031_h"])
    out["hours_repairs_sun"] = float_categorical_to_float(raw["pli0034_v4"])
    out["hours_hobbies_sat"] = float_categorical_to_float(raw["pli0036"])
    out["hours_work_workday"] = float_categorical_to_float(raw["pli0038_h"])
    out["hours_errands_workday"] = int_categorical_to_int(raw["pli0040"])
    out["hours_housework_workday"] = int_categorical_to_int(raw["pli0043_h"])
    out["hours_childcare_workday"] = int_categorical_to_int(raw["pli0044_h"])
    out["hours_care_workday"] = float_categorical_to_float(
        raw["pli0046"],
    )  # TODO: Check on full dataset
    out["hours_repairs_workday"] = int_categorical_to_int(raw["pli0049_h"])
    out["hours_hobbies_workday"] = int_categorical_to_int(raw["pli0051"])
    out["hours_errands_sat"] = int_categorical_to_int(raw["pli0054"])
    out["hours_care_sat"] = float_categorical_to_float(
        raw["pli0055"],
    )  # TODO: Check on full dataset
    out["hours_care_sun"] = float_categorical_to_float(
        raw["pli0057"],
    )  # TODO: Check on full dataset
    out["hours_sleep_workday"] = float_categorical_to_float(
        raw["pli0059"],
    )  # TODO: Check on full dataset
    out["hours_sleep_weekend"] = float_categorical_to_float(
        raw["pli0060"],
    )  # TODO: Check on full dataset
    out["motor_disability"] = bool_categorical(
        raw["plj0582"],
    )  # TODO: Check on full dataset
    out["trust_public_admin"] = agreement_to_int_categorical(
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
    )  # TODO: Are all categories covered?
    out["trust_government"] = agreement_to_int_categorical(
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
    )  # TODO: Are all categories covered?

    return out


def ppathl(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the ppathl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = apply_lowest_int_dtype(raw["hid"], remove_negatives=True)
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["current_east_west"] = str_categorical(
        raw["sampreg"],
        renaming={
            "[1] Westdeutschland, alte Bundeslaender": "Westdeutschland (alte Bundeslaender)",
            "[2] Ostdeutschland, neue Bundeslaender": "Ostdeutschland (neue Bundeslaender)",
        },
        ordered=False,
    )
    out["befragungsstatus"] = str_categorical(raw["netto"], ordered=False)
    out["year_immigration"] = apply_lowest_int_dtype(
        raw["immiyear"],
        remove_negatives=True,
    )
    out["born_in_germany"] = str_categorical(raw["germborn"], ordered=False)
    out["country_of_birth"] = str_categorical(raw["corigin"], ordered=False)
    out["birth_month_ppathl"] = categorical_to_int_categorical(
        raw["gebmonat"],
        ordered=False,
        renaming=month_mapping.de,
    )
    out["east_west_1989"] = str_categorical(raw["loc1989"], ordered=False)
    out["migrationshintergrund"] = str_categorical(raw["migback"], ordered=False)
    out["sexual_orientation"] = str_categorical(raw["sexor"], ordered=False)
    out["birth_bundesland"] = str_categorical(raw["birthregion"], ordered=False)
    out["p_bleibe_wkeit"] = apply_lowest_float_dtype(raw["pbleib"])
    out["p_gewicht"] = apply_lowest_float_dtype(raw["phrf"])
    out["p_gewicht_nur_neue"] = apply_lowest_float_dtype(raw["phrf0"])
    out["p_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw["phrf1"])
    out["pointer_partner"] = apply_lowest_int_dtype(
        raw["parid"],
        remove_negatives=True,
    )
    out["has_partner"] = str_categorical(raw["partner"], ordered=False)

    return out
