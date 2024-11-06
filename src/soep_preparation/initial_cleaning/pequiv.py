import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    bool_categorical,
    categorical_to_int_categorical,
    int_categorical_to_int,
    str_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pequiv dataset."""
    out = pd.DataFrame()
    out["hh_id_orig"] = apply_lowest_int_dtype(raw["cid"])
    out["hh_id"] = apply_lowest_int_dtype(raw["hid"])
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
    out["self_empl_earnings_prev"] = int_categorical_to_int(raw["iself"]).fillna(0)
    out["arbeitsl_geld_soep_prev"] = int_categorical_to_int(raw["iunby"])
    out["arbeitsl_hilfe_soep_prev"] = int_categorical_to_int(raw["iunay"])
    out["unterhaltsgeld_soep_prev"] = int_categorical_to_int(raw["isuby"])
    out["uebergangsgeld_soep_prev"] = int_categorical_to_int(raw["ieret"])
    out["mutterschaftsgeld_soep_prev"] = int_categorical_to_int(raw["imaty"])
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
