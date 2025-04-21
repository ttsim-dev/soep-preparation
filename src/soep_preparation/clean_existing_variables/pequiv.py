"""Functions to pre-process variables for a raw pequiv dataset."""

import pandas as pd

from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    object_to_bool_categorical,
    object_to_int,
    object_to_int_categorical,
    object_to_str_categorical,
)


def _create_med_var_column(
    pid_sr: pd.Series,
    var_sr: pd.Series,
    renaming: dict,
    ordered: bool,  # noqa: FBT001
) -> pd.Series:
    data = pd.DataFrame({"pid": pid_sr, var_sr.name: var_sr})
    data[var_sr.name] = object_to_bool_categorical(
        data[var_sr.name],
        renaming=renaming,
        ordered=ordered,
    )
    return data.groupby("pid")[var_sr.name].ffill()


def _create_med_pe_subj_status_column(
    pid_sr: pd.Series,
    var_sr: pd.Series,
    renaming: dict,
    ordered: bool,  # noqa: FBT001
) -> pd.Series:
    data = pd.DataFrame({"pid": pid_sr, var_sr.name: var_sr})
    data[var_sr.name] = object_to_int_categorical(
        data[var_sr.name],
        renaming=renaming,
        ordered=ordered,
    )
    return data.groupby("pid")[var_sr.name].ffill()


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Clean the pequiv dataset."""
    out = pd.DataFrame()

    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])
    out["hh_id_orig"] = apply_lowest_int_dtype(raw_data["cid"])
    out["gender"] = object_to_str_categorical(
        raw_data["d11102ll"],
        ordered=False,
        renaming={"[1] Male": "male", "[2] Female": "female"},
    )
    out["age"] = object_to_int(raw_data["d11101"])
    out["hh_size"] = apply_lowest_int_dtype(raw_data["d11106"])

    out["anz_minderj_hh1"] = apply_lowest_int_dtype(raw_data["d11107"])
    out["annual_work_h_prev"] = object_to_int(raw_data["e11101"])
    out["employment_status_pequiv"] = object_to_str_categorical(
        raw_data["e11102"],
        ordered=False,
    )
    out["employment_level_pequiv"] = object_to_str_categorical(
        raw_data["e11103"],
        ordered=False,
    )
    out["einkommen_vor_steuer_hh_prev"] = object_to_int(raw_data["i11101"])
    out["einkommen_nach_steuer_hh_prev"] = object_to_int(raw_data["i11102"])
    out["labor_earnings_prev"] = object_to_int(raw_data["i11110"])
    out["bundesland"] = object_to_str_categorical(raw_data["l11101"], ordered=False)
    out["earnings_job_1_prev"] = object_to_int(raw_data["ijob1"])
    out["earnings_job_2_prev"] = object_to_int(raw_data["ijob2"])
    out["self_empl_earnings_prev"] = object_to_int(raw_data["iself"]).fillna(0)
    out["arbeitsl_geld_soep_prev"] = object_to_int(raw_data["iunby"])
    out["arbeitsl_hilfe_soep_prev"] = object_to_int(raw_data["iunay"])
    out["unterhaltsgeld_soep_prev"] = object_to_int(raw_data["isuby"])
    out["uebergangsgeld_soep_prev"] = object_to_int(raw_data["ieret"])
    out["mutterschaftsgeld_soep_prev"] = object_to_int(raw_data["imaty"])
    out["student_grants_prev"] = object_to_int(raw_data["istuy"])
    out["alimony_prev"] = object_to_int(raw_data["ialim"])
    out["other_transfers_prev"] = object_to_int(raw_data["ielse"])
    out["christmas_bonus_prev"] = object_to_int(raw_data["ixmas"])
    out["vacation_bonus_prev"] = object_to_int(raw_data["iholy"])
    out["profit_share_prev"] = object_to_int(raw_data["igray"])
    out["other_bonuses_prev"] = object_to_int(raw_data["iothy"])
    out["rental_income_y_hh_prev"] = object_to_int(raw_data["renty"])
    out["maintenance_c_hh_prev"] = object_to_int(raw_data["opery"])
    out["capital_income_hh_prev"] = object_to_int(raw_data["divdy"])
    out["kindergeld_pequiv_hh_prev"] = object_to_int(raw_data["chspt"])
    out["wohngeld_soep_hh_prev"] = object_to_int(raw_data["house"])
    out["pflegegeld_hh_prev"] = object_to_int(raw_data["nursh"])
    out["soc_assist_hh_prev"] = object_to_int(raw_data["subst"])
    out["soc_assist_spec_hh_prev"] = object_to_int(raw_data["sphlp"])
    out["housing_sup_hh_prev"] = object_to_int(raw_data["hsup"])

    out["gov_ret_pay_prev"] = object_to_int(raw_data["igrv1"])
    out["soc_miners_pension_prev"] = object_to_int(raw_data["ismp1"])
    out["civ_serv_pension_prev"] = object_to_int(raw_data["iciv1"])
    out["warvictim_pension_prev"] = object_to_int(raw_data["iwar1"])
    out["farmer_pension_prev"] = object_to_int(raw_data["iagr1"])
    out["stat_accident_insurance_prev"] = object_to_int(raw_data["iguv1"])
    out["civ_supp_benefits_prev"] = object_to_int(raw_data["ivbl1"])
    out["company_pen_prev"] = object_to_int(raw_data["icom1"])
    out["private_pen_prev"] = object_to_int(raw_data["iprv1"])
    out["other_pen_prev"] = object_to_int(raw_data["ison1"])
    out["riester_prev"] = object_to_int(raw_data["irie1"])

    out["gov_ret_pay_widow_prev"] = object_to_int(raw_data["igrv2"])
    out["soc_miners_pension_widow_prev"] = object_to_int(raw_data["ismp2"])
    out["civ_serv_pension_widow_prev"] = object_to_int(raw_data["iciv2"])
    out["warvictim_pension_widow_prev"] = object_to_int(raw_data["iwar2"])
    out["farmer_pension_widow_prev"] = object_to_int(raw_data["iagr2"])
    out["stat_accident_insur_widow_prev"] = object_to_int(raw_data["iguv2"])
    out["civ_supp_benefits_widow_prev"] = object_to_int(raw_data["ivbl2"])
    out["company_pen_widow_prev"] = object_to_int(raw_data["icom2"])
    out["other_pen_widow_prev"] = object_to_int(raw_data["ison2"])
    out["private_pen_widow_prev"] = object_to_int(raw_data["iprv2"])
    out["riester_widow_prev"] = object_to_int(raw_data["irie2"])

    out["med_pe_krnkhaus"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11101"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schlaganf"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11105"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_bluthdrck"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11106"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_diabetes"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11107"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_krebs"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11108"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_psych"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11109"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_gelenk"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11110"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_herzkr"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11111"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pe_schw_treppen"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11113"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_anziehen"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11115"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_bett"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11116"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_einkauf"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11117"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pe_schw_hausarb"] = _create_med_var_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11119"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )

    out["med_pe_groesse"] = object_to_int(raw_data["m11122"])
    out["med_pe_gewicht"] = object_to_int(raw_data["m11123"])
    out["med_pe_zufrieden"] = object_to_int_categorical(
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

    out["med_pe_subj_status"] = _create_med_pe_subj_status_column(
        pid_sr=raw_data["pid"],
        var_sr=raw_data["m11126"],
        renaming={
            "[1] Very good": 1,
            "[2] Good": 2,
            "[3] Satisfactory": 3,
            "[4] Poor": 4,
            "[5] Bad": 5,
        },
        ordered=True,
    )
    out["soc_assist_eld_hh_prev"] = object_to_int(raw_data["ssold"])
    out["arbeitsl_geld_2_soep_hh_prev"] = object_to_int(raw_data["alg2"])
    out["kinderzuschlag_pequiv_hh_prev"] = object_to_int(raw_data["adchb"])
    out["adv_child_maint_payment_prev"] = object_to_int(raw_data["iachm"])
    out["childcare_subsidy_hh_prev"] = object_to_int(raw_data["chsub"])
    out["caregiver_alimony_prev"] = object_to_int(raw_data["ichsu"])
    out["divorce_alimony_prev"] = object_to_int(raw_data["ispou"])
    return out
