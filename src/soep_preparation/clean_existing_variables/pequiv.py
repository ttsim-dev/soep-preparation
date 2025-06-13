"""Clean and convert SOEP pequiv variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    create_dummy,
    object_to_bool_categorical,
    object_to_int,
    object_to_int_categorical,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the pequiv file.

    Args:
        raw_data: The raw pequiv data.

    Returns:
        The processed pequiv data.
    """
    out = pd.DataFrame()

    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["hh_id_original"] = apply_lowest_int_dtype(raw_data["cid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])

    # hh characteristics
    out["number_of_persons_hh"] = apply_lowest_int_dtype(raw_data["d11106"])
    out["number_of_children_hh"] = apply_lowest_int_dtype(raw_data["d11107"])
    # hh income
    out["einkommen_vor_steuer_hh_betrag_y"] = object_to_int(raw_data["i11101"])
    out["einkommen_nach_steuer_hh_betrag_y"] = object_to_int(raw_data["i11102"])
    out["rental_income_hh_amount_y"] = object_to_int(raw_data["renty"])
    out["capital_income_hh_amount_y"] = object_to_int(raw_data["divdy"])
    # hh social benefits
    out["grundsicherung_im_alter_hh_betrag_y"] = object_to_int(raw_data["ssold"])
    out["alg2_pequiv_hh_betrag_y"] = object_to_int(raw_data["alg2"])
    out["kindergeld_pequiv_hh_betrag_y"] = object_to_int(raw_data["chspt"])
    out["kinderzuschlag_pequiv_hh_betrag_y"] = object_to_int(raw_data["adchb"])
    out["childcare_subsidy_hh_amount_y"] = object_to_int(raw_data["chsub"])
    out["wohngeld_pequiv_hh_betrag_y"] = object_to_int(raw_data["house"])
    out["pflegegeld_hh_betrag_y"] = object_to_int(raw_data["nursh"])
    out["social_assistance_hh_amount_y"] = object_to_int(raw_data["subst"])
    out["social_assistance_special_hh_amount_y"] = object_to_int(raw_data["sphlp"])
    out["owned_housing_support_hh_amount_y"] = object_to_int(raw_data["hsup"])
    # hh costs
    out["maintenance_costs_hh_amount_y"] = object_to_int(raw_data["opery"])

    # individual characteristics
    out["gender"] = object_to_str_categorical(
        raw_data["d11102ll"],
        ordered=False,
        renaming={"[1] Male": "male", "[2] Female": "female"},
    )
    out["age"] = object_to_int(raw_data["d11101"])
    out["federal_state_of_residence"] = object_to_str_categorical(
        raw_data["l11101"], ordered=False
    )
    out["employment_status_dummy"] = create_dummy(
        raw_data["e11102"], true_value="[1] Employed", kind="equal"
    )
    out["employment_level"] = object_to_str_categorical(
        raw_data["e11103"],
        ordered=False,
    )
    # individual medical characteristics
    out["med_pequiv_krnkhaus"] = object_to_bool_categorical(
        raw_data["m11101"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pequiv_schlaganfall"] = object_to_bool_categorical(
        raw_data["m11105"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pequiv_bluthochdruck"] = object_to_bool_categorical(
        raw_data["m11106"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pequiv_diabetes"] = object_to_bool_categorical(
        raw_data["m11107"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pequiv_krebs"] = object_to_bool_categorical(
        raw_data["m11108"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pequiv_psych"] = object_to_bool_categorical(
        raw_data["m11109"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pequiv_gelenk"] = object_to_bool_categorical(
        raw_data["m11110"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pequiv_herzkrankheit"] = object_to_bool_categorical(
        raw_data["m11111"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_pequiv_schwierigkeiten_treppen"] = object_to_bool_categorical(
        raw_data["m11113"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_pequiv_schwierigkeiten_anziehen"] = object_to_bool_categorical(
        raw_data["m11115"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_schwierigkeiten_bett"] = object_to_bool_categorical(
        raw_data["m11116"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_schwierigkeiten_einkauf"] = object_to_bool_categorical(
        raw_data["m11117"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_schwierigkeiten_hausarb"] = object_to_bool_categorical(
        raw_data["m11119"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )

    out["med_pequiv_groesse"] = object_to_int(raw_data["m11122"])
    out["med_pequiv_gewicht"] = object_to_int(raw_data["m11123"])
    out["med_pequiv_zufrieden"] = object_to_int_categorical(
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

    out["med_pequiv_subjective_status"] = object_to_int_categorical(
        raw_data["m11126"],
        renaming={
            "[1] Very good": 1,
            "[2] Good": 2,
            "[3] Satisfactory": 3,
            "[4] Poor": 4,
            "[5] Bad": 5,
        },
        ordered=True,
    )
    out["hours_worked_annually"] = object_to_int(raw_data["e11101"])
    # individual income
    out["einkuenfte_aus_arbeit_betrag_y"] = object_to_int(raw_data["i11110"])
    out["einkuenfte_aus_erster_arbeit_betrag_y"] = object_to_int(raw_data["ijob1"])
    out["einkuenfte_aus_zweiter_arbeit_betrag_y"] = object_to_int(raw_data["ijob2"])
    out["einkuenfte_aus_selbststaendiger_arbeit_betrag_y"] = object_to_int(
        raw_data["iself"]
    ).fillna(0)
    out["christmas_bonus_amount_y"] = object_to_int(raw_data["ixmas"])
    out["vacation_bonus_amount_y"] = object_to_int(raw_data["iholy"])
    out["profit_share_amount_y"] = object_to_int(raw_data["igray"])
    out["other_bonuses_amount_y"] = object_to_int(raw_data["iothy"])
    # individual social benefits
    out["alg2_betrag_y"] = object_to_int(raw_data["iunby"])
    out["arbeitslosen_hilfe_betrag_y"] = object_to_int(raw_data["iunay"])
    out["unterhalt_empfangen_betrag_y"] = object_to_int(raw_data["isuby"])
    out["uebergangsgeld_empfangen_betrag_y"] = object_to_int(raw_data["ieret"])
    out["maternity_benefit_amount_y"] = object_to_int(raw_data["imaty"])
    out["student_grants_amount_y"] = object_to_int(raw_data["istuy"])
    out["private_transfers_received_amount_y"] = object_to_int(raw_data["ielse"])
    out["alimony_received_amount_y"] = object_to_int(raw_data["ialim"])
    out["caregiver_alimony_received_amount_y"] = object_to_int(raw_data["ichsu"])
    out["divorce_alimony_received_amount_y"] = object_to_int(raw_data["ispou"])
    out["adv_child_maint_received_payment_amount_y"] = object_to_int(raw_data["iachm"])

    out["gesetzliche_rente_betrag_y"] = object_to_int(raw_data["igrv1"])
    out["social_miners_pension_amount_y"] = object_to_int(raw_data["ismp1"])
    out["civil_servant_pension_amount_y"] = object_to_int(raw_data["iciv1"])
    out["civil_servant_supplementary_benefits_amount_y"] = object_to_int(
        raw_data["ivbl1"]
    )
    out["warvictim_pension_amount_y"] = object_to_int(raw_data["iwar1"])
    out["farmer_pension_amount_y"] = object_to_int(raw_data["iagr1"])
    out["gesetzliche_unfallversicherung_rente_betrag_y"] = object_to_int(
        raw_data["iguv1"]
    )
    out["company_pension_amount_y"] = object_to_int(raw_data["icom1"])
    out["private_pension_amount_y"] = object_to_int(raw_data["iprv1"])
    out["other_pension_amount_y"] = object_to_int(raw_data["ison1"])
    out["riester_pension_amount_y"] = object_to_int(raw_data["irie1"])

    out["gesetzliche_hinterbliebenen_rente_betrag_y"] = object_to_int(raw_data["igrv2"])
    out["social_miners_relatives_pension_amount_y"] = object_to_int(raw_data["ismp2"])
    out["civil_servant_relatives_pension_amount_y"] = object_to_int(raw_data["iciv2"])
    out["civil_servant_supplementary_relatives_benefits_amount_y"] = object_to_int(
        raw_data["ivbl2"]
    )
    out["warvictim_relatives_pension_amount_y"] = object_to_int(raw_data["iwar2"])
    out["farmer_relatives_pension_amount_y"] = object_to_int(raw_data["iagr2"])
    out["gesetzliche_unfallversicherung_hinterbliebenen_rente_betrag_y"] = (
        object_to_int(raw_data["iguv2"])
    )
    out["company_relatives_pension_amount_y"] = object_to_int(raw_data["icom2"])
    out["other_relatives_pension_amount_y"] = object_to_int(raw_data["ison2"])
    out["private_relatives_pension_amount_y"] = object_to_int(raw_data["iprv2"])
    out["riester_relatives_pension_amount_y"] = object_to_int(raw_data["irie2"])
    return out
