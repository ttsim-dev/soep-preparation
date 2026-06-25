"""Clean and convert SOEP pequiv variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.data_manipulator import (
    apply_smallest_int_dtype,
    convert_to_float,
    create_dummy,
    object_to_bool_categorical,
    object_to_float,
    object_to_int,
    object_to_int_categorical,
    object_to_str_categorical,
    replace_not_applicable_answer,
)


def _calculate_frailty(frailty_inputs: pd.DataFrame) -> pd.Series:
    return convert_to_float(frailty_inputs.mean(axis=1))


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned variables from the pequiv module.

    Args:
        raw_data: The raw pequiv data.

    Returns:
        The processed pequiv data.
    """
    out = pd.DataFrame()

    out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
    out["p_id"] = apply_smallest_int_dtype(raw_data["pid"])
    out["hh_id_original"] = apply_smallest_int_dtype(raw_data["cid"])
    out["survey_year"] = apply_smallest_int_dtype(raw_data["syear"])

    # Consumer Price Index (CNEF y11101), for deflating nominal values to real
    # terms. A year-level series; constant within each survey year.
    out["cpi"] = convert_to_float(object_to_float(raw_data["y11101"]))

    # hh characteristics
    out["number_of_persons_hh"] = apply_smallest_int_dtype(raw_data["d11106"])
    out["number_of_children_living_in_hh"] = apply_smallest_int_dtype(
        raw_data["d11107"]
    )
    # hh income
    out["income_before_tax_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["i11101"], value=0)
    )
    out["income_after_tax_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["i11102"], value=0)
    )
    out["income_from_rental_leasing_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["renty"], value=0)
    )
    out["income_from_interest_dividends_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["divdy"], value=0)
    )

    # individual characteristics
    out["gender"] = object_to_str_categorical(
        series=raw_data["d11102ll"],
        ordered=False,
        renaming={"[1] Male": "Male", "[2] Female": "Female"},
    )
    out["age"] = object_to_int(raw_data["d11101"])
    out["federal_state_of_residence"] = object_to_str_categorical(
        series=raw_data["l11101"], ordered=False
    )

    # hh social benefits
    out["kindergeld_y_hh_pequiv"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["chspt"], value=0)
    )
    out["kindergeld_m_hh_pequiv"] = convert_to_float(out["kindergeld_y_hh_pequiv"] / 12)
    out["mutterschaftsgeld_received_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["imaty"], value=0)
    )
    # betreuungsgeld only available 2014 through 2016
    out["betreuungsgeld_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["chsub"], value=0)
    )

    out["kinderzuschlag_y_hh_pequiv"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["adchb"], value=0)
    )
    out["kinderzuschlag_m_hh_pequiv"] = convert_to_float(
        out["kinderzuschlag_y_hh_pequiv"] / 12
    )
    out["wohngeld_y_hh_pequiv"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["house"], value=0)
    )
    out["wohngeld_m_hh_pequiv"] = convert_to_float(out["wohngeld_y_hh_pequiv"] / 12)

    # individual social benefits
    out["arbeitslosengeld_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iunby"], value=0)
    )
    # arbeitslosenhilfe available 1984 through 2005
    out["arbeitslosenhilfe_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iunay"], value=0)
    )
    out["arbeitslosengeld_2_y_hh_pequiv"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["alg2"], value=0)
    )
    out["arbeitslosengeld_2_m_hh_pequiv"] = convert_to_float(
        out["arbeitslosengeld_2_y_hh_pequiv"] / 12
    )

    out["sozialhilfe_general_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["subst"], value=0)
    )
    # sonstige sozialhilfe available in 1984 through 1991 and 2001 through 2009
    out["sozialhilfe_other_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["sphlp"], value=0)
    )
    # grundsicherung only available 1984 through 2014
    out["grundsicherung_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["isuby"], value=0)
    )
    out["grundsicherung_im_alter_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ssold"], value=0)
    )
    out["pflegegeld_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["nursh"], value=0)
    )

    # eigenheimzulage only available 1996 through 2014
    out["eigenheimzulage_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["hsup"], value=0)
    )
    # private transfers contains
    # alimony in 1984 through 2000
    # divorce and caregiver alimonies in 1984 through 2014
    # unterhaltsvorschuss in 1984 through 2009
    out["private_transfers_received_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ielse"], value=0)
    )
    # alimony received only available 2001 through 2014
    out["unterhalt_received_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ialim"], value=0)
    )
    # caregiver alimony received available since 2015
    out["kindesunterhalt_received_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ichsu"], value=0)
    )
    out["kindesunterhalt_received_m_pequiv"] = out["kindesunterhalt_received_y"] / 12
    # divorce alimony only available in 2015
    out["ehegattenunterhalt_received_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ispou"], value=0)
    )
    # unterhaltsvorschuss available since 2010
    out["unterhaltsvorschuss_received_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iachm"], value=0)
    )
    out["bafög_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["istuy"], value=0)
    )

    # gesetzliche rente available since 1986
    # contains knappschaftliche rente and alterssicherung landwirte since 2002
    out["gesetzliche_rente_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["igrv1"], value=0)
    )
    out["gesetzliche_rente_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["igrv2"], value=0)
    )
    # knappschaftliche rente available 1986 through 2001
    out["knappschaftliche_rente_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ismp1"], value=0)
    )
    out["knappschaftliche_rente_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ismp2"], value=0)
    )
    # alterssicherung landwirte available 1986 through 2001
    out["alterssicherung_landwirte_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iagr1"], value=0)
    )
    out["alterssicherung_landwirte_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iagr2"], value=0)
    )
    # war victim pension available 1986 through 2001 and 2003 through 2016
    out["kriegsopferversorgung_rente_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iwar1"], value=0)
    )
    out["kriegsopferversorgung_rente_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iwar2"], value=0)
    )
    # beamtenpension available since 1986
    out["beamtenpension_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iciv1"], value=0)
    )
    out["beamtenpension_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iciv2"], value=0)
    )
    # beamten pension zusätzliche versorgung available since 1986
    out["beamtenpension_supplementary_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ivbl1"], value=0)
    )
    out["beamtenpension_supplementary_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ivbl2"], value=0)
    )
    # vorruhestandsgeld only available 1996 through 2001
    out["vorruhestandsgeld_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ieret"], value=0)
    )
    # betriebliche altersversorgung available since 1986
    out["betriebliche_altersversorgung_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["icom1"], value=0)
    )
    out["betriebliche_altersversorgung_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["icom2"], value=0)
    )
    # private altersvorsorge available since 2003
    out["private_altersvorsorge_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iprv1"], value=0)
    )
    out["private_altersvorsorge_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iprv2"], value=0)
    )
    # berufsständische rente available since 2018
    out["berufsständische_altersvorsorge_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ilib1"], value=0)
    )
    out["berufsständische_altersvorsorge_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ilib2"], value=0)
    )
    # riester rente available since 2015
    out["riester_rente_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["irie1"], value=0)
    )
    out["riester_rente_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["irie2"], value=0)
    )
    # gesetzliche unfallversicherung available since 1986
    out["gesetzliche_unfallversicherung_rente_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iguv1"], value=0)
    )
    out["gesetzliche_unfallversicherung_rente_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iguv2"], value=0)
    )
    # andere rente available since 1986;
    # changes its content because different kinds of private pensions
    # are asked for explicitly in different years.
    out["other_pension_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ison1"], value=0)
    )
    out["other_pension_survivor_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ison2"], value=0)
    )

    # individual income
    out["employed_y"] = create_dummy(
        series=raw_data["e11102"],
        value_for_comparison="[1] Employed",
        comparison_type="equal",
    )
    out["employment_level"] = object_to_str_categorical(
        series=raw_data["e11103"],
        ordered=False,
    )
    out["hours_worked_y"] = object_to_float(raw_data["e11101"])
    out["earnings_from_work_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["i11110"], value=0)
    )
    out["earnings_from_first_job_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ijob1"], value=0)
    )
    out["earnings_from_second_job_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ijob2"], value=0)
    )
    out["earnings_from_self_employment_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iself"], value=0)
    )
    out["christmas_bonus_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["ixmas"], value=0)
    )
    out["holiday_bonus_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iholy"], value=0)
    )
    out["profit_sharing_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["igray"], value=0)
    )
    out["other_bonuses_y"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["iothy"], value=0)
    )

    # hh costs
    out["operation_maintenance_costs_y_hh"] = object_to_float(
        replace_not_applicable_answer(series=raw_data["opery"], value=0)
    )

    # individual medical characteristics
    out["med_hospital_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11101"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_stroke_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11105"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_hypertension_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11106"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_diabetes_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11107"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_cancer_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11108"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_mental_illness_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11109"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_joint_disease_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11110"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_heart_disease_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11111"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_difficulty_stairs_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11113"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_difficulty_dressing_pequiv"] = object_to_bool_categorical(
        series=raw_data["m11115"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_difficulty_getting_out_of_bed"] = object_to_bool_categorical(
        series=raw_data["m11116"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_difficulty_shopping"] = object_to_bool_categorical(
        raw_data["m11117"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_difficulty_housework"] = object_to_bool_categorical(
        series=raw_data["m11119"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )

    out["med_height_pequiv"] = object_to_float(raw_data["m11122"])
    out["med_weight_pequiv"] = object_to_float(raw_data["m11123"])

    out["bmi_pequiv"] = convert_to_float(
        out["med_weight_pequiv"] / ((out["med_height_pequiv"] / 100) ** 2),
    )
    out["obese_pequiv"] = create_dummy(
        series=out["bmi_pequiv"], value_for_comparison=30, comparison_type="geq"
    )

    out["med_health_satisfaction_pequiv"] = object_to_int_categorical(
        series=raw_data["m11125"],
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

    out["med_subjective_status_pequiv"] = object_to_str_categorical(
        series=raw_data["m11126"],
        renaming={
            "[1] Very good": "Very good",
            "[2] Good": "Good",
            "[3] Satisfactory": "Satisfactory",
            "[4] Poor": "Poor",
            "[5] Bad": "Bad",
        },
        ordered=True,
    )
    frailty_inputs = out[
        [
            "med_difficulty_dressing_pequiv",
            "med_difficulty_getting_out_of_bed",
            "med_difficulty_shopping",
            "med_difficulty_housework",
            "med_difficulty_stairs_pequiv",
            "med_hospital_pequiv",
            "med_hypertension_pequiv",
            "med_diabetes_pequiv",
            "med_cancer_pequiv",
            "med_heart_disease_pequiv",
            "med_stroke_pequiv",
            "med_joint_disease_pequiv",
            "med_mental_illness_pequiv",
            "obese_pequiv",
        ]
    ].assign(
        med_subjective_status_dummy=create_dummy(
            series=out["med_subjective_status_pequiv"],
            value_for_comparison=["Satisfactory", "Poor", "Bad"],
            comparison_type="isin",
        ),
    )
    out["frailty_pequiv"] = _calculate_frailty(frailty_inputs=frailty_inputs)
    return out
