"""Clean and convert SOEP pequiv variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_smallest_int_dtype,
    create_dummy,
    object_to_bool_categorical,
    object_to_int,
    object_to_int_categorical,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the pequiv data file.

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

    # hh characteristics
    out["number_of_persons_hh"] = apply_smallest_int_dtype(raw_data["d11106"])
    out["number_of_children_hh"] = apply_smallest_int_dtype(raw_data["d11107"])

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

    # hh social benefits
    out["alg2_hh_betrag_y_pequiv"] = object_to_int(raw_data["alg2"])
    out["kindergeld_hh_betrag_y_pequiv"] = object_to_int(raw_data["chspt"])
    out["kinderzuschlag_hh_betrag_y_pequiv"] = object_to_int(raw_data["adchb"])
    # betreuungsgeld only available 2014 through 2016
    out["betreuungsgeld_hh_betrag_y"] = object_to_int(raw_data["chsub"])
    out["wohngeld_hh_betrag_y_pequiv"] = object_to_int(raw_data["house"])
    # eigenheimzulage only available 1996 through 2014
    out["eigenheimzulage_hh_betrag_y"] = object_to_int(raw_data["hsup"])
    out["allgemeine_sozialhilfe_hh_betrag_y"] = object_to_int(raw_data["subst"])
    out["grundsicherung_im_alter_hh_betrag_y"] = object_to_int(raw_data["ssold"])
    out["pflegegeld_hh_betrag_y"] = object_to_int(raw_data["nursh"])
    # sonstige sozialhilfe available in 1984 through 1991 and 2001 through 2009
    out["sonstige_sozialhilfe_hh_betrag_y"] = object_to_int(raw_data["sphlp"])

    # individual social benefits
    out["arbeitslosen_geld_betrag_y"] = object_to_int(raw_data["iunby"])
    # arbeitslosen hilfe only available 1984 through 2005
    out["arbeitslosen_hilfe_betrag_y"] = object_to_int(raw_data["iunay"])
    # sozialhilfe only available 1984 through 2014
    out["sozialhilfe_betrag_y"] = object_to_int(raw_data["isuby"])
    out["mutterschaftsgeld_betrag_y"] = object_to_int(raw_data["imaty"])
    # gesetzliche unfallversicherung available since 1986
    out["gesetzliche_unfallversicherung_rente_betrag_y"] = object_to_int(
        raw_data["iguv1"]
    )
    out["gesetzliche_unfallversicherung_hinterbliebenen_rente_betrag_y"] = (
        object_to_int(raw_data["iguv2"])
    )
    # war victim pension available 1986 through 2001 and 2003 through 2016
    out["war_victim_pension_amount_y"] = object_to_int(raw_data["iwar1"])
    out["warvictim_relatives_pension_amount_y"] = object_to_int(raw_data["iwar2"])
    # civil servant supplementary benefits available since 1986
    out["civil_servant_supplementary_benefits_amount_y"] = object_to_int(
        raw_data["ivbl1"]
    )
    out["civil_servant_supplementary_relatives_benefits_amount_y"] = object_to_int(
        raw_data["ivbl2"]
    )

    # private transfers contains
    # alimony in 1984 through 2000
    # divorce and caregiver alimonies in 1984 through 2014
    # unterhaltsvorschuss in 1984 through 2009
    out["private_transfers_received_amount_y"] = object_to_int(raw_data["ielse"])
    # alimony received only available 2001 through 2014
    out["alimony_received_amount_y"] = object_to_int(raw_data["ialim"])
    # caregiver alimony received available since 2015
    out["caregiver_alimony_received_amount_y"] = object_to_int(raw_data["ichsu"])
    # divorce alimony only available in 2015
    out["divorce_alimony_received_amount_y"] = object_to_int(raw_data["ispou"])
    # unterhaltsvorschuss available since 2010
    out["unterhaltsvorschuss_betrag_y"] = object_to_int(raw_data["iachm"])
    out["student_grants_amount_y"] = object_to_int(raw_data["istuy"])

    # gesetzliche rente available since 1986
    # contains knappschaftliche rente and alterssicherung landwirte since 2002
    out["gesetzliche_rente_empfangener_betrag_y"] = object_to_int(raw_data["igrv1"])
    out["gesetzliche_hinterbliebenen_rente_betrag_y"] = object_to_int(raw_data["igrv2"])
    # knappschaftliche rente available 1986 through 2001
    out["knappschaftliche_rente_empfangener_betrag_y"] = object_to_int(
        raw_data["ismp1"]
    )
    out["knappschaftliche_hinterbliebenen_rente_betrag_y"] = object_to_int(
        raw_data["ismp2"]
    )
    # alterssicherung landwirte available 1986 through 2001
    out["alterssicherung_landwirte_betrag_y"] = object_to_int(raw_data["iagr1"])
    out["alterssicherung_hinterbliebenen_landwirte_betrag_y"] = object_to_int(
        raw_data["iagr2"]
    )
    # beamten pension available since 1986
    out["beamten_pension_betrag_y"] = object_to_int(raw_data["iciv1"])
    out["beamten_hinterbliebenen_pension_betrag_y"] = object_to_int(raw_data["iciv2"])
    # vorruhestandsgeld only available 1996 through 2001
    out["vorruhestandgeld_betrag_y"] = object_to_int(raw_data["ieret"])
    # betriebliche altersversorgung available since 1986
    out["betriebliche_altersversorgung_betrag_y"] = object_to_int(raw_data["icom1"])
    out["betriebliche_hinterbliebenen_altersversorgung_betrag_y"] = object_to_int(
        raw_data["icom2"]
    )
    # private altersvorsorge available since 2003
    out["private_altersvorsorge_betrag_y"] = object_to_int(raw_data["iprv1"])
    out["private_hinterbliebenen_altersvorsorge_betrag_y"] = object_to_int(
        raw_data["iprv2"]
    )
    # other pension available since 1986
    out["other_pension_amount_y"] = object_to_int(raw_data["ison1"])
    out["other_relatives_pension_amount_y"] = object_to_int(raw_data["ison2"])
    # riester rente available since 2015
    out["riester_rente_betrag_y"] = object_to_int(raw_data["irie1"])
    out["riester_hinterbliebenen_rente_betrag_y"] = object_to_int(raw_data["irie2"])

    # hh income
    out["einkommen_vor_steuer_hh_betrag_y"] = object_to_int(raw_data["i11101"])
    out["einkommen_nach_steuer_hh_betrag_y"] = object_to_int(raw_data["i11102"])
    out["rental_income_hh_amount_y"] = object_to_int(raw_data["renty"])
    out["capital_income_hh_amount_y"] = object_to_int(raw_data["divdy"])

    # individual income
    out["employed_y"] = create_dummy(
        raw_data["e11102"], value_for_comparison="[1] Employed", comparison_type="equal"
    )
    out["employment_level"] = object_to_str_categorical(
        raw_data["e11103"],
        ordered=False,
    )
    out["hours_worked_y"] = object_to_int(raw_data["e11101"])
    out["einkünfte_aus_arbeit_betrag_y"] = object_to_int(raw_data["i11110"])
    out["einkünfte_aus_erster_arbeit_betrag_y"] = object_to_int(raw_data["ijob1"])
    out["einkünfte_aus_zweiter_arbeit_betrag_y"] = object_to_int(raw_data["ijob2"])
    out["einkünfte_aus_selbstständiger_arbeit_betrag_y"] = object_to_int(
        raw_data["iself"]
    ).fillna(0)
    out["christmas_bonus_amount_y"] = object_to_int(raw_data["ixmas"])
    out["vacation_bonus_amount_y"] = object_to_int(raw_data["iholy"])
    out["profit_share_amount_y"] = object_to_int(raw_data["igray"])
    out["other_bonuses_amount_y"] = object_to_int(raw_data["iothy"])

    # hh costs
    out["maintenance_costs_hh_amount_y"] = object_to_int(raw_data["opery"])

    # individual medical characteristics
    out["med_krankenhaus_pequiv"] = object_to_bool_categorical(
        raw_data["m11101"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_schlaganfall_pequiv"] = object_to_bool_categorical(
        raw_data["m11105"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_bluthochdruck_pequiv"] = object_to_bool_categorical(
        raw_data["m11106"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_diabetes_pequiv"] = object_to_bool_categorical(
        raw_data["m11107"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_krebs_pequiv"] = object_to_bool_categorical(
        raw_data["m11108"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_psych_pequiv"] = object_to_bool_categorical(
        raw_data["m11109"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_gelenk_pequiv"] = object_to_bool_categorical(
        raw_data["m11110"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_herzkrankheit_pequiv"] = object_to_bool_categorical(
        raw_data["m11111"],
        renaming={0: False, 1: True},
        ordered=True,
    )
    out["med_schwierigkeiten_treppen_pequiv"] = object_to_bool_categorical(
        raw_data["m11113"],
        renaming={"[0] Does not apply": False, "[1] Applies": True},
        ordered=True,
    )
    out["med_schwierigkeiten_anziehen_pequiv"] = object_to_bool_categorical(
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

    out["med_größe_pequiv"] = object_to_int(raw_data["m11122"])
    out["med_gewicht_pequiv"] = object_to_int(raw_data["m11123"])
    out["med_zufrieden_pequiv"] = object_to_int_categorical(
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

    out["med_subjective_status_pequiv"] = object_to_int_categorical(
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
    return out
