"""Clean and convert SOEP pbrutto variables to appropriate data types."""

import pandas as pd

from soep_preparation.utilities.series_manipulator import (
    apply_lowest_int_dtype,
    object_to_int,
    object_to_str_categorical,
)


def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned and sensible data type variables from the pbrutto file.

    Args:
        raw_data: The raw pbrutto data.

    Returns:
    The processed pbrutto data.
    """
    out = pd.DataFrame()

    out["p_id"] = apply_lowest_int_dtype(raw_data["pid"])
    out["hh_id_original"] = apply_lowest_int_dtype(raw_data["cid"])
    out["hh_id"] = apply_lowest_int_dtype(raw_data["hid"])
    out["survey_year"] = apply_lowest_int_dtype(raw_data["syear"])

    out["birth_year"] = object_to_int(raw_data["geburt_v2"])
    out["relationship_to_head_of_hh"] = object_to_str_categorical(
        raw_data["stell_h"],
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
            "[34] Pflegevater,-mutter": "Parent",
            "[35] Schwiegervater,-mutter,(Ehe-/LPartner v.Eltern)": "Parent",
            "[36] Grossvater,-mutter": "Other relative",
            "[40] Geschwister, Schwager/Schwaegerin [bis 2011]": "Other relative",
            "[41] Leibliche/r Bruder,Schwester": "Other relative",
            "[42] Halbbruder,-schwester": "Other relative",
            "[43] Stiefschwester,-bruder(von Elternteilen.verh/lpar)": "Other relative",
            "[44] Adoptivbruder,-schwester": "Other relative",
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
    )
    out["relationship_to_hh_last_year"] = object_to_str_categorical(raw_data["pzugv"])

    # interview related variables
    out["interview_status"] = object_to_str_categorical(raw_data["befstat_h"])
    out["interview_result_one_digit"] = object_to_str_categorical(raw_data["perg"])
    # categories [29] and [39] have identical missing data labels
    # they are reduced to one
    out["interview_result_two_digits"] = object_to_str_categorical(raw_data["pergz"])
    out["interview_result_old"] = object_to_str_categorical(raw_data["hergs"])
    # categories [19] and [39] have identical missing data labels
    # they are reduced to one
    out["willingness_to_participate"] = object_to_str_categorical(
        raw_data["ber"],
        renaming={
            "[4] sehr schlecht": "sehr schlecht",
            "[3] schlecht": "schlecht",
            "[2] gut": "gut",
            "[1] sehr gut": "sehr gut",
        },
        ordered=True,
    )
    return out
