from soep_preparation.config import pd
from soep_preparation.utilities import (
    apply_lowest_int_dtype,
    int_categorical_to_int,
    str_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the pbrutto dataset."""
    out = pd.DataFrame()
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["hh_id_orig"] = int_categorical_to_int(raw["cid"])
    out["hh_id"] = int_categorical_to_int(raw["hid"])
    out["year"] = int_categorical_to_int(raw["syear"])
    out["birth_year"] = int_categorical_to_int(raw["geburt_v2"])
    out["befragungs_status"] = str_categorical(raw["befstat_h"])
    out["hh_position"] = str_categorical(
        raw["stell_h"],
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
        reduce=True,
    )
    out["bearbeitungserg"] = str_categorical(raw["perg"])
    out["bearbeitungserg_ausf"] = str_categorical(
        raw["pergz"],
        reduce=True,
    )  # categories [29] and [39] have identical missing data labels which are reduced to one
    out["hh_position_raw_last_year"] = str_categorical(
        raw["pzugv"],
        reduce=True,
    )  # categories [19] and [39] have identical missing data labels which are reduced to one
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
    out["bearbeitungserg_old"] = str_categorical(raw["hergs"])
    return out
