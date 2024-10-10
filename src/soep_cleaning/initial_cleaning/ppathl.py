from soep_cleaning.config import pd
from soep_cleaning.initial_cleaning import month_mapping
from soep_cleaning.utilities import (
    apply_lowest_float_dtype,
    apply_lowest_int_dtype,
    categorical_to_int_categorical,
    int_categorical_to_int,
    str_categorical,
)


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the ppathl dataset."""
    out = pd.DataFrame()
    out["soep_hh_id"] = int_categorical_to_int(raw["hid"])
    out["p_id"] = apply_lowest_int_dtype(raw["pid"])
    out["year"] = apply_lowest_int_dtype(raw["syear"])
    out["current_east_west"] = str_categorical(
        raw["sampreg"],
        renaming={
            "[1] Westdeutschland, alte Bundeslaender": "Westdeutschland (alte Bundeslaender)",
            "[2] Ostdeutschland, neue Bundeslaender": "Ostdeutschland (neue Bundeslaender)",
        },
    )
    out["befragungsstatus"] = str_categorical(raw["netto"])
    out["year_immigration"] = int_categorical_to_int(raw["immiyear"])
    out["born_in_germany"] = str_categorical(raw["germborn"])
    out["country_of_birth"] = str_categorical(raw["corigin"])
    out["birth_month_from_ppathl"] = categorical_to_int_categorical(
        raw["gebmonat"],
        ordered=False,
        renaming=month_mapping.de,
    )
    out["east_west_1989"] = str_categorical(raw["loc1989"])
    out["migrationshintergrund"] = str_categorical(raw["migback"])
    out["sexual_orientation"] = str_categorical(raw["sexor"])
    out["birth_bundesland"] = str_categorical(raw["birthregion"])
    out["p_bleibe_wkeit"] = apply_lowest_float_dtype(raw["pbleib"])
    out["p_gewicht"] = apply_lowest_float_dtype(raw["phrf"])
    out["p_gewicht_nur_neue"] = apply_lowest_float_dtype(raw["phrf0"])
    out["p_gewicht_ohne_neue"] = apply_lowest_float_dtype(raw["phrf1"])
    out["pointer_partner"] = int_categorical_to_int(raw["parid"])
    out["has_partner"] = str_categorical(raw["partner"])

    return out
