import pandas as pd
from pathlib import Path

from data_management_von_gaudecker.config import SRC, BLD
from data_management_von_gaudecker.data_helper.data_loader import dta_loader

def _kid_var_name_mapping() -> dict:
    """Map the numerical variables."""
    return {
        "kidpnr": "p_id_child",
        "kidgeb": "birth_year_child",
        "kidmon": "birth_month_child",
    }


def clean_biobirth(raw_path: Path) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    raw_data = dta_loader(raw_path)
    out = pd.DataFrame()
    out["hh_id_orig"] = raw_data["cid"]
    for old_name, new_name in _kid_var_name_mapping().items():
        for id_ in range(1, 16):
            two_digit_id_ = "{:02}".format(id_)
            out[f"{new_name}_{two_digit_id_}"] = raw_data[f"{old_name}{two_digit_id_}"]
    return out