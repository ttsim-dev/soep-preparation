import pandas as pd
from pathlib import Path

from data_management_von_gaudecker.config import SRC, BLD
from data_management_von_gaudecker.data_helper.data_loader import dta_loader


def clean_biobirth(raw_path: Path) -> pd.DataFrame:
    """Clean the biobirth dataset."""
    raw_data = dta_loader(raw_path)
    out = pd.DataFrame()
    out["hh_id_orig"] = raw_data["cid"]
    return out