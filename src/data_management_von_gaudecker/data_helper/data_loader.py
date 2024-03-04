from pathlib import Path
import pandas as pd

from data_management_von_gaudecker.config import SRC, BLD

def dta_loader(dir: Path) -> pd.DataFrame:
    """Load a .dta file."""
    return pd.read_stata(dir)