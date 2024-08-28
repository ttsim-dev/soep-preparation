from soep_cleaning.config import pd
from soep_cleaning.create_variables.helper import pequiv_manipulation, pgen_manipulation


def pequiv(data: pd.DataFrame) -> pd.DataFrame:
    return pequiv_manipulation(data)


def pgen(data: pd.DataFrame) -> pd.DataFrame:
    return pgen_manipulation(data)
