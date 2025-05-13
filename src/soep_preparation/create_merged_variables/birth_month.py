"""Module to merge the birth_month variable from ppathl and bioedu datasets."""

import pandas as pd


def merge_variable(ppathl: pd.DataFrame, bioedu: pd.DataFrame) -> pd.DataFrame:
    """Merge the birth_month variable from ppathl and bioedu datasets.

    Args:
        ppathl (pd.DataFrame): DataFrame containing the cleaned ppathl dataset.
        bioedu (pd.DataFrame): DataFrame containing the cleaned bioedu dataset.

    Returns:
        pd.DataFrame: DataFrame containing the merged birth_month variable.
    """
    out = pd.DataFrame()
    out["p_id"] = ppathl["p_id"].unique()
    out["birth_month"] = (
        ppathl.groupby("p_id")["birth_month_from_ppathl"]
        .first()
        .combine_first(bioedu.groupby("p_id")["birth_month_from_bioedu"].first())
        .reset_index(drop=True)
    )
    return out
