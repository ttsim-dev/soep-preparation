"""Module to merge the hh_soep_sample variable from hpathl and design datasets."""

import pandas as pd


def merge_variable(hpathl: pd.DataFrame, design: pd.DataFrame) -> pd.DataFrame:
    """Merge the hh_soep_sample variable from hpathl and design datasets.

    Args:
        hpathl (pd.DataFrame): DataFrame containing the cleaned hpathl dataset.
        design (pd.DataFrame): DataFrame containing the cleaned design dataset.

    Returns:
        pd.DataFrame: DataFrame containing the merged hh_soep_sample variable.
    """
    out = pd.DataFrame()
    out["hh_id"] = hpathl["hh_id"].unique()
    out["hh_soep_sample"] = (
        hpathl.groupby("hh_id")["hh_soep_sample_from_hpathl"]
        .first()
        .combine_first(design.groupby("hh_id")["hh_soep_sample_from_design"].first())
        .reset_index(drop=True)
    )
    return out
