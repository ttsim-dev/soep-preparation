from typing import Annotated

import pandas as pd
import pytask

from soep_preparation.config import DATA_CATALOGS
from soep_preparation.manipulate_dataset.create import (
    create_personal_variables,
    fill_pgen_for_children,
)


def _prepare_long_dataset(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out_personal_vars = create_personal_variables(out)
    out_children = fill_pgen_for_children(out_personal_vars)
    # out_complete = create_
    return out_children


@pytask.mark.skip()
def task_prepare_long_dataset(
    data: Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["example_merged_dataset"]],
) -> Annotated[
    pd.DataFrame, DATA_CATALOGS["merged"]["modified_example_merged_dataset"]
]:
    """Prepare long dataset by imputations and variable creation.

    Args:
        data: The merged dataset.

    Returns:
        The prepared long dataset.
    """
    # I don't like how the code is written here.

    # Drop columns that only exist before 2007
    # out = data.query("survey_year >= 2007").copy()
    # out = out.drop(
    #     columns=[
    #         "altersteilzeit_2001",
    #         "letzte_stelle_betriebsstilll",
    #         "letzte_stelle_grund_1999",
    #         "hours_repairs_sun",
    #         "teilnahmebereitschaft",
    #         "full_empl_v1_prev",
    #     ]
    # )
    return _prepare_long_dataset(data)
