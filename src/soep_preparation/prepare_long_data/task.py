from typing import Annotated

from soep_preparation.config import DATA, DATA_CATALOGS, pd


def task_merge_datasets(
    data: Annotated[dict, DATA / "merged" / "merged_dataset"],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["cleaned"]["merged_cleaned"]]:
    return pd.DataFrame()
