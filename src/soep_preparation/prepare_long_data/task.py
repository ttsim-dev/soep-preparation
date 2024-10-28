from pathlib import Path
from typing import Annotated

import pytask

from soep_preparation.config import DATA_CATALOGS, pd


@pytask.mark.skip()
def task_merge_datasets(
    data: Annotated[Path, DATA_CATALOGS["merged"]["merged_dataset"]],
) -> Annotated[pd.DataFrame, DATA_CATALOGS["merged"]["merged_cleaned"]]:
    return pd.DataFrame()
