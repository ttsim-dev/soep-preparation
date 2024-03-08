import pandas as pd

import pytask
from pytask import task

from soep_cleaning.config import SRC, BLD
from soep_cleaning.dataset_merging.merge_datasets import merge_datasets

@pytask.mark.skip()
def task_merge_datasets(depends_on, produces, data_set_name):
    """
    Merge datasets.
    
    Args:
        depends_on (str): Path to the directory of long and cleaned data files.
        produces (str): Path to save the merged data file.
    """