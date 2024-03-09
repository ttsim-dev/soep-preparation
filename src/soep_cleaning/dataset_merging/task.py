import pytask


@pytask.mark.skip()
def task_merge_datasets(depends_on, produces, data_set_name):
    """Merge datasets.

    Args:
        depends_on (str): Path to the directory of long and cleaned data files.
        produces (str): Path to save the merged data file.

    """
