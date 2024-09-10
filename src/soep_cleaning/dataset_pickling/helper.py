from soep_cleaning.config import pd


def iteratively_read_one_dataset(
    itr: pd.io.stata.StataReader,
    list_of_columns: list,
) -> pd.DataFrame:
    """Read a dataset in chunks."""
    value_labels_dict = {col: {} for col in list_of_columns}
    out = pd.DataFrame()
    for chunk in itr:
        for col in list_of_columns:
            value_labels = {}
            if col in itr.value_labels():
                value_labels = itr.value_labels()[col]
            unique_values = {
                value: value
                for value in chunk[col].unique()
                if value not in value_labels.values()
            }
            value_labels_dict[col] = (
                value_labels_dict[col] | value_labels | unique_values
            )

        out = pd.concat([out, chunk], ignore_index=True)
    cat_dtypes = {
        col: pd.CategoricalDtype(
            categories=list(dict(sorted(value_dict.items())).values()),
            ordered=True,
        )
        for col, value_dict in value_labels_dict.items()
    }
    return out.astype(cat_dtypes)
