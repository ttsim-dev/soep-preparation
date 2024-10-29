import inspect
import re
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pandas.api.types import union_categoricals
from pandas.io.stata import StataReader
from pytask import task

from soep_preparation.config import (
    DATA,
    DATA_CATALOGS,
    SOEP_VERSION,
    SRC,
    pd,
)


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def _columns_for_dataset(dataset: Path) -> list[str]:
    module = SourceFileLoader(
        dataset.resolve().stem,
        str(dataset.resolve()),
    ).load_module()
    function_with_docstring = inspect.getsource(module.clean)
    # Remove the docstring, if existent.
    function_content = re.sub(
        r'""".*?"""|\'\'\'.*?\'\'\'',
        "",
        function_with_docstring,
        flags=re.DOTALL,
    )
    # Find all occurrences of raw["column_name"] or ['column_name'].
    pattern = r'raw\["([^"]+)"\]|\[\'([^\']+)\'\]'
    matches = [match[0] or match[1] for match in re.findall(pattern, function_content)]
    # Return unique matches in the order that they appear.
    return list(dict.fromkeys(matches))


def _extract_num_from_string(s: str):
    match = re.search(r"\[(-?\d+)\]", s)
    return int(match.group(1)) if match else float("inf")


def _create_categorical_column(out, chunk, col):
    # If the categories are not of the same type, we convert them to the same type.
    if out[col].cat.categories.dtype != chunk[col].cat.categories.dtype:
        # Converting the column's categories dtype in the chunk to object (the same as in out).
        if out[col].cat.categories.dtype.name == "object":
            chunk[col] = chunk[col].cat.set_categories(
                chunk[col].cat.categories.astype("object"),
            )
        else:
            # Converting the column's categories dtype of out to the same as of the chunk.
            out[col] = out[col].cat.set_categories(
                out[col].cat.categories.astype(
                    chunk[col].cat.categories.dtype,
                ),
            )
    # Union of the categories of the two columns.
    orig_categories_union = union_categoricals(
        [out[col], chunk[col]],
        ignore_order=True,
    ).categories.to_list()
    orig_numeric_categories = sorted(
        [x for x in orig_categories_union if isinstance(x, int | float)],
    )
    orig_string_categories = sorted(
        [x for x in orig_categories_union if isinstance(x, str)],
        key=_extract_num_from_string,
    )
    # First the string categories, afterwards the numerical.
    sorted_categories = orig_string_categories + orig_numeric_categories
    return (
        out[col].cat.set_categories(
            sorted_categories,
            ordered=True,
        ),
        chunk[col].cat.set_categories(
            sorted_categories,
            ordered=True,
        ),
    )


def _iteratively_read_one_dataset(itr: StataReader, columns: list[str]) -> pd.DataFrame:
    out = pd.DataFrame()
    value_labels = {k: v for k, v in itr.value_labels().items() if k in columns}
    for chunk in itr:
        chunk = chunk.replace(value_labels)
        chunk = chunk.astype("category")
        if out.empty:
            out = chunk
        else:
            col_dtypes = chunk.dtypes
            for col in columns:
                if col_dtypes[col].name == "category":
                    out[col], chunk[col] = _create_categorical_column(out, chunk, col)
                else:
                    out[col] = chunk[col]
            out = pd.concat([out, chunk])
    return out


for dataset in DATA_CATALOGS["single_datasets"].keys():

    @task(id=dataset)
    def task_pickle_one_dataset(
        orig_data: Annotated[Path, DATA / f"{SOEP_VERSION}" / f"{dataset}.dta"],
        cleaning_script: Annotated[
            Path,
            SRC / "initial_cleaning" / f"{dataset}.py",
        ],
    ) -> Annotated[
        pd.DataFrame, DATA_CATALOGS["single_datasets"][dataset][f"{dataset}_raw"]
    ]:
        """Saves the raw dataset to the data catalog in a more efficient procedure.

        Parameters:
            orig_data (Path): The path to the original dataset.
            dataset (str): The name of the dataset.

        Returns:
            pd.DataFrame: A pandas DataFrame to be saved to the data catalog.

        """
        _error_handling_task(orig_data, cleaning_script)
        columns = _columns_for_dataset(cleaning_script.resolve())
        with StataReader(
            orig_data,
            chunksize=100_000,
            columns=columns,
            convert_categoricals=False,
        ) as itr:
            return _iteratively_read_one_dataset(itr, columns)


def _error_handling_task(data, script_path):
    _fail_if_invalid_input(data, "pathlib.PosixPath")
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
