"""Functions to store raw SOEP datasets."""

import inspect
import re
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

import pandas as pd
from pandas.api.types import union_categoricals
from pandas.io.stata import StataReader
from pytask import task

from soep_preparation.config import (
    DATA,
    DATA_CATALOGS,
    SOEP_VERSION,
    SRC,
)


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def _get_relevant_column_names(script: Path) -> list[str]:
    module = SourceFileLoader(
        script.resolve().stem,
        str(script.resolve()),
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
        # Converting the column's categories dtype
        # in the chunk to object (the same as in out).
        if out[col].cat.categories.dtype.name == "object":
            chunk[col] = chunk[col].cat.set_categories(
                chunk[col].cat.categories.astype("object"),
            )
        else:
            # Converting the column's categories dtype
            # of out to the same as of the chunk.
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


def _iteratively_read_one_dataset(
    iterator: StataReader,
    relevant_columns: list[str],
) -> pd.DataFrame:
    out = pd.DataFrame()
    value_labels = {
        k: v for k, v in iterator.value_labels().items() if k in relevant_columns
    }
    for chunk in iterator:
        value_chunk = chunk.replace(value_labels)
        raw_chunk = value_chunk.astype("category")
        if out.empty:
            out = raw_chunk
        else:
            col_dtypes = raw_chunk.dtypes
            for column in relevant_columns:
                out_chunk = pd.DataFrame(index=raw_chunk.index)
                if col_dtypes[column].name == "category":
                    out[column], out_chunk[column] = _create_categorical_column(
                        out,
                        raw_chunk,
                        column,
                    )
                else:
                    out_chunk[column] = chunk[column]
            out = pd.concat([out, chunk])
    return out


for name, catalog in DATA_CATALOGS["single_variables"].items():

    @task(id=name)
    def task_pickle_one_dataset(
        stata_data: Annotated[Path, DATA / f"{SOEP_VERSION}" / f"{name}.dta"],
        cleaning_script: Annotated[
            Path,
            SRC / "initial_cleaning" / f"{name}.py",
        ],
    ) -> Annotated[pd.DataFrame, catalog["raw"]]:
        """Saves the raw dataset to the data catalog.

        Parameters:
            stata_data (Path): The path to the original STATA dataset.
            cleaning_script (Path): The path to the respective cleaning script.

        Returns:
            pd.DataFrame: A pandas DataFrame to be saved to the data catalog.

        """
        _error_handling_task(stata_data, cleaning_script)
        relevant_columns = _get_relevant_column_names(cleaning_script)
        with StataReader(
            stata_data,
            chunksize=100_000,
            columns=relevant_columns,
            convert_categoricals=False,
        ) as stata_iterator:
            return _iteratively_read_one_dataset(stata_iterator, relevant_columns)


def _error_handling_task(data, script_path):
    _fail_if_invalid_input(data, "pathlib.PosixPath")
    _fail_if_invalid_input(script_path, "pathlib.PosixPath")
