import inspect
import re
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Annotated

from pandas.api.types import union_categoricals
from pandas.io.stata import StataReader
from pytask import task
from soep_cleaning.config import DATA, DATA_CATALOG, SOEP_VERSION, SRC, pd
from soep_cleaning.utilities import dataset_scripts


def _extract_num_from_string(s: str):
    match = re.search(r"\[(-?\d+)\]", s)
    return int(match.group(1)) if match else float("inf")


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
                    if out[col].cat.categories.dtype != chunk[col].cat.categories.dtype:
                        if out[col].cat.categories.dtype.name == "object":
                            chunk[col] = chunk[col].cat.set_categories(
                                chunk[col].cat.categories.astype("object"),
                            )
                        else:
                            out[col] = out[col].cat.set_categories(
                                out[col].cat.categories.astype(
                                    chunk[col].cat.categories.dtype,
                                ),
                            )
                    union_categorical = union_categoricals(
                        [out[col], chunk[col]],
                        ignore_order=True,
                    )
                    categories = union_categorical.categories
                    numeric_categories = sorted(
                        [x for x in categories if isinstance(x, int | float)],
                    )
                    string_categories = sorted(
                        [x for x in categories if isinstance(x, str)],
                        key=_extract_num_from_string,
                    )
                    sorted_categories = string_categories + numeric_categories
                    union_categorical = pd.Categorical(
                        categories.to_list(),
                        categories=sorted_categories,
                        ordered=True,
                    )
                    union_categories = union_categorical.set_categories(
                        sorted_categories,
                        ordered=True,
                    )
                    out[col] = out[col].cat.set_categories(
                        union_categories.categories,
                        ordered=True,
                    )
                    chunk[col] = chunk[col].cat.set_categories(
                        union_categories.categories,
                        ordered=True,
                    )
                else:
                    out[col] = chunk[col]
            out = pd.concat([out, chunk])
    return out


def _columns_for_dataset(dataset: Path) -> list:
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


for dataset in dataset_scripts(
    Path(
        SRC.joinpath(
            "initial_cleaning",
        ).resolve(),
    ),
):

    @task(id=dataset)
    def task_pickle_one_dataset(
        orig_data: Annotated[Path, DATA.joinpath(f"{SOEP_VERSION}/{dataset}.dta")],
        initial_cleaning: Annotated[
            Path,
            SRC.joinpath(
                "initial_cleaning",
                f"{dataset}.py",
            ),
        ],
    ) -> Annotated[pd.DataFrame, DATA_CATALOG["raw"][dataset]]:
        """Saves the raw dataset to the data catalog in a more efficient procedure.

        Parameters:
            orig_data (Path): The path to the original dataset.
            dataset (str): The name of the dataset.

        Returns:
            pd.DataFrame: A pandas DataFrame to be saved to the data catalog.

        """
        columns = _columns_for_dataset(initial_cleaning.resolve())
        with StataReader(
            orig_data,
            chunksize=100_000,
            columns=columns,
            convert_categoricals=False,
        ) as itr:
            return _iteratively_read_one_dataset(itr, columns)
