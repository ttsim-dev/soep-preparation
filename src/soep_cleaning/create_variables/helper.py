import re

import numpy as np
from soep_cleaning.config import pd


def _fail_if_series_wrong_dtype(sr: pd.Series, expected_dtype: str):
    if expected_dtype not in sr.dtype.name:
        msg = f"Expected dtype {expected_dtype}, got {sr.dtype.name}"
        raise TypeError(msg)


def _fail_series_without_categories(sr: pd.Series):
    if sr.cat.categories is None:
        msg = "Series has no categories"
        raise ValueError(msg)


def _fail_if_invalid_input(input_, expected_dtype: str):
    if expected_dtype not in str(type(input_)):
        msg = f"Expected {input_} to be of type {expected_dtype}, got {type(input_)}"
        raise TypeError(
            msg,
        )


def _fail_if_invalid_inputs(input_, expected_dtypes: str):
    if " | " in expected_dtypes:
        if not any(
            expected_dtype in str(type(input_))
            for expected_dtype in expected_dtypes.split(" | ")
        ):
            msg = (
                f"Expected {input_} to be of type {expected_dtypes}, got {type(input_)}"
            )
            raise TypeError(
                msg,
            )

    else:
        _fail_if_invalid_input(input_, expected_dtypes)


def _error_handling_inputs(
    input_expected_types: list[list] | None = None,
):
    if input_expected_types is None:
        input_expected_types = [[]]
    [_fail_if_invalid_inputs(*item) for item in input_expected_types]


def create_dummy(
    sr: pd.Series,
    true_value: str | list,
    kind: str = "equality",
) -> pd.Series:
    _error_handling_inputs(
        [
            [sr, "pandas.core.series.Series"],
            [true_value, "str | list"],
            [kind, "str"],
        ],
    )
    if kind == "equality":
        bool_sr = sr.dropna() == true_value
    elif kind == "neq":
        bool_sr = sr.dropna() != true_value
    elif kind == "isin":
        bool_sr = sr.dropna().isin(true_value)
    else:
        msg = f"Unknown kind '{kind}' of dummy creation"
        raise ValueError(msg)
    return bool_sr.astype("bool[pyarrow]")


def create_in_education_dummy_categorical(
    employment: pd.Series,
    occupation: pd.Series,
) -> pd.Series:
    in_education = [
        "in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
        "in Ausbildung,             inkl. Weiterbildung, Berufsausbildung, Lehre",
        "Auszubildende (bis 1999)",
        "Auszubildende, gewerblich-technisch",
        "Auszubildende, gewerblich-technisch (ab 2000)",
        "Auszubildende, kaufmaennisch",
        "Volontäre, Praktikanten",
        "Aspiranten",
        "NE: in Ausbildung, inkl. Weiterbildung, Berufsausbildung, Lehre",
    ]
    out = create_dummy(employment, "Ausbildung, Lehre")
    # Don't use employment status to determine in_education if not working
    out.loc[employment == "Nicht erwerbstätig"] = pd.NA
    return out.fillna(create_dummy(occupation, in_education, "isin"))


def create_selfemployed_occupations(occupation: pd.Series) -> list:
    list_of_occ_names = list(occupation.dropna().unique())
    occs_of_interest = re.compile("^.*Freiberufler.*$|^.*selbstä.*$")
    return list(filter(occs_of_interest.match, list_of_occ_names))


def generate_education_variable(casmin: pd.Series, isced: pd.Series, mappings: dict):
    # Generate qualification variables from casmin and isced education variables
    # use casmin in education_mapping to create education
    out = pd.DataFrame()
    out["education"] = casmin.dropna().map(mappings.casmin)

    # fill missing values from isced in education_mapping
    out.loc[out["education"].isnull(), "education"] = isced.dropna().map(mappings.isced)
    val_quali = [
        "primary_and_lower_secondary",
        "upper_secondary",
        "tertiary",
    ]
    cat_type = pd.CategoricalDtype(categories=val_quali, ordered=True)
    return out["education"].astype(cat_type)


def manipulate_mschaftsgeld_monate(
    mschaftsgeld_monate: pd.Series,
    mschaftsgeld_bezogen: pd.Series,
) -> pd.Series:
    # TODO: discuss this logic of mschaftsgeld_monate
    _error_handling_inputs(
        [
            [mschaftsgeld_monate, "pandas.core.series.Series"],
            [mschaftsgeld_bezogen, "pandas.core.series.Series"],
        ],
    )
    mschaftsgeld_monate_less_four = np.maximum((mschaftsgeld_monate.cat.codes - 3), 0)
    mschaftsgeld_monate_less_four.loc[mschaftsgeld_bezogen == 0] = 0
    return mschaftsgeld_monate_less_four


def generate_employment_status(
    data: pd.DataFrame,
    m: int,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    # full_empl_prev_ manipulation
    data.loc[(data["year"] < 1997), f"full_empl_prev_{m}"] = data[
        f"full_empl_v1_prev_{m}"
    ]
    data.loc[(data["year"] >= 1997), f"full_empl_prev_{m}"] = data[
        f"full_empl_v2_prev_{m}"
    ]

    # half_empl_prev_ manipulation
    data[f"half_empl_b_prev_{m}"] = data[f"half_empl_prev_{m}"].cat.codes.between(0, 1)

    # employed_m_prev_ manipulation
    data.loc[
        (
            (data[f"full_empl_prev_{m}"] == 1)
            | (data[f"half_empl_b_prev_{m}"])
            | (data[f"mini_job_prev_{m}"] == 1)
        ),
        f"employed_m_prev_{m}",
    ] = 1
    return (
        data[f"full_empl_prev_{m}"],
        data[f"half_empl_prev_{m}"],
        data[f"employed_m_prev_{m}"],
    )
