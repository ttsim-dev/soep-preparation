import pandas as pd

from soep_preparation.config import DATA_CATALOGS

MERGE_KEYS = ["survey_year", "p_id", "hh_id", "hh_id_orig"]


def _get_clean_dataset(
    name: str,
    min_survey_year: int,
    max_survey_year: int,
) -> pd.DataFrame:
    datacatalog = DATA_CATALOGS["single_datasets"][name]
    df = (
        datacatalog["merged"].load()
        if "merged" in datacatalog._entries
        else datacatalog["cleaned"].load()
    )
    if "survey_year" in df.columns:
        return df.query(f"{min_survey_year} <= survey_year <= {max_survey_year}")
    return df


def _get_names_of_corresponding_datasets(
    var_to_datasets: dict[str, list[str]],
    variable_names: list[str],
) -> list[str]:
    selected_datasets = set()
    for variable in variable_names:
        if variable in var_to_datasets:
            datasets = var_to_datasets[variable]
            selected_datasets.update(datasets)
    return list(selected_datasets)


def _get_variable_to_names_map(
    name_to_df_mapping: dict[str, pd.DataFrame],
) -> dict[str, list[str]]:
    var_to_datasets = {}
    for name, df in name_to_df_mapping.items():
        for variable in df.columns:
            if variable not in var_to_datasets:
                var_to_datasets[variable] = []
            var_to_datasets[variable].append(name)
    return var_to_datasets


def _get_df_level(dataset: pd.DataFrame) -> str:
    if "p_id" in dataset.columns:
        if "survey_year" in dataset.columns:
            return "individual-time-varying"
        return "individual-time-constant"
    if "hh_id" in dataset.columns or "hh_id_orig" in dataset.columns:
        if "survey_year" in dataset.columns:
            return "household-time-varying"
        return "household-time-constant"
    msg = "Dataset does not contain a valid level."
    raise ValueError(msg)


def _get_shared_columns(left_df: pd.DataFrame, right_df: pd.DataFrame) -> list:
    shared_columns = [
        col
        for col in MERGE_KEYS
        if (col in left_df.columns) and (col in right_df.columns)
    ]
    if len(shared_columns) == 0:
        msg = "No shared columns specified or found to merge on."
        raise ValueError(msg)
    return shared_columns


def get_names_to_variables_by_level(
    name_to_df_mapping: dict[str, pd.DataFrame],
    variables: list[str],
) -> dict:
    variable_to_names_mapping = _get_variable_to_names_map(name_to_df_mapping)
    variables_not_merge_keys = [var for var in variables if var not in MERGE_KEYS]
    names = _get_names_of_corresponding_datasets(
        variable_to_names_mapping,
        variables_not_merge_keys,
    )

    out = {
        "individual-time-varying": {},
        "individual-time-constant": {},
        "household-time-varying": {},
        "household-time-constant": {},
    }
    for name in names:
        df = name_to_df_mapping[name]
        df_level = _get_df_level(df)
        out[df_level][name] = [
            var for var in variables_not_merge_keys if var in df.columns
        ]
    return out


def merge_data_by_level(
    name_to_df_mapping: dict[str, pd.DataFrame],
    name_to_variables_mapping: dict[str, list],
    how: str = "outer",
) -> pd.DataFrame:
    out = None
    for name, variables in name_to_variables_mapping.items():
        columns = [
            var for var in MERGE_KEYS if var in name_to_df_mapping[name].columns
        ] + variables
        if out is None:
            out = name_to_df_mapping[name][columns]
        else:
            shared_columns = _get_shared_columns(out, name_to_df_mapping[name])
            right = name_to_df_mapping[name][columns]
            out = out.merge(right, on=shared_columns, how=how)
    return out


def merge_data(
    datasets: list[pd.DataFrame],
    how: str = "outer",
) -> pd.DataFrame:
    out = None
    for df in datasets:
        if out is None:
            out = df
        elif df is not None:
            shared_columns = _get_shared_columns(out, df)
            out = out.merge(df, on=shared_columns, how=how)
    return out


def dataset_creation(
    variables: list[str],
    min_survey_year: int,
    max_survey_year: int,
    merging_behavior: str = "auto",
) -> pd.DataFrame:
    # TODO (@felixschmitz): add error handling
    # Check if the provided variable names are valid type
    # Check if the provided variable names are present in any dataset
    # Check if the provided min/max survey years are valid
    # Check if the provided merging behavior is supported

    # TODO (@hmvgaudecker): what do you think?
    # there are two variables with identical information, but potentially different values
    # - 'birth_month_from_bioedu', 'birth_month_from_ppathl'; previously NA values in the first were filled with the second, favoring this since the first one does not have a time component
    # - 'hh_soep_sample_from_design', 'hh_soep_sample_from_hpathl'; previously NA values in the first were filled with the second, same argument as above

    name_to_df_mapping = {
        name: _get_clean_dataset(name, min_survey_year, max_survey_year)
        for name in DATA_CATALOGS["single_datasets"]
    }

    names_to_variables_by_level = get_names_to_variables_by_level(
        name_to_df_mapping,
        variables,
    )

    # TODO (@hmvgaudecker): is this too much "blackbox"?
    time_varying_individual_data = merge_data_by_level(
        name_to_df_mapping,
        names_to_variables_by_level["individual-time-varying"],
    )
    time_constant_individual_data = merge_data_by_level(
        name_to_df_mapping,
        names_to_variables_by_level["individual-time-constant"],
    )
    time_varying_household_data = merge_data_by_level(
        name_to_df_mapping,
        names_to_variables_by_level["household-time-varying"],
    )
    time_constant_household_data = merge_data_by_level(
        name_to_df_mapping,
        names_to_variables_by_level["household-time-constant"],
    )

    # TODO (@felixschmitz)
    # Make sure that there are time-invariant variables in the dataset
    # Fill missing values (due to no observations in the survey year) with pd.NA
    return merge_data(
        [
            time_varying_individual_data,
            time_constant_individual_data,
            time_varying_household_data,
            time_constant_household_data,
        ],
    )


def task_access_variables():
    dataset_creation(
        variables=[
            "survey_year",
            "p_id",
            "hh_id_orig",
            "hh_id",
            "age",
            "bmi_pe",
            "hh_gewicht_nur_neue",
            "hh_position",
        ],
        min_survey_year=2018,
        max_survey_year=2020,
    )
