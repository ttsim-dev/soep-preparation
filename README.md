# SOEP data preparation for use with GETTSIM and in other projects

[![image](https://img.shields.io/github/actions/workflow/status/felixschmitz/soep_preparation/main.yml?branch=main)](https://github.com/felixschmitz/soep_preparation/actions?query=branch%3Amain)
[![image](https://codecov.io/gh/felixschmitz/soep_preparation/branch/main/graph/badge.svg)](https://codecov.io/gh/felixschmitz/soep_preparation)

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/felixschmitz/soep_preparation/main.svg)](https://results.pre-commit.ci/latest/github/felixschmitz/soep_preparation/main)
[![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Project Overview

This project processes the SOEP-Core data for use with
[GETTSIM](https://gettsim.readthedocs.io/en/stable/). The raw data is provided by the
German Institute for Economic Research (DIW Berlin) and is a panel dataset that follows
the same individuals over time. The data is collected annually and contains information
on various topics such as income, employment, and health. The data is available for
scientific use, for more information visit the
[Research Data Center SOEP](https://www.diw.de/en/diw_01.c.678568.en/research_data_center_soep.html).

The top-level directory is structured as follows:

- `src`: source code and raw data
- `tests`: tests of the source code
- `bld`: build directory with processed data (will be created automatically)
- other files include the environment configuration, pre-commit hooks, and some
  meta-files like this README

## Usage

To get started, install [pixi](https://prefix.dev/docs/pixi/overview#installation) if
you haven't already.

**_Inside the directory `soep_preparation/src/soep_preparation/data` place the folder
`V38` containing the raw `.dta` datafiles._**

To build the project, type

```console
$ pixi run pytask
```

## Working with the Data and Modules

The SOEP data is available in different waves, with the latest being wave 39.

### Understanding the SOEP-Core Data

To understand which variables are additionally available for a dataset, the URL
`https://paneldata.org/soep-core/datasets/{dataset_name}` might be helpful. Here you can
use the GUI to search for variable names.

If one wants to find out what the variable `hh_id_orig` contains, one searches in the
directory `src/soep_preparation` for the corresponding script and `raw_data` variable
name e.g. `biobirth.py`and `cid`. From here one can use the URL
`https://paneldata.org/soep-core/datasets/biobirth/cid` to get an understanding of the
variable. The "Codebook (PDF)" might be helpful in understanding. The URL takes hence
the general form:
`https://paneldata.org/soep-core/datasets/{dataset_name}/{variable_name}`

### Additional Variables from an Existing Dataset

If you want to include an additional variable from a dataset that is already being
cleaned, follow this approach:

Each new variable should be created by processing a column (or several columns) from the
raw data. The results of this processing will then be added to the final dataset that
the system builds.

Here’s how you can do that:

1. Identify the raw variable you want to transform or clean from your input data.

1. Use or create a function that transforms this raw variable into the final form you
   need.

1. Assign the result of that transformation to the out DataFrame, which represents your
   cleaned dataset.

Suppose you want to add a new variable, `age`, to your final dataset based on the `raw`
data. Here’s how the process would look:

```python
def clean(raw: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()

    # Example: Adding a variable 'age' after processing the 'birth_year' column
    out["age"] = calculate_age_from_birth_year(raw["birth_year"])

    return out
```

### Adding a New Dataset Module

To add a new SOEP-Core dataset to the project, follow these steps:

1. Add the Dataset to the Data Directory

   Each dataset should be placed in appropriate data directory (e.g., inside
   `soep_preparation/data/V38`). As an example, say you want to add the dataset
   `pequiv.dta` (nevermind this already exists).

1. Create a Corresponding Python Script

   For each new dataset, create a corresponding Python module (here: `pequiv.py`) inside
   the initial_preparation directory. Each module must include a clean function that
   takes a `pd.DataFrame` as input and returns the cleaned dataset, also as a
   `pd.DataFrame`.

   Example template for the clean function:

   ```python
   # to guarantee the correct pandas settings
   from soep_preparation.config import pd


   def clean(raw: pd.DataFrame) -> pd.DataFrame:
       """Clean the <dataset_name> dataset."""
       out = pd.DataFrame()

       # Apply cleaning steps to raw data
       out["hh_id_orig"] = cleaning_function(raw["cid"])

       return out
   ```

## Creating a Merged Panel Dataset

To create a merged dataset with columns from different pre-processed datasets, you can
inspect the example in the directory `src/soep_preparation/dataset_merging`. For merging
datasets, the columns and survey years of interest need to be specified. Other
components of the merging process are handled via the implemented helper functions. Do
not include any of the ID variables (`survey_year`, `hh_id`, `hh_orig_id`, `p_id`) in
the columns list, as these are automatically included.

## Further Structure Description

The `src/soep_preparation` directory contains the subdirectories `data`,
`dataset_merging` and `initial_preparation` and the python-scripts `config.py` and
`utilities.py`. **_Inside `data` place the folder `V38` containing all `.dta` files to
be cleaned and processed._**

The `initial_preparation` directory contains the scripts for the initial cleaning of the
datasets. Data cleaning follows the functional form introduced during the lecture and
creates a task for cleaning and transforming depending on each specified raw dataset.
For each group of datasets (bio, h, p and other), there is a `_specific_cleaner.py`
script with the actual implementation of the respective dataset. Further the `helper.py`
script contains functions to clean the different kinds of columns to be found inside the
raw data. The usual implementation of cleaning a column is:

```python
out["new_name"] = cleaning_function(raw["old_name"])
```

where `out` is the dataset created from the bottom up with the results from
`cleaning_function()`. The latter takes a `pd.Series` as argument (sometimes additional,
but optional inputs) and return the cleaned series as `pd.Series`. `raw` is the original
and uncleaned dataset currently being cleaned.

The `dataset_merging` directory contains the scripts for merging the datasets (to be
implemented).

The `config.py` specifies global constants and sets the options for modern pandas.
`utilities.py` contains general helper functions.

## Next steps

- Currently Work on `NaN` values conversion to `pd.NA` inside categorial columns (of
  numerical dtype); currently missing
- Some datasets contain categorical columns with identical category names (after
  cleaning/removing the response code), further discussion needed (e.g. dataset `perg`,
  column `pbrutto`)
- Should cleaning helpers handle empty series differently? (currently no error being
  raised and an empty `pd.Series` is being returned)
- Adaptation to different SOEP versions/waves

## Credits

This project was created with [cookiecutter](https://github.com/audreyr/cookiecutter)
and the
[econ-project-templates](https://github.com/OpenSourceEconomics/econ-project-templates).
