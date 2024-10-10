# SOEP data preparation for use with GETTSIM and in other projects

[![image](https://img.shields.io/github/actions/workflow/status/felixschmitz/soep_preparation/main.yml?branch=main)](https://github.com/felixschmitz/soep_preparation/actions?query=branch%3Amain)
[![image](https://codecov.io/gh/felixschmitz/soep_preparation/branch/main/graph/badge.svg)](https://codecov.io/gh/felixschmitz/soep_preparation)

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/felixschmitz/soep_preparation/main.svg)](https://results.pre-commit.ci/latest/github/felixschmitz/soep_preparation/main)
[![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Project Overview

This project aims to pre-process the SOEP data for usage with
[GETTSIM](https://github.com/iza-institute-of-labor-economics/gettsim). The raw data is
provided by the German Institute for Economic Research (DIW Berlin) and is a panel
dataset that follows the same individuals over time. The data is collected annually and
contains information on various topics such as income, employment, and health. The data
is available for scientific use, for more information visit the
[Research Data Center SOEP](https://www.diw.de/en/diw_01.c.678568.en/research_data_center_soep.html).

The project is structured as follows: Inside the `soep_preparation` directory, there are
directories for the source code `src`, tests of the source code `tests`, and the build
directory `bld`. The `src` directory contains the main source code. and raw data for the
project, while the `tests` directory contains the tests for the source code. Inside the
`bld` directory there are processed files and outputs of the code. The `pyproject.toml`
file contains the project configuration, while `environment.yml` includes the
dependencies and the configuration for the pre-commit hooks is in
`.pre-commit-config.yaml`.

## Usage

To get started, install [pixi](https://prefix.dev/docs/pixi/overview#installation) if
you haven't already.

**_Inside the directory `soep_preparation/src/soep_preparation/data` place the folder
`V37` containing the raw `.dta` datafiles._**

To build the project, type

```console
$ pixi run pytask
```

To clean a single dataset, specify the "dataset_name" by typing:

```console
$ pixi run pytask -k "dataset_name"
```

## How to Add a New Dataset Module or Additional Functions

To add a new SOEP dataset to the project or include additional functions in an existing
module, follow these steps:

1. Add the Dataset to the Data Directory

   Each dataset should be placed in appropriate data directory (e.g., inside
   `soep_preparation/data/V38`). As an theexample, say you want to add the dataset
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
       out["soep_initial_hh_id"] = cleaning_function(raw["cid"])

       return out
   ```

1. Adding more variables from a dataset that is already included

   If you want to include an additional variable from a dataset that is already being
   cleaned, follow this approach:

   Each new variable should be created by processing a column (or several columns) from
   the raw data. The results of this processing will then be added to the final dataset
   that the system builds.

   Here’s how you can do that:

   1. Identify the raw variable you want to transform or clean from your input data.

   1. Use or create a function that transforms this raw variable into the final form you
      need.

   1. Assign the result of that transformation to the out DataFrame, which represents
      your cleaned dataset.

   Suppose you want to add a new variable, `age`, to your final dataset based on the
   `raw` data. Here’s how the process would look:

   ```python
   def clean(raw: pd.DataFrame) -> pd.DataFrame:
       out = pd.DataFrame()

       # Example: Adding a variable 'age' after processing the 'birth_year' column
       out["age"] = calculate_age_from_birth_year(raw["birth_year"])

       return out
   ```

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
