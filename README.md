# Data management (Prof. von Gaudecker)

[![image](https://img.shields.io/github/actions/workflow/status/felixschmitz/soep_cleaning/main.yml?branch=main)](https://github.com/felixschmitz/soep_cleaning/actions?query=branch%3Amain)
[![image](https://codecov.io/gh/felixschmitz/soep_cleaning/branch/main/graph/badge.svg)](https://codecov.io/gh/felixschmitz/soep_cleaning)

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/felixschmitz/soep_cleaning/main.svg)](https://results.pre-commit.ci/latest/github/felixschmitz/soep_cleaning/main)
[![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Project Overview

This project aims to pre-process the SOEP data for usage with
[GETTSIM](https://github.com/iza-institute-of-labor-economics/gettsim). The raw data is
provided by the German Institute for Economic Research (DIW Berlin) and is a panel
dataset that follows the same individuals over time. The data is collected annually and
contains information on various topics such as income, employment, and health. The data
is available for scientific use, for more information visit the
[Research Data Center SOEP](https://www.diw.de/en/diw_01.c.678568.en/research_data_center_soep.html).

The project is structured as follows: Inside the `soep_cleaning` directory, there are
directories for the source code `src`, tests of the source code `tests`, and the build
directory `bld`. The `src` directory contains the main source code and raw data for the
project, while the `tests` directory contains the tests for the source code. Inside the
`bld` directory there are processed files and outputs of the code. The `pyproject.toml`
file contains the project configuration, while `environment.yml` includes the
dependencies and the configuration for the pre-commit hooks is in
`.pre-commit-config.yaml`.

## Usage

To get started, create and activate the environment with

```console
$ conda/mamba env create -f environment.yml
$ conda activate soep_cleaning
```

To include further dependencies, add them to the `environment.yml` file and (re-)create
the environment as specified above.

**_Inside the directory `soep_cleaning/src/soep_cleaning/data` place the folder `V37`
containing the raw `.dta` datafiles._**

To build the project, type

```console
$ pytask
```

To clean a single dataset, specify the "dataset_name" by typing:

```console
$ pytask -k "dataset_name"
```

## How to Add a New Dataset Module or Additional Functions

To add a new SOEP dataset to the project or include additional functions in an existing
module, follow these steps:

1. Add the Dataset to the Data Directory

Each dataset should be placed in the appropriate data directory (e.g., inside
soep_cleaning/src/soep_cleaning/data).

2. Create a Corresponding Python Script

For each new dataset, create a corresponding Python module (i.e., dataset.py) inside the
initial_cleaning directory. Each module must include a clean function that takes a
pd.DataFrame as input and returns the cleaned dataset.

Example template for the clean function:

```python
import pandas as pd


def clean(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the <dataset_name> dataset."""
    out = pd.DataFrame()

    # Apply cleaning steps to raw data
    out["soep_initial_hh_id"] = cleaning_function(raw["cid"])

    return out
```

- Module Location: Place the new dataset.py script inside the initial_cleaning directory
- Function Naming: The cleaning function must be named clean, take a pd.DataFrame called
  raw, and return a pd.DataFrame containing the cleaned data.

3. Integrate Additional Functions

When adding new functions to an existing module, follow the same approach. Ensure that
each cleaning function processes a column or set of columns and that the result is
assigned to the out DataFrame.

Example of additional cleaning:

```python
def clean(raw: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["soep_initial_hh_id"] = cleaning_function(raw["cid"])
    out["age"] = another_cleaning_function(raw["age_column"])
    return out
```

## Further Structure Description

The `src/soep_cleaning` directory contains the subdirectories `data`, `dataset_merging`
and `initial_cleaning` and the python-scripts `config.py` and `utilities.py`. **_Inside
`data` place the folder `V37` containing all `.dta` files to be cleaned and
processed._**

The `initial_cleaning` directory contains the scripts for the initial cleaning of the
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

[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/EVOsE4mq)
