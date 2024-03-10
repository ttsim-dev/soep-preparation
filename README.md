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

## Further Structure Description

The `src/soep_cleaning` directory contains the subdirectories `data`, `dataset_merging`
and `initial_preprocessing` and the python-scripts `config.py` and `utilities.py`.
**_Inside `data` place the folder `V37` containing all `.dta` files to be cleaned and
processed._**

The `initial_preprocessing` directory contains the scripts for the initial cleaning of
the datasets. Data cleaning follows the functional form introduced during the lecture
and creates a task for cleaning and transforming depending on each specified raw
dataset. For each group of datasets (bio, h, p and other), there is a
`_specific_cleaner.py` script with the actual implementation of the respective dataset.
Further the `helper.py` script contains functions to clean the different kinds of
columns to be found inside the raw data. The usual implementation of cleaning a column
is:

```python
out["new_name"] = cleaning_function(raw_data["old_name"])
```

where `out` is the dataset created from the bottom up with the results from
`cleaning_function()`. The latter takes a `pd.Series` as argument (sometimes additional,
but optional inputs) and return the cleaned series as `pd.Series`. `raw_data` is the
original and uncleaned dataset currently being cleaned.

The `dataset_merging` directory contains the scripts for merging the datasets (to be
implemented).

The `config.py` specifies global constants and sets the options for modern pandas.
`utilities.py` contains general helper functions.

## Next steps

- [data catalog](https://pytask-dev.readthedocs.io/en/stable/tutorials/using_a_data_catalog.html)
- Work on `NaN` values inside categorial columns (of any dtype); currently missing
  implementation of working conversion to `pd.NA`
- Some datasets contain columns with integer values, possibly indicating missing values
  (e.g. dataset `hwealth`, column `finanzverm_hh_a` contains values like "-8" even
  though not plausible)
- Some datasets contain categorical columns with identical category names (after
  cleaning/removing the response code), further discussion needed (e.g. dataset `perg`,
  column `pbrutto`)
- Should cleaning helpers handle empty series differently? (currently no error being
  raised and an empty `pd.Series` is being returned)
- Uncertain about implementation of file that contains words that will be ignored by
  `codespell` (currently `.codespell-wordlist.txt`) -> `.codespell-wordlist.yaml`, but
  "File must contain 1 word per line."
- Adaptation to different SOEP versions/waves

## Credits

This project was created with [cookiecutter](https://github.com/audreyr/cookiecutter)
and the
[econ-project-templates](https://github.com/OpenSourceEconomics/econ-project-templates).

[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/EVOsE4mq)
