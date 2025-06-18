# SOEP data preparation for use with research projects

[![image](https://img.shields.io/github/actions/workflow/status/felixschmitz/soep_preparation/main.yml?branch=main)](https://github.com/felixschmitz/soep_preparation/actions?query=branch%3Amain)
[![image](https://codecov.io/gh/felixschmitz/soep_preparation/branch/main/graph/badge.svg)](https://codecov.io/gh/felixschmitz/soep_preparation)

[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/felixschmitz/soep_preparation/main.svg)](https://results.pre-commit.ci/latest/github/felixschmitz/soep_preparation/main)
[![image](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Project Overview

The project focusses on casting variables to adequate data types, manipulating them in
further ways (e.g. by sensibly filling missing values or reducing the number of
categories), combines them where relevant to new variables (e.g. bmi dummy from
continuous medical variables), and selects them for the creation of a final output
dataset. The flow of the project can be seen in the mermaid diagram below.

```mermaid
flowchart LR
  subgraph Data Files
    id1@{ shape: processes, label: "Convert STATA to pandas" }-->id2@{ shape: processes, label: "Clean existing variables" }
  end

  subgraph Variables
    id2-->idDecision1{Are there variables to derive from and add to a single dataset?}
    idDecision1-->|Yes|id3@{ shape: processes, label: "Create derived variables and add merged to variables datacatalog" }
    idDecision1-->|No|id4@{ shape: processes, label: "Copy cleaned data to variables datacatlog" }
    id2-->id5@{ shape: processes, label: "Create combined variables" }
    id3-->id6@{ shape: processes, label: "Create single metadata" }
    id4-->id6
    id5-->id6
  end

  subgraph Merged Variables
    id6-->id7["Create merged metadata"]
    id7-->id8@{ shape: trap-t, label: "Dataset merging \n(Example function in <code>dataset_merging/task_example.py</code>)" }
    id8-->id9@{ shape: lin-cyl, label: "Dataset \n(Stored in root directory)" }
  end
```

This project processes the SOEP-Core data for use with research projects. The examples
are geared towards using the data with
[GETTSIM](https://gettsim.readthedocs.io/en/stable/). The raw data is provided by the
German Institute for Economic Research (DIW Berlin) and is a panel dataset that follows
the same individuals over time. The data is collected annually and contains information
on various topics such as income, employment, and health. The data is available for
scientific use, for more information visit the
[Research Data Center SOEP](https://www.diw.de/en/diw_01.c.678568.en/research_data_center_soep.html).

The top-level directory is structured as follows:

- `data`: raw data in `.dta` format with directories for each version (e.g. `V38`)
- `src`: source code with tasks for preparing the raw data
- `tests`: tests of the source code
- other files include the processed output dataset (will be created automatically), the
  environment configuration, pre-commit hooks, and some meta-files like this README

## Usage

To get started, install [pixi](https://prefix.dev/docs/pixi/overview#installation) if
you haven't already.

**_Inside the directory `soep_preparation/data` place the folder `V38` containing the
raw `.dta` datafiles._**

To build the project, type

```console
$ pixi run pytask
```

If all tasks run, the file `example_merged_dataset.pickle` is created in the root
directory.

## Working with the Data and Modules

The SOEP data is available in different waves, with the latest being version 39. This
project is currently set up to work with version 38. It is relevant to note that the
SOEP is a survey, which usually asks questions regarding variables in the previous
calendar year (e.g. "What was your annual income _last year_?").

### Terminology

One wave contains "data files" based on different survey modules. For example
`hwealth.dta` contains the wealth information on household level. One of the "variables"
in the dataset is `p010ha` describing roughly speaking the market value of the property
of primary residence (see
[https://paneldata.org/soep-core/datasets/hwealth/p010ha](https://paneldata.org/soep-core/datasets/hwealth/p010ha)).

### Understanding the SOEP-Core Data

If you want to understand the variable `number_of_children` contains, search in the
directory `src/soep_preparation` for the corresponding script and `raw_data` variable
name e.g. `biobirth.py`and `sumkids`. From here one can use the URL
[https://paneldata.org/soep-core/datasets/biobirth/sumkids] to get an understanding of
the variable. The "Codebook (PDF)" might be helpful in understanding. The URL takes
hence the general form:
\[https://paneldata.org/soep-core/datasets/*dataset_name*/*variable_name*\]

To understand which variables are additionally available for a dataset, the URL
`https://paneldata.org/soep-core/datasets/{dataset_name}` might be helpful. Here you can
search for variable names within this file.

### Creating your own Merged Dataset

The directory `src/soep_preparation/dataset_merging` contains two modules. Inside
`helper.py` you can find the function `create_dataset_from_variables`. The function
takes most importantly a list of `variables` and survey years you are interested in. The
survey years can be either passed to the function as a tuple characterizing the min and
max survey years (e.g. `min_and_max_survey_years=(2020, 2025)`) or all the survey years
(e.g. `survey_years=[2020, 2021, 2022, 2023, 2024, 2025]`). Further the argument
`variable_to_file_mapping` is generated by the tasks and contains the meta information
for the variables of interest and their corresponding prepared data. See the example for
the correct specification for the function call of `create_dataset_from_variables`.

Further, there is `task_example.py` which contains an example on how to write your own
task to merge `variables` of interest for a range of `survey_year`'s to a dataset using
the helper module. Other components of the merging process are handled via the
implemented helper functions. Do not include any of the ID variables (`survey_year`,
`hh_id`, `hh_orig_id`, `p_id`) in the columns list, as these are automatically included.

### Advanced: Additional Variables from an Existing Dataset

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

### Advanced: Creating Derived Variables from Multiple Data Sources

See the modules `household.py` and `personal.py` in the directory
`src/soep_preparation/create_derived_variables` for functions creating new variables
from data sources. Inside `personal.py` the function `derive_birth_month` takes as
arguments the cleaned data `ppathl` and `bioedu`, both contain a `birth_month_from_`
variable. The function returns a DataFrame with an unique `birth_month` variable

You can do so similarly by either creating your own function to derive a certain
variable or by adding your variable to an existing function.

### Advanced: Adding a New Dataset Module

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

## Further Structure Description

**_Inside `data` place the folder `V38` containing all `.dta` files to be cleaned and
processed._**

The `src/soep_preparation` directory contains the subdirectories `data`,
`dataset_merging` and `initial_preparation` and the python-scripts `config.py` and
`utilities.py`.

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

## Credits

This project was created with [cookiecutter](https://github.com/audreyr/cookiecutter)
and the
[econ-project-templates](https://github.com/OpenSourceEconomics/econ-project-templates).
