@.ai-instructions/profiles/tier-b-research.md

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in
this repository.

## Project Overview

**soep-preparation** is a data pipeline for preparing German Socio-Economic Panel (SOEP)
survey data. It converts raw Stata `.dta` files into typed, cleaned pandas DataFrames
with standardized variable names, then creates a metadata catalog and a helper for
building final merged datasets.

Part of the [gettsim ecosystem](https://github.com/ttsim-dev/ttsim) — the output of this
pipeline feeds into **gettsim** for microsimulation of the German tax and transfer
system. Built on **pytask** for task orchestration and **pixi** for environment
management.

## Commands

```bash
# Environment setup
pixi install

# Run the full data pipeline
pixi run pytask

# Run tests
pixi run -e py314 tests
pixi run -e py314 tests-with-cov   # with coverage
pixi run -e py314 tests -n 7       # parallel with xdist

# Run a single test file
pixi run -e py314 tests tests/clean_existing_variables/test_create_dummy.py

# Run a single test by name
pixi run -e py314 tests -k "test_name"

# Type checking
pixi run -e py314 ty

# Quality checks (linting, formatting, codespell, etc.)
prek run --all-files

# Available environments: py314
```

## Verification Before Finishing Any Code Change

Before finishing any task that modifies code, always run these three verification steps
in order:

1. `pixi run -e py314 ty` (type checker)
1. `prek run --all-files` (quality checks: linting, formatting, yaml, etc.)
1. `pixi run -e py314 tests -n 7` (full test suite)

## Architecture

### Pipeline Stages (pytask DAG)

1. **convert_stata_to_pandas/** — Reads raw `.dta` files into pandas DataFrames, stored
   in `RAW_DATA_FILES` DataCatalog.
1. **clean_modules/** — Per-module cleaning scripts (e.g. `pbrutto.py`, `pequiv.py`).
   Each exposes a `clean(raw_data: pd.DataFrame) -> pd.DataFrame` function. Results
   stored in `MODULES` DataCatalog.
1. **combine_modules/** — Derives new variables from multiple cleaned modules (e.g.
   `pequiv_pl.py` creates BMI from health variables). Each exposes a `combine(...)`
   function.
1. **create_metadata/** — Generates `variable_to_metadata_mapping.yaml` mapping each
   variable to its module, dtype, and available survey years.

### Key Modules

- **config.py** — Global constants (`SOEP_VERSION`, `SURVEY_YEARS`, path constants),
  DataCatalogs (`RAW_DATA_FILES`, `MODULES`), `METADATA` dict, pandas options.
- **final_dataset.py** — `create_final_dataset(modules, variables, survey_years)` merges
  selected variables from multiple modules via outer join on index variables.
- **utilities/data_manipulator.py** — Core transformation functions: `object_to_int`,
  `object_to_float`, `object_to_str_categorical`, `object_to_bool_categorical`,
  `apply_smallest_int_dtype`, `create_dummy`, `combine_first_and_make_categorical`, etc.
- **utilities/error_handling.py** — Validation with `fail_if_*` pattern.
- **utilities/general.py** — File discovery helpers (`get_raw_data_file_names`,
  `get_combine_module_names`, `load_script`).

### Data Flow Pattern

Cleaning scripts follow a consistent pattern:

```python
def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["new_var"] = cleaning_function(raw_data["raw_var"])
    return out
```

Combined modules follow:

```python
def combine(module_a: pd.DataFrame, module_b: pd.DataFrame) -> pd.DataFrame: ...
```

## Code Conventions

- **PyArrow-backed dtypes** throughout (`int8[pyarrow]`, `string[pyarrow]`,
  `bool[pyarrow]`, etc.) for memory efficiency.
- **SOEP missing data**: negative single-digit values (-1 to -8) and strings like
  `"[-1] Missing"` are converted to `pd.NA`.
- **Index variables**: `p_id`, `hh_id`, `hh_id_original`, `survey_year`.
- **Ruff with `ALL` rules** enabled (see `pyproject.toml` for specific ignores).
  Google-style docstrings.
- **Type checker**: `ty` (not mypy).
- No direct commits to `main` (enforced by pre-commit hook).
- Markdown files are wrapped at 88 characters (`mdformat`).
