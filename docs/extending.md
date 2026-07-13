# Extending the pipeline

All cleaning follows functional data management: build the result column by column from an
empty frame, touch each variable once, one pure function per transformation, and spell out
every input column at the assignment site. Follow the [Naming conventions](naming_conventions.md)
for every new variable.

## Add a variable to an existing module

Each cleaned variable is one column built from one or more raw columns. In the module's
`clean(raw_data)` function, assign it to the `out` frame:

```python
def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["age"] = calculate_age_from_birth_year(raw_data["gebjahr"])
    return out
```

The cleaning function takes the specific raw `pd.Series` it needs and returns the cleaned
series. Add a focused test for the cleaning function.

## Derive a variable from several modules

`combine_modules/` holds scripts that build a variable from more than one cleaned module.
Each exposes `combine(...)` taking the cleaned modules it needs and returning a frame with
the new variable. The result is itself a module, named `{module_1}_{module_2}` (e.g.
`pequiv_pkal`). See `combine_modules/` for examples; reconcile the inputs' grain and
reference period explicitly when they differ.

## Add a new module

1. Place the raw `.dta` file in the data directory (e.g. `soep_preparation/data/V41`).
2. Create `clean_modules/<module>.py` exposing
   `clean(raw_data: pd.DataFrame) -> pd.DataFrame`:

   ```python
   import pandas as pd

   from soep_preparation.utilities.data_manipulator import cleaning_function


   def clean(raw_data: pd.DataFrame) -> pd.DataFrame:
       """Create cleaned, typed variables from the raw <module> module."""
       out = pd.DataFrame()
       out["hh_id_original"] = cleaning_function(raw_data["cid"])
       return out
   ```

3. The metadata catalogue picks the module up automatically on the next `pytask` run.

For the available cleaning helpers (dtype conversions, dummy creation, missing-code
handling, category merging), see `utilities/data_manipulator.py`.
