# Scope

soep-preparation turns raw SOEP-Core Stata files into typed, cleaned, documented
variables. What it does, in three tiers:

1. **Clean raw variables.** Cast each SOEP variable to an adequate dtype, convert the
   SOEP missing codes to `pd.NA`, and give it a readable name (see
   [Naming conventions](naming_conventions.md)). One cleaned module per raw module.
2. **Add standard derived variables.** Variables that are generally useful and follow
   directly from the raw data — e.g. a BMI dummy from height and weight, or a single
   birth month reconciled across modules.
3. **Add derived variables relevant as GETTSIM inputs.** Variables shaped for
   microsimulation with [GETTSIM](https://github.com/ttsim-dev/gettsim): German
   programme names aligned to GETTSIM's spelling, reference periods made explicit so
   income can be aligned to its policy year.

## Out of scope

- **Analysis.** soep-preparation prepares data; it does not run models or produce
  results.
- **A fixed merged dataset.** It ships a helper (`create_final_dataset`) and a metadata
  catalogue; you assemble the variables and waves you need
  (see [Creating a dataset](creating_a_dataset.md)).
- **Reweighting.** SOEP design and longitudinal weights are passed through, not applied.
- **Imputation of missing answers** beyond what the SOEP delivers (the provisional
  wealth-imputation harness is a separate, opt-in component).
