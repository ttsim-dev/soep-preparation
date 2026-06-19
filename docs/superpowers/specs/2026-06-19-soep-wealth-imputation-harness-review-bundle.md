# Review bundle v2 — SOEP wealth-imputation harness

**For:** an independent re-review of the **v2** design after a prior review's must-fixes
were incorporated. No prior context assumed.
**Date:** 2026-06-19
**Companion spec:** `2026-06-19-soep-wealth-imputation-harness-design.md` (v2, same folder).

## How to use this bundle

You are re-reviewing a plan, not code (none written). Two jobs: (a) confirm the v1
must-fixes actually landed in v2 (Section 0), and (b) attack the **residual methodology**
(Section 4), now grounded in real codebase excerpts (Section 3). End with the Section 5
verdict. Be concrete and numbered. The output is fed back to the implementer before any
code is written.

## 0. What changed v1 → v2 (verify these landed)

The first review returned "build with substantial changes" and nine ranked must-fixes. v2
responds:

1. **Person-level construction, then aggregate** → adopted. Modelling unit is
   `person × wave × component × response_state`; households are a downstream sum.
2. **Drop one-GBM-per-implicate; draw stochastically** → adopted. HGB is a
   propensity/ranking engine feeding **PMM** + **bootstrap**; not the value generator.
3. **Response-state machine** (observed / edited / filter-/amount-/share-missing / INR /
   PUNR / structural) → adopted, plus the SOEP `n02220`/`n022h0` provenance flag.
4. **Observed 2022 answers as fixed values + primary donor pool** → adopted
   (current-wave anchoring).
5. **Rolling-origin validation + artificial masking + completed-data metrics + provenance
   audit** → adopted; pooled R² dropped as a headline.
6. **Wave semantic comparability** → flagged as a build-time verification item; the
   `-8`-masked vehicles component is concrete evidence it is non-uniform.
7. **Gross assets vs liabilities separate; joint dependence; accounting identities** →
   adopted (property gross + mortgage separate; chained components; totals = exact sums).
8. **Sample P / weights / subsample reporting** → flagged; weights exist (`phrf`),
   subsample indicator (`psample`) to be added.
9. **Machine-readable server-side artifacts, not log-only** → adopted.

The output tier was also re-scoped: **person-level provisional proxy + calibrated
intervals**, explicitly *not* labelled DIW implicates and *not* for Rubin's-rules
inference (a deliberate cost/faithfulness choice).

## 1. Problem

DIW has not released the imputed 2022 wealth module; only raw answers exist, and the
imputed release lags years (heavy edit + multiple imputation). Learn the process from
waves with both raw and completed values (2002/07/12/17) and produce a **provisional 2022
household wealth proxy** for use as a covariate.

## 2. v2 design in one paragraph

Per-person component targets come from cleaned `pwealth` (already share-resolved by DIW);
features are covariates (`pgen`/`hgen`/`pequiv`/`ppathl`/biography), person-level lagged
wealth carried via stable `p_id`, and observed current-wave answers. Per component: an HGB
ownership-propensity model + an `asinh`-scale amount model used as a **PMM** ranking engine
that draws real donor values (preferring current-wave respondents), with **bootstrap** for
parameter uncertainty and **chained** component generation. Observed/edited cells are held
fixed. Person draws are summed over current household members to households; totals are
exact component sums. Validation is rolling-origin + artificial masking + completed-data
distributional metrics.

## 3. Codebase context (grounding)

Real excerpts from `ttsim-dev/soep-preparation` (public source).

**Target structure — `clean_modules/pwealth.py` (person level).** Already per-person,
share-resolved amounts; five implicate suffixes `a`–`e`; a provenance flag:

```python
out["p_id"]  = apply_smallest_int_dtype(raw_data["pid"])
out["hh_id"] = apply_smallest_int_dtype(raw_data["hid"])
out["property_value_primary_residence_a"]      = apply_smallest_float_dtype(raw_data["p0100a"])
out["net_property_value_primary_residence_a"]  = apply_smallest_float_dtype(raw_data["p0110a"])
out["financial_assets_value_a"]                = apply_smallest_float_dtype(raw_data["f0100a"])
out["private_insurances_value_a"]              = apply_smallest_float_dtype(raw_data["h0100a"])
out["consumer_debt_value_a"]                   = apply_smallest_float_dtype(raw_data["c0100a"])
out["vehicles_value_a"]                        = apply_smallest_float_dtype(raw_data["v0100a"].replace({-8: pd.NA}))
out["gross_overall_wealth_a"]                  = apply_smallest_float_dtype(raw_data["w0101a"])
out["net_overall_wealth_a"]                    = apply_smallest_float_dtype(raw_data["w0111a"])
# provenance — overall aggregate only:
out["imputation_flag_..."] = object_to_str_categorical(
    raw_data["n02220"],
    renaming={"[0] No imputation": "No imputation", "[1] Edited": "Edited", "[2] Imputed": "Imputed"},
    ordered=True)
```

In-repo comment on method and dispersion:

```text
# due to non-response and to achieve comparable market values,
# a maximum-likelihood Heckman selection regression model is used
# the imputation is repeated leading to postfixes a-e
# variation between the imputation results is marginal, averaging is sensible
```

**`clean_modules/hwealth.py`** mirrors these at household level (`p010ha`, `f010ha`,
`w011ha`, …) with `n022h0` as the household provenance flag — i.e. `HWEALTH` is the
household aggregation of `PWEALTH`.

**Cleaning utilities are crude missing-handling only** (`utilities/data_manipulator.py`):
`replace_missing_codes_with_na` (negatives −1…−9 and `[-N] …` strings → NA),
`replace_not_applicable_answer` (−2 → a chosen value), `object_to_float/int/...`,
`apply_smallest_float_dtype`. There is **no edit/impute logic today** — the harness must
add the response-state machine and draws itself.

**Pipeline conventions** (`config.py`, `clean_modules/task.py`, `utilities/general.py`):
one `clean_modules/<name>.py` per raw `<name>.dta`; `RAW_DATA_FILES`/`MODULES` are
`DataCatalog`s keyed by those names; a loop builds one pytask task per module that calls
the module's `clean(raw_data)`; missing raw files fail fast. New work is a sibling
subpackage consuming these catalogs.

**Panel + design (`clean_modules/ppathl.py`):** stable `p_id`; partner pointer (`parid`);
weights `phrf`/`phrf0`/`phrf1`; region (`sampreg`); migration. No `psample` subsample
indicator extracted yet.

## 4. Residual questions for this round

Confirm the v1 fixes are adequately specified, then attack these — several are new and
code-grounded:

1. **PUNR breaks the member-sum (highest priority).** `HWEALTH` includes households where
   some adults did not answer individually but household-level info exists (partial unit
   nonresponse), imputed by DIW. If we predict per-person from per-person raw answers and
   sum responding members, we will **undercount** PUNR households versus `hwealth`. Is the
   per-person-sum target valid at all when PUNR individuals may have no `pwealth` row? Options:
   (a) target `hwealth` directly for PUNR households; (b) add a person-level PUNR
   imputation stage; (c) model a household-level residual/correction on top of the
   member-sum. Which is least biased, and does it undermine the "always person-level"
   stance?

2. **Provenance is only on the overall aggregate.** `n02220`/`n022h0` flag
   no-imputation/edited/imputed for *net overall wealth*, not per component. Reconstructing
   per-component response-state from raw filter/amount/share variables is error-prone. Is
   per-component response-state necessary, or can the harness operate on overall-aggregate
   provenance plus a component decomposition? What is the risk if per-component states are
   mislabelled?

3. **PMM vs SOEP's actual method.** SOEP's process is multi-step and evolving (logical →
   Heckman-type selection → regression / MICE-PMM). We chose PMM for distribution
   preservation and back-transform-bias avoidance, **not** to mirror DIW. Is that defensible
   for a proxy, or should we mirror the selection-model structure (two-step ownership/value)
   to better reproduce the conditional distributions?

4. **Near-degenerate DIW implicates.** The repo states a–e variation is "marginal." Given
   that, is releasing **one prediction + calibrated intervals** clearly preferable to
   emulating five near-identical draws? And what should the intervals represent — imputation
   uncertainty only, or also model/parameter uncertainty? Does "marginal variation" weaken
   the case for bootstrap at all?

5. **Non-uniform wave coverage.** Vehicles and vehicle-inclusive totals are `-8`-masked in
   waves that didn't collect them, so per-component training windows differ and a lag
   feature can reference a component absent in the lag wave. How should the harness predict
   a 2022 component whose historical support is one or two waves, and handle missing lags
   for late-introduced components?

6. **Validation truth.** DIW implicates are themselves model output and near-degenerate.
   For the masked-prediction block, the "truth" should be genuinely *observed* reported
   cells (mask those), not DIW-imputed cells. Confirm the masking design uses only
   genuinely-observed values, and that agreement-with-DIW is reported as a separate
   distributional block, never as cell-level accuracy.

7. **2017 → 2022 shift, anchored.** With observed 2022 respondents fixed and forming the
   donor pool, is the residual COVID/inflation/asset-boom shift adequately handled for the
   *nonresponse* rows, or is an external index offset still warranted as a sensitivity arm?

8. **Engineering.** Fits the pytask / `src` / pure-function / catalog conventions? Is the
   semantic-contract registry with fail-closed `# VERIFY` the right abstraction or
   over-engineered? Are the three test layers (synthetic unit/property, schema-contract,
   disclosure-safe server integration with failing invariants) adequate given no dev-box
   data access? Pinned scikit-learn + a regularized-linear/PMM baseline — right calls?

## 5. Requested verdict

1. Did the v1 must-fixes land adequately (per Section 0)? Any only partially addressed?
2. Build v2 as specified / build with changes / reconsider.
3. Must-fix before building (ranked), focusing on Section 4.
4. Any place a Section 4 framing or mitigation is wrong.
5. Nice-to-have.
