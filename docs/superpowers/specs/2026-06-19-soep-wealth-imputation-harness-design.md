# SOEP wealth-imputation harness — design (v3)

**Date:** 2026-06-19
**Status:** design. Milestone 0 + universe/registry/test scaffolding are ready to build;
the model + draw stage is buildable once this spec's modelling sections are treated as
final (they now are).
**Project:** `soep-preparation`
**History:** v1 (household-direct, five deterministic GBMs) retired after review 1; v2
(person-level proxy + PMM) revised after review 2. This v3 writes in review 2's nine
must-fixes and ten framing corrections.

## Goal

DIW has not released the imputed 2022 wealth module; only raw answers exist, and the
imputed release lags years. Learn the wealth edit-and-impute process from waves with both
raw and completed values (2002/07/12/17) and produce a **provisional 2022 household wealth
proxy** for use as a covariate/level.

## What this is — and is not

- **Is:** a person-level donor-based provisional imputation with a household-level PUNR
  completion layer, released as a central prediction **plus historically-calibrated
  predictive intervals** (optionally a few named draws), anchored in observed 2022 answers,
  with three-axis cell provenance.
- **Is not:** the official DIW release; not valid multiple imputation for Rubin's-rules
  inference; output is **never labelled "implicates."** Label: *"model-based provisional
  wealth, historically calibrated under the documented support and missingness assumptions
  — not official, not for MI inference."*

## Runtime & data-access rules

Development is local (synthetic-fixture tests). Real-data execution is on **gpu-01**:
`ssh hmga@gpu-01.iame.uni-bonn.de` (alias `gpu-01`); on gpu-01 only ever `git pull`,
`pixi run pytask [flags]`, read logs. SOEP V41 at `/home/hmga/sciebo-SOEP/SOEP_V41`;
clone from `/home/hmga/soep-preparation`. I never read row-level data — every task emits
**machine-readable, disclosure-safe artifacts** (counts, weighted moments, quantiles,
calibration curves, coverage, hashes), never row-level dumps.

## Data reality (from the pipeline)

- One `clean_modules/<name>.py` per raw `<name>.dta`; `RAW_DATA_FILES`/`MODULES` are
  `DataCatalog`s keyed by those names; `clean_modules/task.py` loops one task per module.
- `pl`/`hl`/`pwealth`/`hwealth`/`pequiv`/`pgen`/`hgen`/`ppathl`/biography are themed **long**
  files (harmonized names, stacked across waves with `syear`).
- `p_id` stable across waves; `hh_id` not stable across the five-year gaps.
- Cleaned `pwealth` holds per-person, **already share-resolved** amounts (`p0100a`, …) with
  implicate suffixes a–e; `n02220`/`n022h0` give a `No imputation / Edited / Imputed` flag
  **only for the overall aggregate**. The repo notes SOEP uses a Heckman-type selection
  model and that a–e variation is "marginal." Cleaning utilities do crude missing-handling
  only — no edit/impute logic exists yet.
- **`HWEALTH` is not a pure member sum:** for partial-unit-nonresponse (PUNR) households —
  members with valid household info who did not complete the individual questionnaire — DIW
  imputes and includes the wealth in `HWEALTH`. This drives the architecture below.

## Architecture — `src/soep_preparation/wealth_imputation/`

### Estimand and target

- **Primary models** (ownership incidence, amount) are trained on **genuinely observed,
  plausible, share-resolved raw outcomes only.** Completed `pwealth` values for historical
  *nonresponders* are **not** observed training truth.
- Completed `pwealth`/`hwealth` are used for: PUNR residual construction, distributional
  teacher-fidelity checks, editing-outcome comparison, and possible *secondary* calibration
  — never as cell-level prediction truth.
- **Internal representation:** `person_component_value` is a **final, share-resolved euro
  amount**. Ownership share is metadata and a raw→person *construction input*, applied once
  when turning a joint questionnaire answer into a person amount — **never** a second
  multiplier at aggregation. Household aggregation is therefore a **plain sum** over members.

### Person-respondent core + household PUNR completion

`HWEALTH` = (sum of person component values over represented members) + a **household-level
PUNR completion residual**. For each historical implicate `m`, component `c`, household `h`:

```
R[h,c,m] = H[h,c,m] − Σ_{p ∈ represented members} P[p,c,m]
```

Pipeline:
1. Build the complete eligible-adult roster **independently of `pwealth`**.
2. Sum person-level values for responding / item-imputed members.
3. Apply deterministic PUNR rules from household-questionnaire info, other members'
   reports, ownership/renting status, and ownership-share constraints.
4. Model the remaining component-level PUNR residual using **only PUNR-available
   information** (no individual-questionnaire covariates for the nonresponding person).
5. Add the residual to the respondent member sum; the **household total is authoritative**.
6. *(Optional)* allocate the residual to PUNR adults for bookkeeping only.

Reconciliation gates (pre-modelling): residual ≈ 0 within tolerance for fully-responding
households; residual incidence matches independently-identified PUNR; gross/liability
residual signs respected; same implicate suffix on both sides; component-definition/rounding
gaps quantified; PUNR not conflated with ordinary item nonresponse.

### Components & wave coverage

Canonical ontology (registry-mapped per wave): owner-occupied property **gross** + its
**mortgage** (separate internally; net derived), other real estate, financial assets,
private pensions/insurance/building-savings, business assets, vehicles/tangibles, consumer
debt. Coverage is **non-uniform** (e.g. vehicles + vehicle-inclusive totals are `-8`-masked
in waves that didn't collect them). Gross/net household totals are the **exact sum** of
aggregated components (assets − liabilities), never separately predicted.

### Three-axis cell provenance

Replace any single provenance field with three independent axes (retained into the output):
- **A — raw input state:** observed-complete; observed filter+amount, missing share;
  observed ownership, missing amount; missing filter; PUNR; structural/not-collected;
  inconsistent; ambiguous.
- **B — historical alignment evidence:** raw-derived agrees with all implicates; agrees with
  implicate mean within tolerance; official differs; aggregate flag conflicts; unavailable.
- **C — harness action:** preserved; deterministic logical edit; ownership imputed; amount
  PMM-imputed; share imputed; PUNR household residual; statistical/outlier replacement;
  suppressed.

`n02220`/`n022h0` are used as **aggregate consistency checks, not component routers.** Only
observed-plausible values and **deterministic logical edits** are fixed across replications;
statistical/model replacements **vary** across draws.

### Feature blocks

- **Covariates** (all waves incl. 2022): `pgen` (age/education/employment), `pequiv`
  (income), `hgen` (homeownership, composition), self-employment (business proxy), region,
  East/West.
- **Person-level lag:** each continuing member's prior-wave component + raw ownership share
  carried via stable `p_id`; continuity features (share/number of current adults observed
  prior, number of prior households contributing, entrants/leavers, split/merge,
  reference-person change, partner continuity, lag cell's A/B/C state). No-lag variant where
  continuity is absent.
- **Current-wave anchoring:** observed plausible target-wave answers (incl. 2022
  respondents) are fixed and form the **primary donor pool**.

### Draw mechanism & PMM/propensity policy

HGB (`HistGradientBoosting{Classifier,Regressor}`) is a **propensity/ranking engine, not
the value generator**. Per component, chained: ownership → gross amount → debt → share,
later components conditioned on earlier current-draw values.

- **Ownership:** calibrate incidence probabilities on held-out grouped data (calibration,
  not just discrimination) before drawing Bernoulli ownership; or reproduce the documented
  deterministic filter — choice stated per component.
- **Amounts via PMM:** amount model on `asinh(y / s_c)` (component scale `s_c` recorded;
  nominal amounts deflated; wave indicators) → predicted mean → draw from **multiple** near
  donors (not single nearest), preferring current-wave respondents. PMM avoids the analytic
  `sinh`-of-mean retransformation step but is **not bias-free**: it can be biased through
  donor mismatch, response selection, poor common support, sparse top-tail donors. Required
  policy: validate `k`; exact/coarse matching strata (component, target wave, PUNR/INR,
  homeownership, sample, region); a distance **caliper** / common-support criterion;
  **exclude** recipient, linked co-owners, current household, and lineage from the donor set;
  record donor distances, reuse, effective donor count, current-vs-historical donor share;
  a **declared fallback** (simpler model, broader stratum, deliberately wide interval, or
  suppression); test weighted vs unweighted donor selection.
- **Baseline:** a regularized logistic/linear + PMM model runs alongside HGB; a Heckman-style
  selection model is a *benchmark*, not the primary method.

### Interval protocol (core, not deferred)

Internal simulation is a **required stage**. Nested generation:
1. Fit a moderate number of **household-lineage bootstrap** models (parameter uncertainty).
2. From each fit, generate multiple **donor/incidence draws**.
3. Aggregate **each complete joint draw** to the household (sum components within the draw).
4. Central value = **predictive mean** of household totals; interval = **empirical quantiles
   of the household-total distribution**. **Never** sum component interval endpoints.
5. Increase replications until reported means and interval endpoints are stable under a
   pre-specified Monte-Carlo tolerance (tens of draws are too few for tail quantiles).

Intervals represent: incidence + donor/residual + parameter + PUNR-residual + cross-component
dependence uncertainty. They do **not** represent survey-sampling uncertainty, measurement
error, arbitrary misspecification, or a full unmodelled 2017–2022 structural break.
"Calibrated" is an empirical operation: tune/verify widths on held-out masked + pseudo-PUNR
cases and report **coverage** by component, response state, lag availability, PUNR vs INR,
wealth quantile, and subsample. Output draws (if released) are named `draw_1…`, never
"implicates."

### Output

Person-level provisional values + household-level PUNR completion → household totals; central
prediction + calibrated intervals; three-axis cell provenance + method/version; provisional
label; weighted + unweighted distributional summary. Monetary outputs stay `float64` (no
smallest-float downcast). Two totals are maintained: a **common-component total** (comparable
across validation waves) and the **full 2022 total**.

### Component-wave evidence tiers

Milestone 0 produces a matrix: `component × raw wave × completed wave × lag availability ×
semantic comparability × observed donor count`. Each 2022 component is assigned a tier:
- **A** (comparable multiwave): full rolling-origin lagged design.
- **B** (current-wave support, no comparable lag): target-wave cross-sectional + current
  donors; `lag_not_collected` as an explicit state, not generic NA.
- **C** (one short window + current): simpler regularized model, current-wave PMM, broader
  uncertainty.
- **D** (insufficient): do not manufacture; release a partial/common-component total,
  suppress the full total, or give an explicit scenario.

Release gates per component: minimum usable donors, weighted effective donor count, subgroup
support, max donor distance, out-of-support share, interval width vs component scale.

### Validation (deployment-mimicking, leakage-proof)

- **Rolling-origin:** develop on the earliest transition, predict 2012 using only pre-2012
  info, freeze, hold 2017 as the final temporal test. Never train on a future wave to
  predict an earlier one.
- **Masked truth = genuinely observed, plausible raw cells only.** Gold-set rules: complete
  raw filter/amount/share; successful raw→person share resolution; no conflicting duplicate/
  co-owner info; no statistical replacement; no structural missing; no unresolved registry
  item; no later-wave-derived target. DIW-imputed cells are **not** truth; agreement-with-DIW
  is a **separate completed-distribution block**, never cell-level accuracy.
- **Within-target-wave cross-fit** (mirrors deployment): split observed target-wave cases
  into adaptation/donor folds vs held-out masking folds; a held-out value is never a training
  outcome or a donor; rotate folds; aggregate only after every held-out household has an
  out-of-fold prediction. (Resolves the "2017 untouched" ambiguity: *evaluation cells* are
  untouched, but other 2017 respondents may be used, exactly as 2022 deployment uses observed
  respondents.)
- **Cluster masking:** mask filter/amount/share/derived together; mask all co-owner reports
  of the same asset; **pseudo-PUNR** removes a person's whole individual questionnaire while
  retaining genuinely-still-available household-questionnaire info; preserve nonresponse
  correlation across components. Report separately for logically-recoverable, ordinary INR,
  unknown-ownership, PUNR, and no-lag cases.
- **Metric blocks** (weighted + unweighted; pooled R² never a headline): identity on
  untouched; editing agreement on edited; predictive accuracy on artificially-masked observed
  cells; agreement with DIW **completed-data distributions** (means, quantiles, incidence,
  inequality, transitions). Teacher fidelity compares to the MI **mean/spread or pooled
  draws**, never predicted-`a`-vs-official-`a`.
- **Downstream use** (not "regression coverage", since this is not valid MI): report
  coefficient bias/attenuation under masking, rank/quintile stability, coefficient
  sensitivity across internal draws, and predictive performance of wealth-as-covariate
  models. No nominal CI-coverage claims without an explicit uncertainty-propagation method.
- **Target-provenance audit:** for V41, determine whether earlier waves were retrospectively
  reprocessed, which waves fed each target, and each target/lag cell's A/B/C state — so no
  future information leaks through an imputed lag.

## Registry as a typed semantic contract (`raw_wealth_registry.py`)

Per *(component, wave, source)*: source file, raw variable, questionnaire concept, unit &
sign, universe/filter, missing/refusal/bracket codes, amount↔filter↔share relationships,
co-ownership/aggregation rule, expected type/range, canonical component, and **typed
verification fields** — `verification_status`, `verification_evidence`, `codebook_version`,
`probe_status`, `required_for_release` (not a literal `# VERIFY` comment). An unresolved
*optional* component-wave entry fails only that evidence tier; an unresolved entry
*required for the advertised full total* fails the release. A raw-name crosswalk is not
enough — wave semantics (esp. 2002) must be proven comparable.

## Milestone 0 — structured source probe (gpu-01)

A disclosure-safe **structured report** (not log lines): per registry entry — present?,
variable label, row counts, nonresponse-code frequencies, merge cardinalities, duplicate-key
checks, plausible ranges, coverage by wave/component/response-state — plus the
component-wave evidence matrix and a PUNR roster/residual reconciliation summary.

## Dependencies

Add `scikit-learn` (conda-forge), **pinned**; re-lock `pixi`, commit `pixi.lock` in the
same commit. Persist training recipe, feature schema, data snapshot/hash, git commit,
registry hash, lock hash, seed schedule per run (sklearn estimators aren't portable across
versions). Implement a small PMM (multi-donor draw on predicted mean) helper.

## Testing

1. **Unit/property (synthetic):** missing-code conversion; share resolution applied exactly
   once; co-owner logic; splits/merges; aggregation identities; observed-value preservation;
   deterministic edits fixed while statistical replacements vary; structurally-uncollected
   values never enter the donor pool nor become zero; lineage-level seed/bootstrap
   reproducibility. Plus an **end-to-end golden synthetic panel** with co-owners, PUNR, INR,
   a split, a merge, a structurally-absent component, and a no-lag entrant.
2. **Schema-contract:** source columns, labels, dtypes, universes, merge cardinalities,
   registry completeness / typed-verification resolution.
3. **Compute-server integration (gpu-01):** machine-readable disclosure-safe artifacts; the
   run **fails** on critical-invariant breach — no future-wave feature in a historical fold;
   observed/deterministic cells identical across draws; statistical cells show calibrated
   spread; household totals == component sums; **PUNR residual reconciliation** within
   tolerance; no duplicate `p_id × wave`/household-component rows; no impossible ownership/
   debt combos; lag vs no-lag compared on the same eligible sample; donor-distance/reuse/
   common-support gates; propensity calibration (reliability), not just discrimination;
   interval Monte-Carlo stability and historical coverage.

Server artifact includes: counts by every raw/harness state; PUNR roster + residual
reconciliation; component-wave support matrix; donor distances/reuse/out-of-support rates;
ownership calibration curves; interval coverage/width by subgroup; weighted + unweighted
component moments; exact accounting-identity failures; model-vs-baseline; data/registry/code/
lock/seed hashes.

## Build order

1. Registry + structured source probe (gpu-01) — resolve sources, build the component-wave
   matrix, validate the typed contract.
2. Eligible-person roster + **PUNR reconciliation**.
3. Canonical raw-state construction (three-axis provenance) + observed-gold construction +
   deterministic editing.
4. Validation masks + simple baseline.
5. Features (covariates, person-level lag, current-wave anchoring).
6. HGB/PMM fitting (propensity calibration + PMM policy) + chained components.
7. PUNR completion layer.
8. Aggregation + interval calibration + release gates + disclosure-safe reporting.
9. 2022 provisional output + intervals + provenance + caveats.

Steps 1–4 (+ test scaffolding) are unblocked now; 5–9 follow once the open data gates close.

## Open data gates (close before fitting the affected models)

- High-wealth sample **P** / refreshment **R** coverage; `psample` not yet extracted (add it);
  person- vs household-weight choice (`phrf`/`hhrf` available).
- 2002 specifics: component count/definitions vs later waves, the sub-threshold rule, and any
  retrospective/longitudinal imputation putting later-wave info into a 2002 lag.
- The per-vintage/per-component imputation mix in the V41 wealth methodology doc, and each
  component's per-wave availability.

## Corrected framings (do not reintroduce)

- `HWEALTH` is **not** always a member sum — PUNR adds a household completion layer.
- Architecture is **person-respondent core + household PUNR completion**, not "always
  person-level."
- Historical raw ownership shares are needed too (not only 2022) for gold-standard donors and
  masking truth.
- Aggregation is a **plain sum** of share-resolved person values (no second share multiplier).
- **Only deterministic** edits are fixed across draws; statistical replacements vary.
- PMM avoids the analytic retransformation step but is **not** generally bias-free.
- "Marginal" official a–e spread describes DIW's implementation, **not** the predictive
  uncertainty this proxy must quantify.
- Internal simulation (many draws) is **core**, not nice-to-have.
- No nominal downstream CI-coverage claims for a non-MI product.
- Current-wave donors handle much of the **level** shift only where support/assumptions hold;
  they don't by themselves resolve the 2017→2022 break.

## Deferred (nice-to-have)

Index-updated lag benchmark/sensitivity arm for components with a defensible external
mapping; Heckman/response-weighted benchmark for selection sensitivity; MNAR/top-tail stress
scenarios; chain-order/multivariate-bundle sensitivity; a published model card (evidence
tiers, interval interpretation, prohibited uses, unsupported subgroups).
