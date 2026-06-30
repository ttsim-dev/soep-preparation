# Wealth imputation (provisional 2022 proxy)

The `wealth_imputation` package builds a **provisional 2022 household-wealth proxy** for
SOEP-Core households. It is an opt-in pipeline (it does not run as part of
`pixi run pytask`); see [Running it](#running-it).

```{warning}
This is a **historical-model synthetic projection**, not an edit-and-impute of an
observed 2022 wealth wave. The release used here — SOEP-Core V41 (data through 2024) —
contains **no 2022 wealth wave at all**: a row-count probe of `pwealth`/`hwealth` finds
2017 fully populated (all five implicates) but **zero rows for 2022** (raw or imputed),
and 2017 is the latest wealth wave present. The SOEP wealth module is fielded every five
years, so a 2022 wealth file is expected only in a later SOEP release. Until then
**there are no raw 2022 wealth cells to anchor on**: every 2022 household is predicted
from the 2002–2017 wealth waves plus 2022 covariates, and no observed 2022 wealth value
is preserved. `summary["uses_observed_2022_answers"]` is therefore `False`. Treat the
output as a model-based projection, not a measured distribution.
```

## What it produces

For each 2022 household the pipeline reports a point estimate and a band for two totals,
side by side:

- **Component-only total** (primary) — the sum of the modelled wealth components. This
  is the headline output because it is the object the out-of-fold backtest validates.
- **Residual-inclusive total** (scenario) — the component-only total plus a signed
  *reconciliation residual* to the official net total. It is the more complete economic
  object, but it rests on a single-wave (2017-only) residual fit that no backtest can
  validate, so it is published as a scenario, not the headline.

Both are written to `bld/wealth_imputation/`, together with a run summary whose
`distribution_across_draws` describes the predictive wealth distribution (see
[Uncertainty](#uncertainty-what-the-bands-mean)).

## Components modelled

Six components are modelled from the DIW-aggregated household wealth file (`hwealth`) and
the person wealth file (`pwealth`). Donor values from earlier waves are deflated to 2022
terms by an asset-class index where one applies:

| Component | Source | Deflation to 2022 |
|---|---|---|
| Owner-occupied property (gross) | `hwealth` | House-price index |
| Owner-occupied mortgage (liability) | `hwealth` | None (nominal debt balance) |
| Financial assets | `hwealth` | MSCI World equity index |
| Vehicles | `hwealth` | None (depreciating; no asset index) |
| Private pension / insurances | `pwealth`, summed over members | REX government-bond index |
| Consumer debt | `pwealth`, summed over members | None (nominal debt) |

Joint components (property, financial, vehicles) come from the household file, which
already resolves ownership shares correctly. The two **person-direct** components
(insurances, consumer debt) carry no ownership share and are absent from the household
file, so they are summed from the person file across household members.

**Other real estate** and **business assets** are *not* modelled separately — they are
the dominant part of the residual (below).

## Method

The pipeline composes small, individually tested blocks (`impute.run_imputation`):

1. **Heads and features.** Assemble the person feature matrix, attach each person's
   lagged prior-wave wealth, and represent each household by its oldest member.
2. **Targets.** Merge the joint household targets and the summed person-direct targets
   onto household heads.
3. **Two-part component models.** Restricting training to the wealth waves
   (2002/2007/2012/2017, the prediction wave always excluded), fit per component an
   **ownership** model (incidence) and an **amount** model (value given ownership) on the
   continuous and one-hot categorical predictors. A component is skipped if it has too
   few training rows or owners.
4. **Donor draws.** Simulate complete joint draws: each component value is drawn by
   **predictive mean matching** (PMM) from the nearest historical donors on an
   asinh-scaled matching score, and the signed components are summed to a household net
   total. Earlier-wave donor values are deflated to 2022 by the indices above.
5. **Residual draw.** If a residual model is fit, the signed reconciliation residual is
   drawn by PMM each draw and reported as the separate residual-inclusive total.

## The reconciliation residual

The residual is `official_net_total − Σ(component_sign × modelled_component)` over the
fitted components, kept in euros and **signed** (negative where modelled wealth meets or
exceeds the official total). It is a *reconciliation* residual: dominated by the
unmodelled business and other-real-estate mass, but it also absorbs omitted liabilities,
editing/measurement discrepancies, implicate noise, and model-definition mismatch.

It is drawn by signed PMM (preserving the sign and empirical distribution of the donor
residuals) and deflated to 2022 by a property/equity blend keyed to its dominant mass. It
is **provisional**: its outcome (the augmented official total `n011h`) exists only from
2017, so it is fit on a single wave and cannot be temporally validated. This is why the
residual-inclusive total is a scenario, not the headline.

## Uncertainty: what the bands mean

The `lower`/`upper` bounds are **conditional donor-randomisation spreads**, *not*
calibrated predictive intervals. They capture ownership/PMM draw variability while
holding the fitted models and the single wealth implicate fixed (the residual model is
fixed but its donor value is redrawn per draw), and they carry no modelled
cross-component covariance — components and the residual are drawn independently given
the covariates, so only shared-covariate co-movement enters the total. Treat the bands as
a **lower bound** on predictive uncertainty.

Two summaries serve two jobs:

- **Per-household intervals** collapse each household's draws to a median point estimate
  and a band — for covariate use, one row per household.
- **`distribution_across_draws`** computes each distributional statistic (Gini,
  top/zero/negative shares, percentiles) *within* each complete draw and then summarises
  it across draws. This is the correct predictive wealth distribution: the cross-section
  of per-household medians is **not**, because it erases the zero and negative mass any
  single complete draw carries.

## Backtest

The proxy is validated by holding out the latest observed wealth wave (2017): fit on the
earlier waves, impute 2017 out of fold, and compare the imputed **component-only** total
against the held-out wave's **completed-component** sum (the six-component sum kept only
where every component is present). This is a completed-component fidelity check, not
raw-observed truth, and it does not cover the residual (which has no earlier outcome
wave). Metrics are low-cell-count-screened aggregates — distribution summaries, a
quintile confusion matrix, rank accuracy, and band coverage.

## Intended use and calibration status

The component-only total is a **rank/covariate proxy** for downstream use (e.g. as a
GETTSIM input), not a calibrated population estimate. The 2017 temporal backtest shows
its **level** is off (imputed mean ~33% above observed), its **inequality** is
understated (Gini below observed), and its **zero mass** and **negative tail** are not
calibrated to observed. So 2022 levels, inequality, and tails must not be headlined as
population estimates — only the proxy's relative/rank information is intended to carry.

The across-draws bands are **draw dispersion** — Monte-Carlo spread over donor draws —
**not** predictive intervals, so they do not measure calibration coverage. Keep the
no-anchoring warning and the support diagnostics beside every 2022 level, tail, and
mobility table.

**Path to calibration (future, partly data-gated):** proper predictive intervals
(bootstrap over model fits combined with the five a–e implicates), an all-wave `w011h`
residual with a pseudo-out-of-fold backtest, and the raw 2022 wealth wave once SOEP
releases it.

## Known limitations

- **No raw 2022 anchoring** — SOEP-Core V41 ships zero 2022 wealth rows (see the warning
  above), so no observed 2022 cell exists to use or preserve; every recipient receives a
  historical-model donor. This is not a code gap a caliper or support gate can close: it
  is gated on a future SOEP release that ships the 2022 wealth wave. Until then the
  operative regime is disclosure — donor-distance, donor-wave composition, single-wave
  flags, and the asset-class-index sensitivity of the deflation — not observed-2022
  anchoring.
- **Person-direct sums require full representation** — summing person-direct insurances
  and consumer debt over members is unbiased only for households where every eligible
  adult's person-wealth row is present; partial-unit-nonresponse adults absent from
  `pwealth` are omitted (`_household_person_direct`).
- **Independent recombination** — the household total enforces the accounting sum within
  a draw but not the empirical joint law of the components; tail mass and inequality can
  reflect recombination rather than observed households.
- **Nominal debts** — mortgage, vehicle, and consumer-debt donors are not deflated.
- **Single implicate** — implicate `a` is used as the representative value; multiple-
  implicate uncertainty is not propagated.

Two distinct objects both involve donor score-distance and should not be conflated:

- The **`out_of_support` diagnostic** is what production reports. It is an *ungated*
  measurement: nearest-donor score-distance quantiles plus an `out_of_support_share`,
  computed without altering any draw. It describes how far recipients sit from their
  donors; it does not filter or change the drawn value.
- An optional PMM **`caliper`** is an *eligibility gate*. It filters each recipient's
  donor candidates to those within the caliper *before* nearest-k selection, so it
  changes the drawn value and raises if no candidate survives. Production runs ungated
  (`caliper=None`), so only the diagnostic is in effect.

## Running it

The pipeline is opt-in. Enable it with the `SOEP_WEALTH_IMPUTATION` environment variable,
or the convenience task:

```bash
pixi run wealth        # equivalent to SOEP_WEALTH_IMPUTATION=1 pixi run pytask
```

Outputs land in `bld/wealth_imputation/`:

- `household_wealth_2022_component_only.arrow` — primary component-only intervals.
  This is the **modelled-component** net-wealth proxy (the six modelled components); it
  omits business assets and other real estate, so it is *not* total household net
  wealth. The residual-inclusive scenario below is the more complete (but unvalidated)
  object.
- `household_wealth_2022_residual_inclusive.arrow` — residual-inclusive scenario
  intervals.
- `imputation_summary.json` — run settings, component counts, and the across-draw
  distributions (component-only and the residual scenario).
- `backtest_2017_report.json` — the out-of-fold 2017 backtest scorecard.
