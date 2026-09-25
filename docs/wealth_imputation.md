# Projected household wealth in 2022

The `wealth_imputation` package constructs a model-based measure of household net wealth
for the 2022 SOEP-Core sample. The procedure uses the observed wealth waves 2002, 2007,
2012, and 2017 together with household characteristics observed in 2022. It is disabled
in the standard pipeline and must be run explicitly.

```{warning}
SOEP-Core V41 contains no wealth observations for 2022. The latest available wealth
wave is 2017; neither `pwealth` nor `hwealth` contains raw or imputed records for 2022.
Consequently, the procedure does not impute item nonresponse in an otherwise observed
2022 wealth distribution. It extrapolates the conditional distribution estimated from
earlier waves to the 2022 covariate distribution. No 2022 wealth observation is used or
preserved.

The resulting variable should therefore be described as **projected household wealth**,
not as observed or conventionally imputed 2022 wealth.
```

## Target variables

Let $c$ index the six modelled components and let $s_c$ equal $1$ for assets and $-1$
for liabilities. The primary outcome for household $i$ is

$$
W^{C}_{i,2022} = \sum_{c=1}^{6} s_c W_{ic,2022}.
$$

This **component total** includes the following balance-sheet items.

| Component                                | Sign | Source                                 | Conversion to 2022 prices |
| ---------------------------------------- | ---: | -------------------------------------- | ------------------------- |
| Owner-occupied property, gross value     |  $+$ | `hwealth`                              | House-price index         |
| Mortgage debt on owner-occupied property |  $-$ | `hwealth`                              | None                      |
| Financial assets                         |  $+$ | `hwealth`                              | MSCI World index          |
| Vehicles                                 |  $+$ | `hwealth`                              | None                      |
| Private pensions and insurance wealth    |  $+$ | `pwealth`, aggregated within household | REX bond index            |
| Consumer debt                            |  $-$ | `pwealth`, aggregated within household | None                      |

The household file supplies jointly held property, financial assets, and vehicles. For
private pensions, insurance wealth, and consumer debt, the pipeline sums person-level
amounts over observed household members.

Business assets and other real estate cannot be identified separately in the reduced
SOEP-Core wealth files used here. They are therefore absent from the component total.
The package also reports a **residual-inclusive scenario**,

$$
W^{R}_{i,2022} = W^{C}_{i,2022} + R_{i,2022},
$$

where $R$ reconciles the modelled components with the official SOEP net-wealth total in
the estimation sample. Because the relevant official total is available only in 2017,
the residual model cannot be evaluated out of time. The residual-inclusive measure is
therefore a sensitivity analysis, not the preferred outcome.

## Estimation sample and covariates

The estimation sample consists of households observed in the wealth waves 2002, 2007,
2012, and 2017. When an earlier wave is used as a validation outcome, that wave is
excluded from estimation. The prediction sample consists of SOEP households observed in
2022\.

Covariates are assembled at the person level and include demographic, labour-market,
income, and household characteristics, as well as wealth from the preceding wealth wave.
Each household is represented by its oldest observed member. Continuous covariates enter
linearly; categorical covariates are represented by indicator variables. Missing
continuous values are replaced by estimation-sample medians, and missing or previously
unseen categorical values map to the omitted all-zero category.

This setup identifies a projection under a stability assumption: after conditioning on
the included covariates and lagged wealth, the component-specific relationships
estimated through 2017 are assumed to remain informative for households in 2022. Changes
between 2017 and 2022 that are not captured by these covariates or the price indices
remain extrapolation error.

## Econometric specification

### Two-part models for wealth components

Each component is estimated with a two-part model. For component $c$, define the
ownership indicator

$$
D_{ic} = \mathbf{1}\{W_{ic} > 0\}.
$$

The extensive margin is a logit model,

$$
\Pr(D_{ic}=1\mid X_i) = \Lambda(X_i'\beta_c),
$$

and the intensive margin is estimated among owners by OLS after an inverse-hyperbolic-
sine transformation,

$$
\operatorname{asinh}(W_{ic}/a_c) = X_i'\gamma_c + u_{ic}.
$$

The scale $a_c$ is the median positive amount for component $c$. A component is omitted
if its estimation sample has fewer than five households, fewer than two owners, or no
variation in ownership.

Mortgage debt is coupled to owner-occupied housing. The algorithm first draws a property
donor and assigns that donor's mortgage to the same recipient. This preserves the
observed property-mortgage pair and prevents a mortgage from being assigned to a
predicted non-owner.

### Predictive mean matching

The fitted regressions are used to define donor proximity, not to extrapolate euro
amounts directly. Conditional on simulated ownership, the procedure compares a 2022
recipient's fitted intensive-margin index with the corresponding indices for historical
owners. It then samples an observed amount from the $k=10$ nearest donors. This is
predictive mean matching (PMM) on the asinh scale.

Historical asset values are converted to 2022 prices before donor matching where an
asset-specific index is available. Mortgage and consumer debt remain in nominal euros;
vehicles are also left unadjusted because no asset-price index is assigned to them.

Apart from the coupled property-mortgage draw, component donors are sampled separately
conditional on $X_i$. Thus, the accounting identity holds within each simulated
household, but the method does not preserve the unconditional multivariate distribution
of balance-sheet components.

### Reconciliation residual

For estimation observations with complete modelled components, the residual is

$$
R_i = W_i^{\text{official}} - \sum_c s_c W_{ic}.
$$

It captures omitted business assets and other real estate, but also any omitted
liabilities, measurement differences, and discrepancies between component definitions
and the official total. A separate linear model is fitted to this signed residual, and
PMM supplies a residual draw. Historical residuals are converted with a blended
property-equity index.

The residual outcome is available only for 2017. Its conditional distribution and its
transport from 2017 to 2022 are therefore not identified from repeated waves. The
residual-inclusive result should not be interpreted as a validated estimate of total
household net wealth.

## Statistical uncertainty

The package provides two different simulation products. They answer different questions
and should not be combined mechanically.

### Conditional PMM intervals

The main output uses 200 completed draws. For each household, the point estimate is the
median across draws and the reported lower and upper bounds are the central 90 percent
of those draws.

These bounds condition on the fitted coefficients and on SOEP implicate `a`. They
reflect simulated ownership and donor selection, including a new residual donor in each
residual-inclusive draw. They do not include coefficient uncertainty, uncertainty from
the other SOEP implicates, systematic forecast error from transporting the model to
2022, or the full dependence between components. They are therefore simulation intervals
conditional on the estimated model, not confidence intervals and not calibrated
prediction intervals.

Distributional statistics must be calculated within each completed draw. The summary
field `distribution_across_draws` follows this rule for the Gini coefficient, wealth
shares, and quantiles. Computing these statistics from household-specific medians would
distort the mass at zero and below zero.

### Projection replicates

A second output contains five complete component-total projections. Replicate $m$
combines:

- a Dirichlet$(1,\ldots,1)$ Bayesian-bootstrap reweighting of estimation households;
- a refit of the extensive- and intensive-margin models;
- one PMM draw; and
- one of the five SOEP donor implicates `a` through `e`.

The same bootstrap weights enter estimation and donor selection. Consequently, the
between-replicate variance combines coefficient, donor-selection, and donor-implicate
variation. With only five replicates, these sources cannot be estimated separately. The
optional layer-ablation exercise describes their relative importance but does not turn
the release into formal multiple imputation.

```{important}
The five projection columns are not the original SOEP implicates and do not support
Rubin's combining rules. They are draws from this projection algorithm, each using a
different SOEP implicate as donor data. The metadata therefore sets `rubin_valid` to
`false`.
```

The summary distinguishes the between-replicate standard deviation from the Monte Carlo
standard error of the replicate mean. The former describes variation generated by the
projection procedure; the latter equals that standard deviation divided by the square
root of the number of replicates and describes finite-simulation precision.

### Aggregate transport scenarios

Because no 2022 outcome is observed, the package does not estimate a 2017--2022
forecast-error distribution. Instead, it reports a separate macroeconomic sensitivity
analysis. Each replicate receives a common shock

$$
W'_{im} = A\sinh\!\left[\operatorname{asinh}(W_{im}/A)+\delta_m\right],
\qquad \delta_m \sim N(0,\sigma_T^2),
$$

where $A$ is the weighted median absolute wealth total. A common shock changes the
aggregate rather than averaging out across households. Since the inverse asinh
transformation is nonlinear, a mean-zero shock on the transformed scale is not
mean-neutral in euros.

Two values of $\sigma_T$ are reported:

- `historical_growth_dispersion` uses the dispersion of five-year log changes in the
  design-weighted official wealth aggregate;
- `excluding_largest_growth_step` omits the largest historical change.

Neither value is estimated from forecast errors. Both are scenario calibrations and must
remain separate from the no-transport projection replicates. In the production run,
these scenarios generate more aggregate variation than the projection replicates, so
conclusions about wealth levels are especially sensitive to the transport assumption.

## Validation evidence

The package implements two out-of-sample exercises.

1. **2017 component validation.** The models are estimated on 2002--2012 and used to
   predict 2017. Predicted component totals are compared with the sum of the six
   completed 2017 components. This evaluates the primary component outcome, not the
   residual-inclusive scenario.
1. **Rolling-origin validation.** Each of 2007, 2012, and 2017 is predicted using only
   earlier wealth waves. Predicted component totals are compared with the official
   net-wealth total using rank-based statistics. Because the two variables cover
   different balance-sheet concepts, this exercise assesses ordering rather than level
   agreement. Vehicles cannot be estimated in these folds because they are observed only
   in 2017.

The rolling-origin Spearman correlation is about 0.69 on average and 0.731 for the 2017
fold. Exact-quintile classification is about 0.40, with a mean absolute error of roughly
0.8 quintiles. In the 2017 temporal validation, the projected mean is approximately 33
percent above the observed mean, inequality is understated, and neither the zero mass
nor the negative tail is well reproduced.

These results support using the component total as a coarse continuous rank covariate.
They do not support inference about the 2022 wealth level, Gini coefficient, tail
shares, exact wealth groups, or household wealth mobility.

## Assumptions and limitations

- **No target-wave wealth data.** All 2022 values are extrapolated from historical
  relationships. Donor-distance diagnostics describe overlap but cannot validate the
  target-wave outcome distribution.
- **Conditional stability.** The projection assumes stable conditional relationships
  between covariates, ownership, and amounts after accounting for the selected price
  indices.
- **Incomplete balance sheet.** The primary outcome excludes business assets and other
  real estate. The residual-inclusive alternative is based on a single outcome wave.
- **Partial unit nonresponse.** Person-level pensions, insurance wealth, and consumer
  debt are summed only over household members represented in `pwealth`. An absent
  eligible adult causes downward measurement error in these components.
- **Conditional independence across components.** Except for housing and mortgage debt,
  donor amounts are sampled separately. This can alter tail dependence and measured
  inequality.
- **Limited time support.** Vehicles and the reconciliation residual are estimated from
  2017 only.
- **Nominal liabilities.** Mortgage and consumer debt are not converted to 2022 prices.
- **Single-implicate main intervals.** The 200-draw household intervals use SOEP
  implicate `a`; variation across implicates enters only the projection-replicate
  output.

The default run does not impose a PMM caliper. Its `out_of_support` block reports
nearest-donor distances without changing the sample or assigned values. If a caliper is
set programmatically, it becomes a common-support restriction on eligible donors and
raises an error when no donor is available; recipients are not silently dropped.

## Running the projection

Enable the opt-in tasks with either command:

```bash
SOEP_WEALTH_IMPUTATION=1 pixi run pytask
pixi run wealth
```

To run only the replicate task:

```bash
pixi run wealth -k task_wealth_imputation_replicates
```

Set `SOEP_WEALTH_LAYER_ABLATION=1` to add the computationally expensive layer-ablation
diagnostic to the replicate summary.

Outputs are written to `bld/wealth_imputation/`:

| File                                                               | Contents                                                        |
| ------------------------------------------------------------------ | --------------------------------------------------------------- |
| `household_wealth_2022_component_only.arrow`                       | Household medians and conditional PMM intervals for $W^C$       |
| `household_wealth_2022_residual_inclusive.arrow`                   | Scenario medians and intervals for $W^R$                        |
| `imputation_summary.json`                                          | Run settings, support diagnostics, and draw-level distributions |
| `backtest_2017_report.json`                                        | 2017 component-validation results                               |
| `transport_backtest_report.json`                                   | Rolling-origin rank-validation results                          |
| `household_wealth_2022_component_only_projection_replicates.arrow` | Five projection draws and transport-scenario draws              |
| `projection_replicates_summary.json`                               | Replicate variation, scenario calibration, and metadata guards  |

The replicate columns `component_only_net_wealth_2022_a` through `_e` contain the
no-transport projections. Columns beginning with
`transport_scenario_component_only_net_wealth_2022_` contain the primary transport
scenario. Select by the complete prefix to avoid mixing the two objects.
