# Concepts

## Waves, modules, variables

The SOEP is a panel survey that follows the same individuals over time. It is run in
yearly **waves**. Each wave consists of several survey **modules**, and the SOEP team
distributes each module as one **raw data file** (Stata `.dta`). For example
`hwealth.dta` holds household-level wealth; one of its variables, `p010ha`, is the market
value of the primary residence
([paneldata.org/soep-core/datasets/hwealth/p010ha](https://paneldata.org/soep-core/datasets/hwealth/p010ha)).

After conversion to pandas, each module is cleaned into typed, renamed **variables**.
Variables from several modules can be combined into a new module, named
`{module_1}_{module_2}` (e.g. `pequiv_pkal`).

## SOEP file names (#27)

SOEP module names combine a unit prefix with a content suffix:

| Affix | Meaning |
|---|---|
| `p…` | person level |
| `h…` | household level |
| `bio…` | biographical / retrospective |
| `kid…` | children |
| `…gen` | SOEP-*generated* (cleaned, consistent) variables |
| `…brutto` | fieldwork / gross sample data (contact and response status) |
| `…pathl` | the panel *path* (tracking) file, long format |
| `…l` | long format (the yearly questionnaire data) |
| `…equiv` | cross-nationally equivalent file (CNEF), income reference = previous year |
| `…kal` | activity calendar (Kalendarium) for the previous year |

The modules in this pipeline. Most follow the affix scheme above; `cirdef`, `design`,
and `health` are standalone files that do not:

| Module | Contents |
|---|---|
| `ppathl` / `hpathl` | person / household tracking (IDs, weights, status across waves) |
| `pgen` / `hgen` | generated person / household variables |
| `pbrutto` | fieldwork / response status per person |
| `pl` / `hl` | yearly person / household questionnaire data |
| `pequiv` | CNEF-harmonised person income and household aggregates |
| `pkal` | person activity / income calendar for the previous year |
| `pwealth` / `hwealth` | person / household wealth (multiply imputed) |
| `biobirth`, `bioparen`, `bioedu`, `biol` | fertility, parental, education, life-history biographies |
| `kidlong` | child-level information across waves |
| `cirdef` | household random-group / SOEP teaching-sample membership |
| `design` | survey sampling design (random group, stratum, sample) |
| `health` | derived SF-12 health module (physical / mental component summaries) |

For the full contents of any module or variable, see the online SOEP documentation; URLs
follow `https://paneldata.org/soep-core/datasets/{module}/{variable}`. The "Codebook
(PDF)" there is especially helpful.

## Index variables and why there is no index

Every cleaned module carries the identifier columns it has among `p_id`, `hh_id`,
`hh_id_original`, and `survey_year`. These align modules for merging.

These are kept as **plain columns, not a pandas index**. The merged dataset feeds
GETTSIM, which expects flat columns, and a flat layout keeps merges explicit. If you want
them as an index in your own analysis, set it yourself:

```python
df = df.set_index(["p_id", "survey_year"])
```

## Reference period

Whether a value describes the survey year, the previous calendar year, or the month
before the interview is recorded as per-variable metadata, not in the name. See
[Naming conventions → Reference period](naming_conventions.md) for the model and the
`ryear` / `rmonth` recipe.

## Looking up a variable

Search the project for a variable name. You will find it in at least two places: its
entry in `create_metadata/variable_to_metadata_mapping.yaml` (module, dtype, available
survey years) and its definition in `clean_modules/` or `combine_modules/`. The
definition shows the raw SOEP variable(s) it is built from, which you can then look up on
paneldata.org. The [Variables](variables.md) reference lists every variable with its
module, dtype, and available survey years.
