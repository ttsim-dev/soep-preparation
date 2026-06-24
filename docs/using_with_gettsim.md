# Using SOEP data with GETTSIM

GETTSIM expects its data inputs as a flat table whose column labels are
double-underscore qualified names (qnames), for example `wohnen__wohnfläche_hh` or
`sozialversicherung__rente__jahr_renteneintritt`. The `soep_preparation.gettsim_inputs`
package maps the project's cleaned SOEP final variables onto those inputs and emits a
GETTSIM-ready dataset.

## The mapping

`soep_preparation.gettsim_inputs.mapping.SOEP_TO_GETTSIM` is an immutable mapping keyed
by GETTSIM input qname. Each value is either the name of the SOEP final variable that
supplies it, or `None` where no SOEP source exists yet:

```python
from soep_preparation.gettsim_inputs.mapping import SOEP_TO_GETTSIM

SOEP_TO_GETTSIM["wohnen__wohnfläche_hh"]  # -> "living_space_hh"
SOEP_TO_GETTSIM["lohnsteuer__steuerklasse"]  # -> None (no SOEP source)
```

The keys cover the union of GETTSIM data inputs across the SOEP survey range. The set of
required inputs is policy-date dependent: rules that expire (such as Altersrente für
Frauen) drop their inputs from later policy dates, so the mapping is a superset of any
single date's requirements. The mapping is deliberately conservative — only 1:1 or
obviously derivable matches carry a value. Every non-`None` value is a real key in
`create_metadata/variable_to_metadata_mapping.yaml`, asserted by a test. Anything
uncertain stays `None` with a `# TODO:` note describing the gap.

## Current coverage

Of the GETTSIM data inputs, the following are mapped (the two index variables `p_id` and
`hh_id` are carried through verbatim rather than renamed):

| GETTSIM input qname | SOEP final variable |
| --- | --- |
| `p_id` | `p_id` |
| `hh_id` | `hh_id` |
| `geburtsjahr` | `birth_year` |
| `arbeitsstunden_w` | `actual_working_hours_w` |
| `behinderungsgrad` | `disability_degree` |
| `wohnen__wohnfläche_hh` | `living_space_hh` |
| `wohnen__heizkosten_m_hh` | `heating_costs_m_hh` |
| `wohnen__bruttokaltmiete_m_hh` | `rent_minus_heating_costs_m_hh` |
| `familie__p_id_ehepartner` | `partner_p_id` |
| `familie__p_id_elternteil_1` | `mother_p_id` |
| `einnahmen__bruttolohn_m` | `gross_labor_income_previous_month_m` |
| `einkommensteuer__einkünfte__aus_selbstständiger_arbeit__betrag_y` | `earnings_from_self_employment_y` |
| `unterhalt__tatsächlich_erhaltener_betrag_m` | `kindesunterhalt_received_m` |
| `sozialversicherung__rente__jahr_renteneintritt` | `first_pension_receipt_year` |

## Known gaps

The large majority of GETTSIM inputs are not yet supplied from SOEP and stay `None`.
Notable groups:

- **Pension biography** (`sozialversicherung__rente__entgeltpunkte*`,
  `..._monate`, `grundrente__*`): not in the survey; these need the FDZ-RV record
  linkage reachable via `rv_id`.
- **Tax bracket** (`lohnsteuer__steuerklasse`) and joint-assessment status
  (`einkommensteuer__gemeinsam_veranlagt`): no direct SOEP source.
- **Unemployment-insurance history**
  (`sozialversicherung__arbeitslosen__monate_*`): not assembled.
- **Only a mother pointer in SOEP**: `familie__p_id_elternteil_1` maps to `mother_p_id`,
  but there is no father pointer, so `familie__p_id_elternteil_2` stays `None`.
- **Derivable but not yet wired**: `weiblich` (from `gender`), `wohnort_ost_hh` (from the
  federal-state variable), `wohnen__bewohnt_eigentum_hh` (from `rented_or_owned`),
  `sozialversicherung__pflege__beitrag__hat_kinder` (from a number-of-children variable).
  Each carries an entry in `mapping.GAP_NOTES`.
- **Unit/timing mismatches** kept honest as `None`: monthly private-pension inputs where
  SOEP records annual amounts; household-level capital and rental income where GETTSIM
  expects an individual figure.

## The build helper

`build_gettsim_inputs` takes a SOEP final dataset (single-underscore SOEP column names)
and returns a flat frame with GETTSIM input qnames as column labels. It keeps the index
variables (`p_id`, `hh_id`, `survey_year`) and renames every mapped SOEP column that is
present; mapped inputs whose SOEP column is absent are skipped, so the result holds
exactly the inputs the data can supply.

```python
from soep_preparation.gettsim_inputs.build import build_gettsim_inputs

gettsim_inputs = build_gettsim_inputs(final_dataset)
```

`fail_if_required_inputs_missing(required_inputs, available_columns)` reports, against a
concrete set of required GETTSIM inputs, which are unmapped or whose mapped SOEP column
is absent — for visibility before handing data to GETTSIM.

## Regenerating the dataset

The pytask task `task_build_gettsim_inputs` merges the mapped SOEP variables into a final
dataset, renames them, and writes a GETTSIM-ready `bld/gettsim_inputs/gettsim_inputs.arrow`.
`task_write_mapping_report` writes `bld/gettsim_inputs/mapping_report.json` with the
mapped/unmapped counts and the full table. Run:

```bash
pixi run pytask
```
