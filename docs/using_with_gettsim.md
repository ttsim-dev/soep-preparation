---
kernelspec:
  name: python3
  display_name: Python 3
---

# Using SOEP data with GETTSIM

GETTSIM expects its data inputs as a flat table whose column labels are
double-underscore qualified names (qnames), for example `wohnen__wohnfläche_hh` or
`sozialversicherung__rente__jahr_renteneintritt`. The `soep_preparation.gettsim_inputs`
package maps the project's cleaned SOEP final variables onto GETTSIM nodes and emits a
GETTSIM-ready dataset.

:::{caution}
This mapping is a **discovery aid, not a ready-to-use input pipe**. For each GETTSIM
concept it records the SOEP final variable that comes *closest* — but a survey proxy
routinely differs in reference period, aggregation level, or exact definition, and
feeding it to GETTSIM directly can be *worse* than letting GETTSIM compute the node. For
an actual GETTSIM run, drive inputs through **gettsim-personas** rather than handing
GETTSIM these columns wholesale; use the table here to discover which SOEP variable
approximates which qname, then decide per node via the DAG.
:::

## The mapping

A SOEP variable can stand in for two kinds of GETTSIM node:

- a **root input** — a node GETTSIM declares with `@policy_input()` and cannot compute,
  so it must be supplied; and
- an internally **computed** node — supplying data there *overrides* GETTSIM's own
  calculation (see the caution below).

The mapping lists either kind: it pairs a GETTSIM qname with the SOEP final variable that
reasonably approximates it, rather than restricting itself to root inputs.

`soep_preparation.gettsim_inputs.mapping.BASE_MAPPING` is the date-invariant union of
those pairings. Each value is the SOEP final variable that supplies the qname, or `None`
where no clean source exists (with an entry in `mapping.GAP_NOTES`). The SOEP→qname
pairing does not change with the policy date — GETTSIM's qname namespace is stable; nodes
only activate and deactivate.

What *does* change by policy date is **which** qnames are part of GETTSIM at all: rules
expire (Erziehungsgeld before 2007, Altersrente für Frauen from 2018) and rules begin
(Grundrente from 2021, Bürgergeld from 2023). `get_soep_to_gettsim(policy_date)` returns
the slice of `BASE_MAPPING` whose qnames are in GETTSIM's policy environment at that date.
The per-date scope is generated offline from GETTSIM and committed as
`gettsim_input_scope.json`, so this package needs no GETTSIM dependency at runtime.

## The mapping for a policy date

The cells below resolve the mapping for one `policy_date` — the qnames GETTSIM uses at
that date, which SOEP variable supplies each, and why the rest are unmapped. The rendered
page bakes in the result for the date set here; to explore a different date, change
`policy_date` and re-run the page locally with `pixi run view-docs` (or via live compute
where it is enabled).

```{code-cell} python
import datetime

import pandas as pd

from soep_preparation.gettsim_inputs.mapping import (
    GAP_NOTES,
    get_soep_to_gettsim,
    period_for_date,
)

# Punch in a policy date:
policy_date = datetime.date(2024, 1, 1)

period = period_for_date(policy_date)
mapping = get_soep_to_gettsim(policy_date)
mapped = {q: v for q, v in mapping.items() if v is not None}

caveat = (
    " — low confidence (GETTSIM pre-2015 environments are incomplete)"
    if period.low_confidence
    else ""
)
print(
    f"Policy date {policy_date.isoformat()}: period "
    f"{period.start_date.isoformat()} to {period.end_date.isoformat()}{caveat}.\n"
    f"{len(mapping)} qnames in scope; {len(mapped)} supplied from SOEP, "
    f"{len(mapping) - len(mapped)} unmapped."
)
```

Supplied from SOEP at that date:

```{code-cell} python
pd.DataFrame(
    sorted(mapped.items()), columns=["GETTSIM qname", "SOEP final variable"]
)
```

Unmapped at that date (no clean SOEP source), with the reason where one is recorded:

```{code-cell} python
pd.DataFrame(
    [
        {"GETTSIM qname": q, "reason": GAP_NOTES.get(q, "")}
        for q in sorted(q for q, v in mapping.items() if v is None)
    ]
)
```

## Overriding computed nodes — a caution

Supplying a column for a *computed* node (one GETTSIM would otherwise calculate) replaces
that calculation and prunes its subtree; GETTSIM warns about the override via
`functions_and_data_columns_overlap` unless you pass `include_warn_nodes=False`. That is
sometimes what you want, but a crude survey proxy can be *worse* than GETTSIM's own
computation. So do not hand GETTSIM every column the mapping can produce: prefer letting
it compute a node unless the SOEP value is genuinely the better measure, and use the DAG
to decide. This is why the computed-amount candidates (`kindergeld__betrag_m`,
`wohngeld__betrag_m_wthh`, `unterhaltsvorschuss__betrag_m`, …) are listed but left `None`
— the available SOEP amounts are at a different aggregation level or reference period than
the node, so feeding them would degrade the result rather than improve it.

## The build helper

`build_gettsim_inputs(final_dataset, mapping)` takes a SOEP final dataset
(single-underscore SOEP column names) and a mapping — either
`get_soep_to_gettsim(policy_date)` for a date-specific build or `BASE_MAPPING` for the
date-invariant union — and returns a flat frame with GETTSIM qnames as column labels. It
keeps the index variables (`p_id`, `hh_id`, `survey_year`) and renames every mapped SOEP
column that is present; mapped qnames whose SOEP column is absent are skipped, so the
result holds exactly the inputs the data can supply.

Build from a dataset restricted to a **single reference date** — one policy date's worth
of rows and columns. Do not feed the raw, multi-wave output of `task_create_soep_dataset`
here: GETTSIM evaluates one policy date at a time, so a frame mixing survey years yields
meaningless inputs.

```python
from soep_preparation.gettsim_inputs.build import build_gettsim_inputs
from soep_preparation.gettsim_inputs.mapping import get_soep_to_gettsim

# `get_soep_to_gettsim` accepts a `datetime.date` or an ISO date string ("YYYY-MM-DD").
mapping = get_soep_to_gettsim("2024-01-01")
gettsim_inputs = build_gettsim_inputs(final_dataset, mapping=mapping)
```

`fail_if_required_inputs_missing(required_inputs, available_columns, mapping)` reports,
against a concrete set of required GETTSIM inputs, which are unmapped or whose mapped SOEP
column is absent — for visibility before handing data to GETTSIM.

## Regenerating

The pytask task `task_build_gettsim_inputs` merges the mapped SOEP variables into a final
dataset, renames them, and writes a GETTSIM-ready
`bld/gettsim_inputs/gettsim_inputs.arrow`. `task_write_mapping_report` writes
`bld/gettsim_inputs/mapping_report.json` with the union coverage and a per-period
breakdown. Run:

```bash
pixi run pytask
```

The per-policy-date scope (`gettsim_input_scope.json`) is regenerated from GETTSIM by
`_generate_input_scope.py`, run from the dev-gettsim workspace root where GETTSIM is
importable. `tests/gettsim_inputs/test_input_scope.py` regenerates it and fails if it has
drifted (skipped where GETTSIM is not installed).
