# Naming conventions

Final variable names follow one rule, with one explicit exception list.

## The rule: English by default, German for named programmes

**A final variable name is English by default.** A token stays **German** only when it
is the proper noun of a German statutory programme, benefit, or pension pillar whose
English translation would be ambiguous or would drift over time. German stems match
[GETTSIM](https://github.com/ttsim-dev/gettsim)'s spelling (so the GETTSIM-input
variables are drop-in) but stay **flat single-underscore** — soep-preparation does not
use GETTSIM's `__` qualified names.

### German allow-list

The only tokens allowed to remain German:

- **Transfers:** `kindergeld`, `kinderzuschlag`, `wohngeld`, `arbeitslosengeld`,
  `arbeitslosengeld_2`, `arbeitslosenhilfe`, `grundsicherung_im_alter`,
  `grundsicherung`, `sozialhilfe`, `hilfe_zum_lebensunterhalt`, `bafög`, `pflegegeld`,
  `mutterschaftsgeld`, `mutterschutz`, `elternzeit`, `elterngeld`,
  `kriegsopferversorgung`, `betreuungsgeld`, `eigenheimzulage`, `vorruhestandsgeld`
- **Maintenance (Unterhalt):** `kindesunterhalt`, `ehegattenunterhalt`, `unterhalt`,
  `unterhaltsvorschuss` — the German *Unterhalt* terms are kept; English "support" /
  "maintenance" / "alimony" are not interchangeable, so the legal term stays German
- **Pension pillars / schemes:** `gesetzliche_rente`, `knappschaftliche_rente`,
  `riester_rente`, `betriebliche_altersversorgung`, `berufsständische_altersvorsorge`,
  `private_altersvorsorge`, `private_rente`, `beamtenpension`,
  `alterssicherung_landwirte`, `gesetzliche_unfallversicherung`, `altersteilzeit`
- **Named legal terms / institutions:** `werkstatt_für_behinderte` (Werkstatt für
  behinderte Menschen, the SGB IX sheltered-employment institution — no faithful
  one-word English equivalent)

Everything else is English. Common German scaffolding and its English replacement:

| German | English |
|---|---|
| `bezieht` / `bezog` | received |
| `aktuell` | current |
| `anzahl_monate` | number_of_months |
| `im_letzten_monat` | last_month |
| `eingezahlt` | paid_in |
| `nach_steuern` / `vor_steuern` | after_tax / before_tax |
| `brutto` | gross |
| `hinterbliebene` | survivor |
| `einkommen` / `einkünfte` | income / earnings |
| `erwerbstätig` | employed |
| `betriebsgröße` | firm_size |
| `gemeldet` | registered |

## PEP 8

Names are `snake_case` and must not start with a digit (`1989_place_of_residence` →
`place_of_residence_1989`). Umlauts and ß are allowed, consistent with GETTSIM (which
uses e.g. `bürgergeld`).

## Suffix convention

Order: `<german_stem>_<english_modifiers>_<time>_<aggregation>_<source>`.

- **time:** `_m` (monthly), `_y` (annual)
- **aggregation:** `_hh` (household)
- **source-module disambiguation:** `_hl`, `_pequiv`, … when the same concept comes from
  more than one module

The **reference period is not encoded in the name** — see below.

Examples:

- `kindergeld_currently_received_hh`
- `arbeitslosengeld_2_number_of_months_hh`
- `gesetzliche_rente_survivor_y`
- `income_after_tax_y_hh`

## Reference period: it lives in metadata, not the name

A value's reference period — does it describe the survey year, the previous calendar
year, or the month before the interview? — is a property of *each variable*, not of the
row: one `(p_id, survey_year)` row holds current-status variables, previous-month
amounts, and previous-calendar-year income at once. So it is recorded as **per-variable
metadata**, and you turn it into `ryear` / `rmonth` columns yourself when you need them.

The SOEP measurement modes (per the
[SOEP companion](https://companion.soep.de/Survey%20Design/Survey%20Concepts%20and%20Modes.html)
and [paneldata.org](https://paneldata.org/soep-core/)):

- **Point-in-time, survey year `t`:** current-status questions (current employment,
  satisfaction, current benefit receipt), measured at the interview.
- **Recent past, ≈ the month before the interview (still `t`):** "current" monthly
  amounts; e.g. `pglabnet` ("Current Net Labor Income") is the *previous month*'s income.
- **Previous calendar year `t − 1`:** annual flows (`_y`), the `pkal` 12-month calendar
  ("January through December of last year"), and number-of-months counts. This is the
  SOEP income reference year.

The cleaning pipeline stores every variable under `survey_year = syear` (the interview
wave `t`) with no year arithmetic, so the reference period is recoverable only from the
metadata `reference` field:

- `current` ⇒ `ryear = survey_year`
- `previous_year` ⇒ `ryear = survey_year − 1`
- `previous_month` ⇒ `rmonth = interview_month − 1`, rolling January back to December of
  `survey_year − 1`
- `time_invariant` ⇒ no reference period (birth year, parents)

### Recipe: build `ryear` / `rmonth`

The `add_reference_columns` helper turns `survey_year` into the reference year and month,
with the January→December rollover for `previous_month`:

```python
from soep_preparation.utilities.reference_period import add_reference_columns

# `reference` for a variable comes from variable_to_metadata_mapping.yaml.
periods = add_reference_columns(
    survey_year=df["survey_year"],
    reference="previous_year",  # or "current" / "previous_month" / "time_invariant"
    interview_month=df["month_interview"],  # only needed for "previous_month"
)
df["ryear"] = periods["ryear"]
df["rmonth"] = periods["rmonth"]
# Then merge on `ryear` (e.g. to align income to its calendar year for GETTSIM) or keep
# `survey_year` — your choice.
```

## Enforcement

A test fails any final variable name (checked against the generated metadata inventory
**and** the `out["…"]` assignments via AST) that contains a German-scaffolding token
outside the allow-list, starts with a digit, or is not Unicode-aware `snake_case`. The
allow-list is matched on exact phrase spans, not substrings, so
`bezieht_kindergeld_aktuell` still fails on `bezieht`/`aktuell`.
