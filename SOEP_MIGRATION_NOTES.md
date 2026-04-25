# SOEP version migration notes

A running log of what changed in this codebase to upgrade between SOEP-Core
versions. The intent is to make the next upgrade cheaper by keeping a precise
record of *what broke and how it was fixed*, not just the upstream release
notes.

For upstream notes see:
<https://git.soep.de/kwenzig/publicecoredoku/-/raw/master/meta/WhatsNew.md>

---

## V40 → V41

Upstream highlights: Sample S, M8d, M10 added. Many `pl`/`biol`/`childl`
variables split into `_v1`/`_v2` first-versions. `pgen` gains `pgreli`,
`pgnationiso3n`. `lfs` and `stib` gain new categories for phased retirement.
GRIPSTR `gs04`/`gs05` swap bug fixed. Weights now rounded to integers.

### Project-level changes

- `src/soep_preparation/config.py`: bumped `SOEP_VERSION` from `"V40"` to
  `"V41"`; extended `SURVEY_YEARS` upper bound from 2023 to 2024.
- `README.md`: replaced `V40` references with `V41`; corrected the stale
  "currently set up to work with version 38" sentence.
- `src/soep_preparation/create_metadata/variable_to_metadata_mapping.yaml`:
  regenerated to reflect new `survey_years: [2024]` entries on every variable
  observed in the new wave (run pytask, then
  `cp bld/variable_to_metadata_mapping.yaml src/soep_preparation/create_metadata/`).
  yamlfix re-formats it on the next prek run; that's fine.

### Per-module breakage and fixes

#### `clean_modules/biobirth.py`
- `cid` now contains `[-2] trifft nicht zu` placeholder rows in V41 (16,466
  rows in the current wave). The previous `float_to_int(cid,
  code_negative_values_as_na=False)` cast fails because the categorical
  mapping turns `-2` into the string label, leaving the column as object dtype.
  → switched `cid` to `object_to_int` (drops the placeholder rows to NA, which
  the existing `wide_to_long(...).dropna(...)` then filters out).
- `pid` is unaffected (no negatives), so it still uses
  `float_to_int(..., code_negative_values_as_na=False)`. Don't blindly switch
  both — `object_to_int` requires object dtype and would fail on the clean
  float column.

#### `clean_modules/pbrutto.py`
- `hid` is now delivered as `int32` (previously object). The
  `object_to_int(raw_data["hid"])` call hit the dtype guard.
  → switched to `apply_smallest_int_dtype(raw_data["hid"])`, matching how
  `pid` and `cid` are handled in the same file.

#### `clean_modules/pkal.py`
- `kal2j01_h` was removed from the V41 distribution (per the upstream
  "Removed from distribution" list).
  → switched to plain `kal2j01`. Positive label changed from `"[1] Ja"` to
  `"[1] genannt"`; renaming dict updated accordingly.
- `kal1b001`–`kal1b012` were split into `_v1`/`_v2` first-versions, with new
  `"[1] genannt"` labels and an added `"[8] Werkstatt für Behinderte"`
  category in `_v2`.
  → renamed the existing helper `_combine_versions_ft_employed_m` to
  `_combine_versions_employed_m` (now reused for kal1a and kal1b), and
  rewrote each `pt_employed_m_X` block to combine the two versions, mirroring
  the `ft_employed_m_X` pattern.
- `kal1n001`–`kal1n012` (minijob) gained value labels in V41 — values now
  arrive as the strings `"[1] genannt"` and `"[8] Werkstatt fuer behinderte
  Menschen"`. Previously the data was raw integer 1, so the V40 renaming
  `{1: "Minijob erwerbstätig"}` matched directly.
  → renaming dict updated to use string keys, and `[8] Werkstatt` is now
  mapped through (it would otherwise be silently dropped to NA, losing
  workshop-employment information).

#### `clean_modules/pl.py`
- The `ple0011_v1` … `ple0023_v1` labels changed from English `"[1] mentioned"`
  (V40) to German `"[1] genannt"` (V41). Affected 13 cleanings of the form
  `med_<condition>_pl`.
  → bulk replace of the renaming dict literal across the file.
- `plc0130_v2` (bezog Arbeitslosengeld m3-m5) lost the `"[2] Nein"` value label
  in V41, but the underlying data still contains raw `int 2` values. After
  categorical mapping, the column held a mix of `"[1] Ja"` strings and bare
  `int 2`s, which broke the bool conversion.
  → renaming dict expanded to `{"[1] Ja": True, 2: False}` (mixed-key dict is
  fine for `pd.Series.replace`).

### Verification trio (passed on V41 raw data)

- `pixi run -e py314 ty` → all checks passed
- `pixi run prek run --all-files` → all hooks pass except
  `no-commit-to-branch` (expected on `main`)
- `pixi run -e py314 tests -n 7` → 47 passed
- `pixi run pytask` → 62 tasks, all green

### Things deliberately *not* done

- New V41 files in `data/V41/` for which no `clean_modules/*.py` exists
  (`gkal`, `lkal`, `mihinc`, `plueckel`, `vpl`, `youthl`, `bnrecruit`,
  `borecruit`, `screeningl`, `boscreening`) were not added. Add a cleaner
  module only when a downstream consumer needs variables from them.
- New V41 variables on already-cleaned modules were *not* exposed:
  `pgreli` (aggregated religion), `pgnationiso3n` (nationality ISO3N),
  `hgeqppool` (swimming pool), `k_oorigin_c` (childl stateless). Add them in
  follow-up PRs as needed.
- `lfs` / `stib` (pgen): V41 added new categories `[7]` / `[14]` for phased
  retirement. The current cleaning code does not pin the category list, so
  the new categories flow through automatically — no code change required.
  If a downstream model needs to reason explicitly about phased retirement,
  add a derived variable rather than rewriting the renaming dict.
