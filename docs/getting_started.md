# Getting started

## Install

Install [pixi](https://prefix.dev/docs/pixi/overview#installation) if you do not have it.
It manages the environment and tasks.

## Add the raw data

The SOEP data is access-restricted and is **not** shipped with this project. Inside
`soep_preparation/data`, fill the version folder (e.g. `V41`) with the raw `.dta` files
downloaded from the SOEP. The project is currently set up for SOEP-Core **version 41**.

## Build

```console
$ pixi run pytask
```

pytask resolves the dependency graph and runs every stage: convert Stata → clean modules
→ combine modules → build the metadata catalogue. On success the example merged dataset
is written to the project root.

## Test

```console
$ pixi run -e py314 tests
```

## Build the docs

```console
$ pixi run build-docs   # render to HTML
$ pixi run view-docs    # live preview
```
