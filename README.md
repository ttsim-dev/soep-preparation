# SOEP data preparation for use with research projects

[![image](https://img.shields.io/github/actions/workflow/status/ttsim-dev/soep-preparation/main.yml?branch=main)](https://github.com/ttsim-dev/soep-preparation/actions?query=branch%3Amain)
[![image](https://codecov.io/gh/ttsim-dev/soep-preparation/branch/main/graph/badge.svg)](https://codecov.io/gh/ttsim-dev/soep-preparation)
[![docs](https://readthedocs.org/projects/soep-preparation/badge/?version=latest)](https://soep-preparation.readthedocs.io/en/latest/)

Prepare German Socio-Economic Panel (SOEP-Core) survey data into typed, cleaned,
documented variables — for research and as inputs to
[GETTSIM](https://gettsim.readthedocs.io/en/stable/). The pipeline casts each raw
variable to an adequate dtype, converts SOEP missing codes to `pd.NA`, combines related
variables across modules, and exposes a metadata catalogue plus a helper for assembling
a final dataset.

## Quickstart

Install [pixi](https://prefix.dev/docs/pixi/overview#installation). Place the raw SOEP
`.dta` files in `soep_preparation/data/V41` (the project targets SOEP-Core version 41),
then:

```console
$ pixi run pytask          # build the pipeline
$ pixi run -e py314 tests  # run the tests
$ pixi run build-docs      # render the documentation
```

## Documentation

The rendered documentation is hosted at
[soep-preparation.readthedocs.io](https://soep-preparation.readthedocs.io/en/latest/).
The sources live in [`docs/`](docs/) — build them with `pixi run build-docs`, preview
live with `pixi run view-docs`:

- **Getting started** — install and run the pipeline.
- **Concepts** — waves, modules, SOEP file names, index variables, reference periods.
- **Scope** — what the project does and does not do.
- **Creating a dataset** — assemble variables with `create_final_dataset`.
- **Extending** — add a variable, a derived variable, or a whole module.
- **Naming conventions** — the German/English rule and reference periods.
- **Variables** — the full catalogue of final variables.

## Credits

This project was created with [cookiecutter](https://github.com/audreyr/cookiecutter)
and the
[econ-project-templates](https://github.com/OpenSourceEconomics/econ-project-templates).
