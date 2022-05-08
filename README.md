# Oceans 1876

## Data Pipeline

> Requires python >= 3.10

### Setup

- After cloning the repo, run `git submodule init` and `git submodule update` to fetch the data
  from [https://github.com/oceans-1876/challenger-data](https://github.com/oceans-1876/challenger-data).
- Install [gnfinder](https://github.com/gnames/gnfinder) v0.19.
- Install [gnverifier](https://github.com/gnames/gnverifier) v0.9.
- Install [Poetry](https://github.com/python-poetry/poetry) on your system, either globally or in your virtual environments (the former is preferred).
- Run `poetry install` to install the project dependencies.
- If you just need to run the extractor and do not need the dev dependencies, you can run `poetry install --no-dev`.
- If you are going to do development work, run `pre-commit install` in the project root.

### How to use

> All scripts must be run from `workflows/` directory.

| Command                              | Description                                                |
|--------------------------------------|------------------------------------------------------------|
| process_stations.py                  | Updates stations text and species                          |
| process_summary_report_species_index | Extracts the species mentioned in the summary report index |
| update_datasource.py                 | Updates Global Names data source info                      |
