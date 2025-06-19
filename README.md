# Football Calendar

Football Calendar is a command-line tool designed for football fans who want to keep track of upcoming matches without the hassle of checking schedules manually. By defining their preferences in a simple configuration file, users get a personalized football events calendar automatically created and synced with their Google Calendar. This way, they never miss a game that matters to them.

## Description

Football Calendar is a personal project written in Python that automates the creation of a customized football events calendar. It retrieves and processes data from multiple APIs and web scrapers, including sources like football-data, ESPN, Football Ranking, and LiveSoccerTV. The focus is on upcoming matches across various leagues and competitions, with data carefully parsed, normalized, and cleaned to ensure accuracy.

A key part of the processing pipeline is matching team names and other entities across different data sources to unify information (e.g., recognizing that "PSG" and "Paris Saint Germain" are the same team). Although still under development, the data will eventually be stored in a relational database organized around teams, competitions, matches, and standings following a structured ETL pipeline.

Users define their football preferences in a simple JSON or YAML file, specifying which teams, competitions, or match conditions they want to follow. Based on this selection, the project automatically generates and syncs a personalized calendar with Google Calendar, making it easy for fans to never miss a game.

Built as a learning tool and for personal use, Football Calendar is an evolving project that combines data engineering, sports fandom, and practical automation.

## Getting Started

### Dependencies

This project uses [Poetry](https://python-poetry.org/) for dependency management. To install all required packages, first make sure Poetry is installed, then run:

```bash
poetry install
```

This will create a virtual environment and install all dependencies listed in `pyproject.toml`.

### Installing

TODO

## Running the Project

The project is run from the command line using Poetry to manage the environment. The basic command structure is:

```bash
poetry run python -m src [COMMAND] [OPTIONS]
```

The available arguments are:

### `run-pipeline`
Runs the data pipeline that fetches, processes, and stores football data from various sources (e.g. scraping, APIs, normalization, writing to internal DB).

```bash
poetry run python -m src run-pipeline [OPTIONS]
```

**Options:**
- `--repo [test|prod]` – Specify the repository environment (default: `test`)
- `--stage [landing|intermediate|staging|production]` – Run a specific pipeline stage
- `--model MODEL_NAME` – Specify a model to run (requires `--stage`)
- `--manual` – Enable manual mode (default is automatic)
- `--reset` – Reset the file(s) and their metadata before running the pipeline
- `--dry-run` – Simulate the pipeline without making changes
- `--verbose` – Enable verbose logging

**Examples:**
```bash
poetry run python -m src run-pipeline
poetry run python -m src run-pipeline --repo prod --stage landing --dry-run --verbose
poetry run python -m src run-pipeline --manual --stage intermediate --model espn_matches
```

---

### `run-validation`
Performs data validation checks (e.g. consistency, completeness, structural correctness).

```bash
poetry run python -m src run-validation [OPTIONS]
```

**Options:**
- `--repo [test|prod]` – Specify the repository environment (default: `test`)
- `--stage [landing|intermediate|staging|production]` – Run validation on a specific stage
- `--model MODEL_NAME` – Specify a model to validate (requires `--stage`)
- `--raise-on-error` – Raise exception if validation fails
- `--verbose` – Enable verbose logging

**Examples:**
```bash
poetry run python -m src run-validation --stage production --raise-on-error
poetry run python -m src run-validation --repo test --stage intermediate
```

---

### `run-selection`
Generates a personalized football calendar and syncs it with Google Calendar.

```bash
poetry run python -m src run-selection
```

**Options:**
- `--name` – Name of the selection to run
- `--dry-run` – Simulate the selection without making changes (in the google calendar)
- `--verbose` – Enable verbose logging

---

### `test`
Runs a specific test routine.

```bash
poetry run python -m src test TEST_NAME
```

---

### `clean` *(not yet implemented)*
Cleans the specified repository stage.

```bash
poetry run python -m src clean [--repo REPO] [--stage STAGE]
```

---

### `revert` *(not yet implemented)*
Reverts the data repository to a previous state by run ID.

```bash
poetry run python -m src revert [RUN_ID]
```

---

### Valid Values

- **Stages**: `landing`, `intermediate`, `staging`, `production`
- **Repos**: `test`, `prod`

---

### 💡 Tips

- `--model` **requires** `--stage` to be specified.
- Default `repo` is `test` unless overridden.
- All commands support `--help` for option hints.

```bash
poetry run python -m src run-pipeline --help
```

## Authors

- Jolan Du Bois — master student in AI and football fan
- This project is a personal learning exercise and hobby.

## Planned Features

- Automated deletion of old data and metadata from the repository
- Complete test coverage, especially for boundary value cases
- Automated deletion of old logs
- Add developer tools for the data pipeline (e.g., add or remove models or layers, build models)
- Improve error handling throughout the entire data pipeline, including boundary and edge cases
