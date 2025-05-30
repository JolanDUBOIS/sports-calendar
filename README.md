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

### Executing Program

The project is run from the command line using Poetry to manage the environment. The basic command structure is:

```bash
poetry run python -m src [ARGUMENT]
```

Available arguments:

- `-udb`, `--update-database`
Updates the repository by querying APIs. This should be run at least 24 hours before running the selection to ensure data freshness.

- `-fu`, `--full-update`
Erases and updates the repository by querying APIs. This is a full reset and should only be run by administrators.

- `--run-selection`
Creates the personalized football calendar based on the user’s selection file.

## Authors

- Jolan Du Bois — mastr student in AI and football fan
- This project is a personal learning exercise and hobby.

## Planned Features

- Automated deletion of old data and metadata from the repository
- Complete test coverage, especially for boundary value cases
- Automated deletion of old logs
- Add developer tools for the data pipeline (e.g., add or remove models or layers, build models)
- Improve error handling throughout the entire data pipeline, including boundary and edge cases
