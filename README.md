# Sports Calendar

Sport Calendar is a command-line tool designed for sports fans who want to keep track of upcoming matches and events without the hassle of checking schedules manually. By defining their preferences in a simple configuration file, users get a personalized sports events calendar automatically created and synced with their Google Calendar. This way, they never miss a game or event that matters to them.

## Scope & Limitations

Sport Calendar is a personal project written in Python that automates the creation of a customized sports events calendar. It retrieves and processes data from the unofficial ESPN API (#TODO - Link to the "documentations"). The focus is on upcoming matches and events across various sports, leagues, and competitions, with data carefully parsed, normalized, and cleaned to ensure accuracy.

Users define their sports preferences in a simple YAML file, specifying which teams, players, competitions, or match conditions they want to follow. Based on this selection, the project automatically generates and syncs a personalized calendar with Google Calendar, making it easy for fans to never miss a game or event.

Built as a learning tool and for personal use, Sport Calendar is an evolving project that combines data engineering, sports fandom, and practical automation.

Currently, only **football** and **F1** events are supported, but the architecture is designed to accommodate more sports in future releases.

## Features

- Retrieves upcoming sports events from the unofficial ESPN API
- Normalizes and cleans raw data to ensure consistent and reliable event information
- Allows users to define custom sports preferences via YAML configuration files
- Generates personalized calendars based on user-defined selections
- Synchronizes events with Google Calendar using the Google Calendar API
- Supports multiple calendars through configurable calendar identifiers

## User Requirements

To install and run Sports Calendar, the following requirements must be met:

- Python **3.11+**
- [`pipx`](https://pipx.pypa.io/stable/) installed

## Installing

Install Sports Calendar globally using `pipx`:

```bash
pipx install git+https://github.com/JolanDUBOIS/sports-calendar.git --python python3.11
```

To reinstall or update an existing installation:

```bash
pipx install --force git+https://github.com/JolanDUBOIS/sports-calendar.git
```

After installation, the `sports-calendar` command will be available system-wide.
Run `sports-calendar --help` to see available commands.

## Configuration

Before using Sports Calendar, the application must have a configuration directory set up. Start by running:

```bash
sports-calendar init
```
This will create a template `logging.yml` file in the user configuration directory **only if it does not already exist**. For normal usage, no modifications are required. Advanced users can customize logging; see the [Python logging documentation](https://docs.python.org/3/library/logging.html)
for details.

The application also requires the following configuration items, each documented separately:
- Google API credentials
- One or more Google Calendar IDs
- At least one selection file

Refer to the dedicated guides for setup:
- [Google credentials setup](docs/google-credentials.md)
- [Google Calendar IDs](docs/google-calendars.md)
- [Selections format](docs/selections.md)

**Note for collaborators:** If someone already has Sports Calendar configured, they can help you get started. You provide your selection (as a YAML file or description), and they will:

1. Add the selection to their `selections/` folder.
2. Create a dedicated Google Calendar for it.
3. Add the calendar to their `.secrets/gcal_ids.yml`.
4. Share the calendar with you.

You can then link the shared calendar to your Google account and start using Sports Calendar without setting up credentials or configuration yourself.

## Usage 

All commands can be listed by running:

```bash
sports-calendar --help
```

Below is a brief overview of the main commands.

### `sports-calendar sync-db`

```bash
sports-calendar sync-db
```

Fetches the latest sports event data from the configured sources and updates the local database.

**Expected outcome:** new or updated event data stored in the runtime data directories.

**Advanced options (mostly for developers or power users):**
- `--stage`: Run the pipeline on a specific stage (`landing`, `intermediate`, `staging`)
- `--model`: Specify a model to run (requires `--stage`)
- `--reset`: Reset data for the selected stage before processing
- `--dry-run`: Run the pipeline without modifying files

### `sports-calendar sync-calendar`

```bash
sports-calendar sync-calendar <calendar-key>
```

Updates the specified Google Calendar with events from the corresponding selection. All future events are first removed and then the new events are added; some events may be deleted and re-added as part of this process.

**Expected outcome:** the calendar reflects the latest events for the selection.

**Advanced option (mostly for developers or power users):**
- `--dry-run`: Run the selection process without synchronizing with the google calendar (to check validity of the selection file for instance)

### `sports-calendar clear-calendar`

```bash
sports-calendar clear-calendar <calendar-key> [--scope all|future|past] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD]
```

Removes all events from the specified Google Calendar.

**Expected outcome:** all events in the defined scope are deleted.

**Notes:**
- Default behavior deletes all events.
- `--scope` can be used to limit deletion to all, future, or past events.
- `--date-from` and `--date-to` refine the date range (used only if `--scope` is not specified).

### `sports-calendar validate-db`

Currently not implemented.

## Runtime Directory Structure

### Configuration Locations

Configuration files are stored in the user config directory:

- **Linux**: `~/.config/sports-calendar/`
- **macOS**: `~/Library/Application Support/sports-calendar/`
- **Windows**: `C:\Users\<your-username>\AppData\Roaming\sports-calendar\`

### Runtime Storage Layout

Sports event data is stored in the user data directory:

- **Linux**: `~/.local/share/sports-calendar/`
- **macOS**: `~/Library/Application Support/sports-calendar/data/`
- **Windows**: `C:\Users\<your-username>\AppData\Local\sports-calendar\data\`

Future releases may allow overriding this location via the CLI.

### Logs

Application logs are stored in the user state directory:

- **Linux**: `~/.local/state/sports-calendar/logs/`
- **macOS**: `~/Library/Application Support/sports-calendar/logs/`
- **Windows**: `C:\Users\<your-username>\AppData\Local\sports-calendar\logs\`

### Tree Overview

```text
<user_config_dir>/sports-calendar/
├── .credentials
│   ├── client_secret.json
│   └── token.json
├── .secrets
│   └── gcal_ids.yml
├── selections
│   ├── main-selection.json
│   └── second-selection.yml
├── logging.yml
<user_data_dir>/sports-calendar/
├── landing       # Raw data from sources
│   ├── sportA
│   │   ├── .meta/
│   │   └── espn_DATA.json
│   └── sportB
├── intermediate # Processed and normalized data
│   ├── sportA
│   │   ├── .meta/
│   │   └── espn_DATA.csv
│   └── sportB
├── staging      # Final stage for calendar synchronization
│   ├── sportA
│   │   ├── .meta/
│   │   └── espn_DATA.csv
│   └── sportB
<user_state_dir>/sports-calendar/logs/
└── sports-calendar-YYYY-mm-DD.log      # All logs
```

## Development

These instructions are for developers or contributors who want to modify, extend, or experiment with the project.

### Requirements

- Python **3.11+** ofc
- [Poetry](https://python-poetry.org/) for dependency management (tested with version 2.2.1; other recent versions likely work).
- Docker (optional, for containerized execution).

### Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/JolanDUBOIS/sports-calendar.git
cd sports-calendar
poetry install
```

### Configuration for Development

The `./config/` folder already contains:
- `logging.yml` — logging configuration
- `.secrets/` — empty directory for calendar IDs
- `.credentials/` — empty directory for Google API credentials
- `selections/dev.yml` — example selection file (quite detailed)

Environment variables (optional for development):
- `DB_DIR` — override the default data directory (`./data`)
- `FOOTBALL_DATA_API_TOKEN` — required only if using the football-data.org client

### Commands

#### Local (Python + Poetry)

```bash
poetry run sports-calendar sync-db
poetry run sports-calendar sync-calendar dev
```

The included Makefile provides shortcuts for the most common tasks:
- `make setup` — sets up the environment and installs dependencies
- `make sync-db` — updates the local database
- `make sync-calendar` — synchronizes the `dev` selection with Google Calendar
- `make all` — runs both `sync-db` and `sync-calendar`

#### Docker

The project can be containerized for reproducible environments:
- `make docker-build` — build the Docker image
- `make docker-sync-db` — run database sync inside Docker
- `make docker-sync-calendar` — run calendar sync inside Docker
- `make docker-all` — run both steps inside Docker

### Additional information

- The data pipeline is divided into `landing`, `intermediate`, and `staging` stages. Modifying these workflows requires understanding how data is retrieved and processed; format changes or API behavior may break the pipeline.
- Several API clients are implemented (ESPN, football-data.org, football-ranking.com, LiveSoccerTV), but merging data from multiple sources is complex. Only ESPN is fully supported for production use.
- Configuration files and selection YAMLs are flexible, but deep modifications may require studying the code and workflow carefully.

## Authors

- Jolan Du Bois — master student in AI and sports fan
- This project is a personal learning exercise and hobby.
