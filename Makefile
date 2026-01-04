# Makefile for complete football workflow

POETRY = poetry
PYTHON = python3.11

# Default target
.PHONY: all
all: update-db run-selection

# Setup poetry environment
.PHONY: setup
setup:
	$(POETRY) env use $(PYTHON)
	$(POETRY) install

# Update the live soccer database
.PHONY: update-db
update-db: setup
	$(POETRY) run python -m src pipeline run --repo data/prod --env prod --manual --verbose

# Run the selection and update Google Calendar
.PHONY: run-selection
run-selection: setup
	$(POETRY) run python -m src calendar run-selection jolan --repo data/prod
