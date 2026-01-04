# Makefile for complete football workflow

REPO_DIR ?= data/prod
ABS_REPO_DIR := $(abspath $(REPO_DIR))

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

RUN_PIPELINE_LOCAL=python -m src pipeline run --repo $(ABS_REPO_DIR) --env prod --manual --verbose
RUN_SELECTION_LOCAL=python -m src calendar run-selection jolan --repo $(ABS_REPO_DIR)

# Update the live soccer database
.PHONY: update-db
update-db: setup
	$(POETRY) run $(RUN_PIPELINE_LOCAL)

# Run the selection and update Google Calendar
.PHONY: run-selection
run-selection: setup
	$(POETRY) run $(RUN_SELECTION_LOCAL)

DOCKER_IMAGE=football-calendar:latest
DOCKER_REPO_DIR=/app/data
DOCKER_RUN=docker run --rm \
	-v $(ABS_REPO_DIR):$(DOCKER_REPO_DIR) \
	-v $(PWD)/logs:/app/logs \
	-v $(PWD)/.env:/app/.env \
	-v $(PWD)/.secrets:/app/.secrets \
	-v $(PWD)/.credentials:/app/.credentials \
	-w /app $(DOCKER_IMAGE)

RUN_PIPELINE_DOCKER=python -m src pipeline run --repo $(DOCKER_REPO_DIR) --env prod --manual --verbose
RUN_SELECTION_DOCKER=python -m src calendar run-selection jolan --repo $(DOCKER_REPO_DIR)

# Build the Docker image
.PHONY: docker-build
docker-build:
	docker build -t $(DOCKER_IMAGE) .

# Run the update-db target inside Docker
.PHONY: docker-update-db
docker-update-db: docker-build
	$(DOCKER_RUN) $(POETRY) run $(RUN_PIPELINE_DOCKER)

# Run the run-selection target inside Docker
.PHONY: docker-run-selection
docker-run-selection: docker-build
	$(DOCKER_RUN) $(POETRY) run $(RUN_SELECTION_DOCKER)

# Combine both Docker targets in one container run
.PHONY: docker-all
docker-all: docker-build
	$(DOCKER_RUN) bash -c "\
		poetry run $(RUN_PIPELINE_DOCKER) && \
		poetry run $(RUN_SELECTION_DOCKER) \
	"