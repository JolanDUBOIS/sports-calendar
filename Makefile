# Makefile – sports-calendar (dev)

PYTHON ?= python3.11
UV ?= uv

DATA_DIR := $(abspath data)
CONFIG_DIR := $(abspath config)

DOCKER_IMAGE := sports-calendar:dev
DOCKER_WORKDIR := /app

DOCKER_RUN := docker run --rm \
    --env-file $(PWD)/.env \
	-v $(DATA_DIR):/app/data \
	-v $(PWD)/logs:/app/logs \
	-w $(DOCKER_WORKDIR) \
	$(DOCKER_IMAGE)

# ------------------
# Local targets
# ------------------

# .PHONY: all
# all: sync-calendar

.PHONY: setup
setup:
	$(UV) env use $(PYTHON)
	$(UV) sync --all-extras

# Install just one side, as it will be shipped.
.PHONY: setup-backend
setup-backend:
	$(UV) sync --extra backend

.PHONY: setup-ui
setup-ui:
	$(UV) sync --extra ui

.PHONY: lint-imports
lint-imports:
	$(UV) run lint-imports

.PHONY: sync-calendar
sync-calendar:
	$(UV) run sports-calendar sync-calendar dev

.PHONY: clear-calendar
clear-calendar:
	$(UV) run sports-calendar clear-calendar dev

.PHONY: validate-selections
validate-selections:
	$(UV) run sports-calendar devtools validate-selections

.PHONY: launch-gui
launch-gui:
	$(UV) run sports-calendar launch-gui

# ------------------
# Docker targets
# ------------------

.PHONY: docker-build
docker-build:
	docker build -t $(DOCKER_IMAGE) .

.PHONY: docker-sync-calendar
docker-sync-calendar: docker-build
	$(DOCKER_RUN) $(UV) run sports-calendar sync-calendar dev
