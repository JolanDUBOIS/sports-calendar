# Makefile – sports-calendar (dev)

PYTHON ?= python3.11
POETRY ?= poetry

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
	$(POETRY) env use $(PYTHON)
	$(POETRY) install

.PHONY: sync-calendar
sync-calendar:
	$(POETRY) run sports-calendar sync-calendar dev

.PHONY: clear-calendar
clear-calendar:
	$(POETRY) run sports-calendar clear-calendar dev

.PHONY: validate-selections
validate-selections:
	$(POETRY) run sports-calendar devtools validate-selections

.PHONY: launch-gui
launch-gui:
	$(POETRY) run sports-calendar launch-gui

# ------------------
# Docker targets
# ------------------

.PHONY: docker-build
docker-build:
	docker build -t $(DOCKER_IMAGE) .

.PHONY: docker-sync-calendar
docker-sync-calendar: docker-build
	$(DOCKER_RUN) $(POETRY) run sports-calendar sync-calendar dev
