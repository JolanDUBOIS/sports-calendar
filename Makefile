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

.PHONY: all
all: sync-db sync-calendar

.PHONY: setup
setup:
	$(POETRY) env use $(PYTHON)
	$(POETRY) install

.PHONY: sync-db
sync-db:
	$(POETRY) run sports-calendar sync-db

.PHONY: sync-calendar
sync-calendar:
	$(POETRY) run sports-calendar sync-calendar dev

.PHONY: clear-calendar
clear-calendar:
	$(POETRY) run sports-calendar clear-calendar dev

# ------------------
# Docker targets
# ------------------

.PHONY: docker-build
docker-build:
	docker build -t $(DOCKER_IMAGE) .

.PHONY: docker-sync-db
docker-sync-db: docker-build
	$(DOCKER_RUN) $(POETRY) run sports-calendar sync-db

.PHONY: docker-sync-calendar
docker-sync-calendar: docker-build
	$(DOCKER_RUN) $(POETRY) run sports-calendar sync-calendar dev

.PHONY: docker-all
docker-all: docker-build
	$(DOCKER_RUN) bash -c "\
		poetry run sports-calendar sync-db && \
		poetry run sports-calendar sync-calendar dev \
	"
