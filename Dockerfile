# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Poetry
RUN apt-get update && apt-get install -y curl build-essential && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# Copy only dependency files first for caching
COPY pyproject.toml poetry.lock* /app/

# Install dependencies (no virtualenv)
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Copy the rest of the project
COPY src /app/src
COPY config /app/config
COPY selections /app/selections

# Create logs directory if not exists/mounted
RUN mkdir -p /app/logs