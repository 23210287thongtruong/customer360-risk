# Makefile for Customer 360 Risk Scoring Project

.PHONY: help install install-dev test lint format type-check clean data docker-up docker-down

# Default target
help:
	@echo "Customer 360 Risk Scoring Project - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install      Install project dependencies"
	@echo "  make install-dev  Install with all development dependencies"
	@echo "  make install-prod Install production dependencies"
	@echo "  make setup        Complete development environment setup"
	@echo ""
	@echo "Development:"
	@echo "  make test         Run all tests"
	@echo "  make lint         Lint code with ruff"
	@echo "  make format       Format code with black"
	@echo "  make type-check   Run type checking with mypy"
	@echo "  make quality      Run all quality checks"
	@echo ""
	@echo "Data:"
	@echo "  make data         Generate sample data"
	@echo "  make data-large   Generate large dataset"
	@echo "  make clean-data   Clean generated data"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-up    Start all Docker services"
	@echo "  make docker-down  Stop all Docker services"
	@echo "  make docker-logs  View Docker logs"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean        Clean temporary files"
	@echo "  make docs         Generate documentation"

# Installation targets
install:
	uv sync --no-dev

install-dev:
	uv sync --all-extras

install-prod:
	uv sync --no-dev --extra testing --extra viz --extra monitoring

setup:
	./setup-dev.sh

# Development targets
test:
	uv run pytest tests/ -v --cov=scripts --cov=spark_jobs

test-fast:
	uv run pytest tests/ -v -x

lint:
	uv run ruff check .

format:
	uv run black .
	uv run ruff check --fix .

type-check:
	uv run mypy scripts/ spark_jobs/

quality: format lint type-check test

# Data generation targets
data:
	uv run python scripts/generate_data.py --customers 10000 --transactions 50 --output ./data/raw

data-small:
	uv run python scripts/generate_data.py --customers 1000 --transactions 20 --output ./data/raw

data-large:
	uv run python scripts/generate_data.py --customers 100000 --transactions 100 --output ./data/raw

clean-data:
	rm -f data/raw/*.csv
	rm -f data/staging/*.csv

# Docker targets
docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-restart:
	docker-compose restart

# Utility targets
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/

docs:
	@echo "Documentation available in:"
	@echo "  - README.md (main documentation)"
	@echo "  - UV_GUIDE.md (dependency management)"
	@echo "  - docs/PROJECT_SUMMARY.md (project overview)"

# Requirements generation (for legacy compatibility)
requirements:
	pip-compile --output-file requirements.txt

# Pre-commit setup
pre-commit:
	uv run pre-commit install
	uv run pre-commit run --all-files

# Database operations
db-init:
	docker-compose exec postgres psql -U postgres -d customer360_dw -f /docker-entrypoint-initdb.d/init_schema.sql

db-connect:
	docker-compose exec postgres psql -U postgres -d customer360_dw

# Airflow operations
airflow-reset:
	docker-compose exec airflow-webserver airflow db reset

# Full pipeline run (for demonstration)
demo:
	@echo "Running full Customer 360 pipeline demo..."
	make docker-up
	sleep 30
	make data
	@echo "Access Airflow at http://localhost:8080 to run the pipeline"
	@echo "Access Metabase at http://localhost:3000 for dashboards"
