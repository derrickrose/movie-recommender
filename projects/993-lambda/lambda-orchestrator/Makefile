SHELL := /bin/bash
.DEFAULT_GOAL := unit-test

BASE=.
SRC_DIR=./src/
TEST_DIR=./tests/

.PHONY: clean-pycache
clean-pycache:
	@echo "Cleaning cache ..."
	@find $(BASE) -type f -name '*.pyc' -delete
	@find $(BASE) -type d -name '__pycache__' -delete

.PHONY: install
install:
	@echo "Installing dependencies ..."
	@python -m pip install --upgrade pip
	@python -m pip install poetry
	@python -m pip install --upgrade poetry
	@python -m poetry install --no-root

.PHONY: format
format:
	@echo "Checking code format with black ..."
	@python -m poetry run black --check $(SRC_DIR)

.PHONY: lint
lint:
	@echo "Running flake8 linter ..."
	@python -m poetry run flake8 --statistics $(SRC_DIR)

.PHONY: type-check
type-check:
	@echo "Type-checking ..."
	@python -m poetry run mypy $(SRC_DIR)

.PHONY: start-localstack
start-localstack:
	@echo "Starting LocalStack ..."
	@docker run -d -p 4566:4566 --name localstack localstack/localstack
	@echo "Waiting 15 seconds for localstack to get ready ..."
	@sleep 15

.PHONY: stop-localstack
stop-localstack:
	@echo "Stopping LocalStack ..."
	@docker stop localstack
	@docker system prune -f

.PHONY: unit-test
unit-test: clean-pycache install format lint type-check start-localstack
	@echo "Running integration tests ..."
	@#python -m poetry run pytest -v $(TEST_DIR)
	@python -m poetry run pytest -v $(TEST_DIR)
	@make stop-localstack
	@make clean-venv

.PHONY: clean-venv
clean-venv:
	@make clean-pycache
	@echo "Cleaning virtualenv ..."
	@rm -rf /root/.cache/pypoetry/virtualenvs*


.PHONY: clean-layer
clean-layer:
	@echo "Cleaning layer ..."
	@rm -rf venv
	@rm -rf python
	@rm -rf orchestrator_lambda_layer.zip

.PHONY: create-venv
create-venv:
	@echo "Creating virtual environment ..."
	@python -m venv venv
	@source venv/bin/activate

.PHONY: create-layer
create-layer: clean-layer create-venv
	@echo "Creating layer zip file ..."
	@mkdir python
	@pip install --upgrade pip
	@pip install -r layer_requirements.txt -t python/
	@zip -r orchestrator_lambda_layer.zip python

.PHONY: help
help:
	@echo "Available commands:"
	@echo "- help             : Display this help message"
	@echo "- clean-pycache    : Remove Python bytecode files and __pycache__ directories"
	@echo "- install          : Update pip, install poetry, and install project dependencies"
	@echo "- format           : Check formatting of source code with black"
	@echo "- lint             : Lint source code with flake8"
	@echo "- type-check       : Run mypy for type checking"
	@echo "- start-localstack : Start localstack to simulate aws environment"
	@echo "- stop-localstack  : Stop localstack simulation environment"
	@echo "- unit-test        : Run all tests"
