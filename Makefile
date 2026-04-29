.PHONY: install dev test fmt lint seed run-prod

install:
	pip install -r requirements-dev.txt
	pip install -e .

dev:
	WSWDY_ENV=dev uvicorn wswdy.main:app --reload --port 8000

run-prod:
	uvicorn wswdy.main:app --host 0.0.0.0 --port 8000 --workers 1

test:
	pytest

fmt:
	ruff format src tests

lint:
	ruff check src tests

seed:
	python scripts/seed.py
