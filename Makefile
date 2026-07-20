.PHONY: test lint compile demo local-up local-down migrate health report

PYTHON ?= .venv/bin/python
STREAMLIT ?= .venv/bin/streamlit

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

compile:
	$(PYTHON) -m compileall -q analytics alerts config dashboard ingestion migrations models orchestration pipelines scripts sentiment simulation storage streaming transformations

demo:
	DASHBOARD_DEMO_MODE=true $(STREAMLIT) run dashboard/app.py --server.address=127.0.0.1 --server.port=8502

local-up:
	docker compose --profile local-pipeline --profile dashboard up -d --build mysql streamlit local-pipeline

local-down:
	docker compose --profile local-pipeline --profile dashboard down

migrate:
	docker compose --profile maintenance run --rm migrate

health:
	docker compose --profile maintenance run --rm health-check

report:
	docker compose --profile reports run --rm weekly-report
