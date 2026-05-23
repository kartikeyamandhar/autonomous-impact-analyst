.PHONY: setup check test test-phase-% dbt-run dbt-compile dbt-docs dbt-test graph-load agent-run dagster-dev bot-start demo

setup:
	bash scripts/setup.sh

check:
	ruff check src/ tests/
	mypy src/

test:
	pytest tests/ -v

test-phase-%:
	pytest tests/ -v -m "phase_$*"

dbt-run:
	cd src/dbt_project && DBT_PROFILES_DIR=$$PWD dbt build

dbt-compile:
	cd src/dbt_project && DBT_PROFILES_DIR=$$PWD dbt compile

dbt-docs:
	cd src/dbt_project && DBT_PROFILES_DIR=$$PWD dbt docs generate

dbt-test:
	cd src/dbt_project && DBT_PROFILES_DIR=$$PWD dbt test

graph-load:
	python -m src.graph_engine.neo4j_loader

agent-run:
	python -m src.agent.graph_agent

dagster-dev:
	dagster dev -m src.orchestration.dagster_definitions

bot-start:
	python -m src.exposure_bot.slack_bot

demo:
	bash scripts/demo.sh

web:
	uvicorn src.web.app:app --host 127.0.0.1 --port 8000
