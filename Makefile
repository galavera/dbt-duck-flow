.PHONY: snow-ingest format

snow-ingest:
	@echo "Running Snowflake ingestion..."
	@poetry run python -m ingestion.pipeline

format:
	ruff format .

test:
	pytest tests