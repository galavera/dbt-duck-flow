
.PHONY: snowflake-ingest format

snowflake-ingest:
	poetry run python -m ingestion.pipeline

format:
	black .

test:
	pytest tests