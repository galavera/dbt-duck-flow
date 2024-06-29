include ~/.env
export

.PHONY: rf-ingest format

DBT_FOLDER = transform/housing_market
DBT_TARGET = dev

rf-ingest:
	@echo "Running Redfin ingestion..."
	@poetry run python -m ingestion.pipeline

rf-transform:
	cd $$DBT_FOLDER && \
	dbt run \
		--$$DBT_TARGET \	

format:
	ruff format .

test:
	pytest tests