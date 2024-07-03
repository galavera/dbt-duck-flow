include ~/.env
export

.PHONY: rf-ingest rf-transform format

DBT_FOLDER = transform/housing_market
DBT_TARGET = dev

rf-ingest:
	@echo "Running Redfin ingestion..."
	@poetry run python -m ingestion.pipeline

rf-transform:
	if [ -z "$(DBT_MODEL)" ]; then \
		echo "No model specified, running all models."; \
		cd $$DBT_FOLDER && dbt run --target $$DBT_TARGET; \
	else \
			echo "Running model: $(DBT_MODEL)"; \
			cd $$DBT_FOLDER && dbt run --target $$DBT_TARGET --select $(DBT_MODEL); \
	fi

format:
	ruff format .

test:
	pytest tests