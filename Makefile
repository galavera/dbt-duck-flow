include ~/.env
export

.PHONY: rf-ingest rf-transform format

DBT_FOLDER = 2_transform/housing_market
#DBT_TARGET = analytics

rf-ingest:
	@echo "Running Redfin ingestion..."
	@poetry run python -m 1_ingestion.pipeline

rf-transform:
	if [ -z "$(DBT_MODEL)" ]; then \
		@echo "No model specified, running all models."; \
		cd $$DBT_FOLDER && dbt run --target $$DBT_TARGET; \
	else \
			echo "Running model: $(DBT_MODEL)"; \
			cd $$DBT_FOLDER && dbt run --target $$DBT_TARGET --select $(DBT_MODEL); \
	fi

rf-dev:
	@echo "Running dev models..."
	cd $$DBT_FOLDER && dbt run --target dev --select tag:staging
	
rf-analytics:
	@echo "Running raw model..."
	cd $$DBT_FOLDER && dbt run --target analytics --select tag:schema

format:
	ruff format .

test:
	pytest tests