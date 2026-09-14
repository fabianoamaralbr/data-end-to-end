.PHONY: install test lint deploy-infra destroy-infra download-data

install:
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v --cov=databricks/notebooks --cov-report=term-missing

lint:
	ruff check databricks/notebooks/ tests/ scripts/

deploy-infra:
	cd infra && terraform init && terraform apply -auto-approve

destroy-infra:
	cd infra && terraform destroy -auto-approve

download-data:
	python scripts/download_kaggle_data.py

deploy-adf:
	@echo "Publishing ADF pipelines via Azure CLI..."
	az datafactory pipeline create \
		--resource-group $$AZURE_RESOURCE_GROUP \
		--factory-name $$ADF_NAME \
		--name pl_ingest_financial_data \
		--pipeline @adf/pipeline/pl_ingest_financial_data.json

run-pipeline:
	az datafactory pipeline create-run \
		--resource-group $$AZURE_RESOURCE_GROUP \
		--factory-name $$ADF_NAME \
		--name pl_ingest_financial_data
