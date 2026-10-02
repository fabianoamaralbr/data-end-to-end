.PHONY: install test test-unit lint tf-init tf-plan tf-apply download-data deploy-adf deploy-job run-pipeline

TF_DIR  := infra
TF_PLAN := tfplan

install:
	pip install -r requirements-dev.txt
	pip install -e .

test:
	pytest tests/ --cov=src/transforms --cov-report=term-missing

# Sem escrita Delta (util no Windows sem winutils)
test-unit:
	pytest tests/unit/

lint:
	ruff check src/ tests/ scripts/ databricks/notebooks/

# ── Infraestrutura ───────────────────────────────────────────────────────────
# Fluxo: plan gera um arquivo revisavel; apply aplica *exatamente* esse plano.
# Em PRs, o workflow terraform.yml comenta o plan; o apply em main exige aprovacao
# do environment "production". Nao existe apply sem plano revisado.
tf-init:
	cd $(TF_DIR) && terraform init -backend-config=backend.hcl

tf-plan: tf-init
	cd $(TF_DIR) && terraform plan -out=$(TF_PLAN)

tf-apply:
	@test -f $(TF_DIR)/$(TF_PLAN) || (echo "Rode 'make tf-plan' e revise o plano antes." && exit 1)
	cd $(TF_DIR) && terraform apply $(TF_PLAN) && rm -f $(TF_PLAN)

# ── Dados e orquestracao ─────────────────────────────────────────────────────
download-data:
	python scripts/download_kaggle_data.py

deploy-adf:
	@for p in pl_ingest_financial_data pl_ingest_bcb_sgs; do \
		az datafactory pipeline create \
			--resource-group $$AZURE_RESOURCE_GROUP --factory-name $$ADF_NAME \
			--name $$p --pipeline @adf/pipeline/$$p.json; \
	done

# Requer UC_CATALOG, ADLS_ACCOUNT_NAME e ALERT_EMAIL no ambiente (.env)
deploy-job:
	envsubst < databricks/jobs/medallion_workflow.json > /tmp/medallion_workflow.json
	databricks jobs create --json @/tmp/medallion_workflow.json

run-pipeline:
	az datafactory pipeline create-run \
		--resource-group $$AZURE_RESOURCE_GROUP \
		--factory-name $$ADF_NAME \
		--name pl_ingest_bcb_sgs
