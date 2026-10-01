#!/usr/bin/env bash
# Cria (uma unica vez) o storage do Terraform state remoto, com versionamento,
# soft delete e acesso apenas via Entra ID (sem account keys).
#
# Uso: RG=rg-terraform-state SA=sttfstatefinancial LOCATION=eastus2 ./scripts/bootstrap_tfstate.sh
set -euo pipefail

RG="${RG:-rg-terraform-state}"
SA="${SA:-sttfstatefinancial}"
LOCATION="${LOCATION:-eastus2}"
CONTAINER="${CONTAINER:-tfstate}"

az group create --name "$RG" --location "$LOCATION" --output none

az storage account create \
  --name "$SA" --resource-group "$RG" --location "$LOCATION" \
  --sku Standard_ZRS --kind StorageV2 \
  --min-tls-version TLS1_2 --allow-blob-public-access false \
  --allow-shared-key-access false \
  --output none

az storage account blob-service-properties update \
  --account-name "$SA" --resource-group "$RG" \
  --enable-versioning true --enable-delete-retention true --delete-retention-days 30 \
  --output none

az storage container create --name "$CONTAINER" --account-name "$SA" --auth-mode login --output none

# Quem roda o Terraform (usuario local ou a identidade federada do CI) precisa deste papel.
ASSIGNEE="${ASSIGNEE:-$(az ad signed-in-user show --query id -o tsv)}"
az role assignment create \
  --assignee "$ASSIGNEE" \
  --role "Storage Blob Data Contributor" \
  --scope "$(az storage account show --name "$SA" --resource-group "$RG" --query id -o tsv)" \
  --output none

echo "State remoto pronto: $SA/$CONTAINER. Copie infra/backend.hcl.example para infra/backend.hcl."
