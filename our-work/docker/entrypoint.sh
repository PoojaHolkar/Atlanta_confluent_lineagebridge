#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Translate the LINEAGE_BRIDGE_* names used in .env into the TF_VAR_* names
# Terraform expects, run `terraform init` if it has not run yet, then hand the
# arguments straight to terraform.
#
#   docker compose run --rm terraform                       # apply
#   docker compose run --rm terraform destroy -auto-approve # tear down
#   docker compose run --rm terraform output -raw flink_api_key_secret
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if [ -z "${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY:-}" ]; then
    echo "LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY is not set." >&2
    echo "Put an organization-level Confluent Cloud key in the repository's .env." >&2
    exit 1
fi

export TF_VAR_confluent_cloud_api_key="$LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_KEY"
export TF_VAR_confluent_cloud_api_secret="${LINEAGE_BRIDGE_CONFLUENT_CLOUD_API_SECRET:-}"

# The Tableflow sink needs no credentials here: Terraform creates its own key.

if [ ! -d .terraform ]; then
    echo "▸ terraform init"
    terraform init -input=false
fi

echo "▸ terraform $*"
exec terraform "$@"
