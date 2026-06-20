#!/usr/bin/env bash
# Build + deploy + run the sharded Phase 2 enrichment as a Cloud Run Job.
#
# Usage:
#   ./cloud/deploy_phase2_job.sh secrets        # one-time: push DB/Supabase creds to Secret Manager (reads .env)
#   ./cloud/deploy_phase2_job.sh build          # build & push the container image
#   ./cloud/deploy_phase2_job.sh deploy         # create/update the Cloud Run Job
#   ./cloud/deploy_phase2_job.sh run <RUN_ID>   # execute the job for a staged run (fans out into $TASKS shards)
#   ./cloud/deploy_phase2_job.sh all            # secrets + build + deploy
#
# Tunables (env vars, with defaults):
#   PROJECT, REGION, IMAGE, JOB, TASKS, PARALLELISM, CPU, MEMORY, TASK_TIMEOUT
set -euo pipefail

# Same GCP project + region as the Aisley Rebrand backend (aisley-backend runs in
# project "aisley", region "us-east1"). Hardcoded so the deploy doesn't depend on
# the active `gcloud config` project, which can drift. Override via env if needed.
PROJECT="${PROJECT:-aisley}"
REGION="${REGION:-us-east1}"
IMAGE="${IMAGE:-aisley-phase2}"
TAG="${TAG:-latest}"
JOB="${JOB:-aisley-phase2}"
TASKS="${TASKS:-5}"                           # number of shards (CLOUD_RUN_TASK_COUNT)
PARALLELISM="${PARALLELISM:-$TASKS}"          # how many run at once (lower to protect Supabase)
CPU="${CPU:-2}"
MEMORY="${MEMORY:-4Gi}"
TASK_TIMEOUT="${TASK_TIMEOUT:-7200s}"         # per-shard wall-clock budget
MAX_RETRIES="${MAX_RETRIES:-1}"

# gcr.io to match the backend's image convention (gcr.io/aisley/aisley-backend);
# no Artifact Registry repo to pre-create.
IMAGE_URI="gcr.io/${PROJECT}/${IMAGE}:${TAG}"
ENV_FILE="${ENV_FILE:-.env}"

# Non-secret config passed straight to the job (read from .env, with fallbacks).
_env() { grep -E "^$1=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- ; }

cmd_secrets() {
  echo ">> Creating/updating secrets in project=$PROJECT from $ENV_FILE"
  local db sr
  db="$(_env DATABASE_URL)"; sr="$(_env SUPABASE_SERVICE_ROLE_KEY)"
  [ -n "$db" ] || { echo "DATABASE_URL not found in $ENV_FILE"; exit 1; }
  [ -n "$sr" ] || { echo "SUPABASE_SERVICE_ROLE_KEY not found in $ENV_FILE"; exit 1; }
  _put_secret aisley-database-url "$db"
  _put_secret aisley-supabase-service-role-key "$sr"
  echo ">> Granting the job's runtime service account access to the secrets"
  local sa; sa="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')-compute@developer.gserviceaccount.com"
  for s in aisley-database-url aisley-supabase-service-role-key; do
    gcloud secrets add-iam-policy-binding "$s" --project "$PROJECT" \
      --member="serviceAccount:${sa}" --role="roles/secretmanager.secretAccessor" >/dev/null
  done
}

_put_secret() {
  local name="$1" val="$2"
  if gcloud secrets describe "$name" --project "$PROJECT" >/dev/null 2>&1; then
    printf '%s' "$val" | gcloud secrets versions add "$name" --project "$PROJECT" --data-file=- >/dev/null
  else
    printf '%s' "$val" | gcloud secrets create "$name" --project "$PROJECT" --replication-policy=automatic --data-file=- >/dev/null
  fi
  echo "   secret $name updated"
}

cmd_build() {
  echo ">> Building $IMAGE_URI (project=$PROJECT region=$REGION)"
  gcloud builds submit --project "$PROJECT" --tag "$IMAGE_URI" .
}

cmd_deploy() {
  local supabase_url bucket path
  supabase_url="$(_env SUPABASE_URL)"
  bucket="$(_env SUPABASE_STORAGE_BUCKET)"; bucket="${bucket:-uploads}"
  path="$(_env SUPABASE_STORAGE_PATH)"; path="${path:-scraped}"
  echo ">> Deploying Cloud Run Job '$JOB' (tasks=$TASKS parallelism=$PARALLELISM)"
  gcloud run jobs deploy "$JOB" \
    --project "$PROJECT" --region "$REGION" \
    --image "$IMAGE_URI" \
    --tasks "$TASKS" --parallelism "$PARALLELISM" \
    --max-retries "$MAX_RETRIES" --task-timeout "$TASK_TIMEOUT" \
    --cpu "$CPU" --memory "$MEMORY" \
    --execution-environment gen2 \
    --set-env-vars "SUPABASE_URL=${supabase_url},SUPABASE_STORAGE_BUCKET=${bucket},SUPABASE_STORAGE_PATH=${path}" \
    --set-secrets "DATABASE_URL=aisley-database-url:latest,SUPABASE_SERVICE_ROLE_KEY=aisley-supabase-service-role-key:latest"
}

cmd_run() {
  local run_id="${1:-${AISLEY_RUN_ID:-}}"
  [ -n "$run_id" ] || { echo "Usage: $0 run <RUN_ID>"; exit 1; }
  echo ">> Executing '$JOB' for run_id=$run_id across $TASKS shards"
  gcloud run jobs execute "$JOB" \
    --project "$PROJECT" --region "$REGION" \
    --update-env-vars "AISLEY_RUN_ID=${run_id}" \
    --wait
}

case "${1:-}" in
  secrets) cmd_secrets ;;
  build)   cmd_build ;;
  deploy)  cmd_deploy ;;
  run)     shift; cmd_run "${1:-}" ;;
  all)     cmd_secrets; cmd_build; cmd_deploy ;;
  *) echo "Usage: $0 {secrets|build|deploy|run <RUN_ID>|all}"; exit 1 ;;
esac
