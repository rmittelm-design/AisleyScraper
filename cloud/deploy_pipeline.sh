#!/usr/bin/env bash
# Build + deploy + run the scraper pipeline on Cloud Run Jobs.
#
#   ./cloud/deploy_pipeline.sh secrets          # one-time: push DB/Supabase creds to Secret Manager (reads .env)
#   ./cloud/deploy_pipeline.sh build            # build & push the image
#   ./cloud/deploy_pipeline.sh deploy           # create/update both jobs (scrape + enrich)
#   ./cloud/deploy_pipeline.sh run <RUN_ID>     # Phase 1 (scrape) then Phase 2 (enrich), waiting on each
#   ./cloud/deploy_pipeline.sh scrape <RUN_ID>  # just Phase 1
#   ./cloud/deploy_pipeline.sh enrich <RUN_ID>  # just Phase 2 (sharded)
#   ./cloud/deploy_pipeline.sh logs <scrape|enrich>   # tail the latest execution's logs (progress + ETA)
#
# Same GCP project/region as the Aisley backend (overridable via env).
set -euo pipefail

PROJECT="${PROJECT:-aisley}"
REGION="${REGION:-us-east1}"
IMAGE="${IMAGE:-aisley-scraper}"
TAG="${TAG:-latest}"
JOB_SCRAPE="${JOB_SCRAPE:-aisley-scrape}"     # Phase 1
JOB_ENRICH="${JOB_ENRICH:-aisley-enrich}"     # Phase 2
TASKS="${TASKS:-5}"                           # Phase 2 shards
PARALLELISM="${PARALLELISM:-$TASKS}"
CPU="${CPU:-2}"
MEMORY="${MEMORY:-4Gi}"
TASK_TIMEOUT="${TASK_TIMEOUT:-7200s}"
MAX_RETRIES="${MAX_RETRIES:-1}"
IMAGE_URI="gcr.io/${PROJECT}/${IMAGE}:${TAG}"
ENV_FILE="${ENV_FILE:-.env}"

_env() { grep -E "^$1=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2- ; }
SECRETS="DATABASE_URL=aisley-database-url:latest,SUPABASE_SERVICE_ROLE_KEY=aisley-supabase-service-role-key:latest"

cmd_secrets() {
  local db sr sa
  db="$(_env DATABASE_URL)"; sr="$(_env SUPABASE_SERVICE_ROLE_KEY)"
  [ -n "$db" ] && [ -n "$sr" ] || { echo "DATABASE_URL / SUPABASE_SERVICE_ROLE_KEY not found in $ENV_FILE"; exit 1; }
  _put aisley-database-url "$db"; _put aisley-supabase-service-role-key "$sr"
  sa="$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')-compute@developer.gserviceaccount.com"
  for s in aisley-database-url aisley-supabase-service-role-key; do
    gcloud secrets add-iam-policy-binding "$s" --project "$PROJECT" \
      --member="serviceAccount:${sa}" --role="roles/secretmanager.secretAccessor" >/dev/null
  done
  echo "secrets ready"
}
_put() {
  if gcloud secrets describe "$1" --project "$PROJECT" >/dev/null 2>&1; then
    printf '%s' "$2" | gcloud secrets versions add "$1" --project "$PROJECT" --data-file=- >/dev/null
  else
    printf '%s' "$2" | gcloud secrets create "$1" --project "$PROJECT" --replication-policy=automatic --data-file=- >/dev/null
  fi
}

cmd_build() {
  echo ">> Building $IMAGE_URI (project=$PROJECT)"
  gcloud builds submit --project "$PROJECT" --tag "$IMAGE_URI" .
}

_envvars() {
  local supabase_url bucket path
  supabase_url="$(_env SUPABASE_URL)"
  bucket="$(_env SUPABASE_STORAGE_BUCKET)"; bucket="${bucket:-uploads}"
  path="$(_env SUPABASE_STORAGE_PATH)"; path="${path:-scraped}"
  echo "SUPABASE_URL=${supabase_url},SUPABASE_STORAGE_BUCKET=${bucket},SUPABASE_STORAGE_PATH=${path}"
}

cmd_deploy() {
  local ev; ev="$(_envvars)"
  echo ">> Deploying Phase 1 job '$JOB_SCRAPE' (1 task)"
  gcloud run jobs deploy "$JOB_SCRAPE" --project "$PROJECT" --region "$REGION" \
    --image "$IMAGE_URI" --tasks 1 --max-retries "$MAX_RETRIES" --task-timeout "$TASK_TIMEOUT" \
    --cpu "$CPU" --memory "$MEMORY" --execution-environment gen2 \
    --set-env-vars "AISLEY_PHASE=1,${ev}" --set-secrets "$SECRETS"
  echo ">> Deploying Phase 2 job '$JOB_ENRICH' (tasks=$TASKS parallelism=$PARALLELISM)"
  gcloud run jobs deploy "$JOB_ENRICH" --project "$PROJECT" --region "$REGION" \
    --image "$IMAGE_URI" --tasks "$TASKS" --parallelism "$PARALLELISM" \
    --max-retries "$MAX_RETRIES" --task-timeout "$TASK_TIMEOUT" \
    --cpu "$CPU" --memory "$MEMORY" --execution-environment gen2 \
    --set-env-vars "AISLEY_PHASE=2,${ev}" --set-secrets "$SECRETS"
}

cmd_scrape() {
  local rid="${1:?Usage: $0 scrape <RUN_ID>}"
  echo ">> Phase 1 (scrape) run_id=$rid — watch progress: $0 logs scrape"
  gcloud run jobs execute "$JOB_SCRAPE" --project "$PROJECT" --region "$REGION" \
    --update-env-vars "AISLEY_RUN_ID=${rid}" --wait
}
cmd_enrich() {
  local rid="${1:?Usage: $0 enrich <RUN_ID>}"
  echo ">> Phase 2 (enrich) run_id=$rid across $TASKS shards — watch: $0 logs enrich"
  gcloud run jobs execute "$JOB_ENRICH" --project "$PROJECT" --region "$REGION" \
    --update-env-vars "AISLEY_RUN_ID=${rid}" --wait
}
cmd_run() {
  local rid="${1:?Usage: $0 run <RUN_ID>}"
  cmd_scrape "$rid"
  cmd_enrich "$rid"
}

cmd_logs() {
  local which="${1:-enrich}" job
  job="$JOB_ENRICH"; [ "$which" = "scrape" ] && job="$JOB_SCRAPE"
  local exec
  exec="$(gcloud run jobs executions list --job "$job" --project "$PROJECT" --region "$REGION" \
          --sort-by=~createTime --limit 1 --format='value(name)')"
  [ -n "$exec" ] || { echo "no executions for $job yet"; exit 1; }
  echo ">> tailing logs for $exec (progress lines show ETA)"
  gcloud beta run jobs executions logs "$exec" --project "$PROJECT" --region "$REGION" --tail
}

case "${1:-}" in
  secrets) cmd_secrets ;;
  build)   cmd_build ;;
  deploy)  cmd_deploy ;;
  run)     shift; cmd_run "${1:-}" ;;
  scrape)  shift; cmd_scrape "${1:-}" ;;
  enrich)  shift; cmd_enrich "${1:-}" ;;
  logs)    shift; cmd_logs "${1:-enrich}" ;;
  *) echo "Usage: $0 {secrets|build|deploy|run <RUN_ID>|scrape <RUN_ID>|enrich <RUN_ID>|logs <scrape|enrich>}"; exit 1 ;;
esac
