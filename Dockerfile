# Phase 2 enrichment worker for Cloud Run Jobs (NVIDIA L4 GPU).
# Build: gcloud builds submit (see cloud/deploy_pipeline.sh)
FROM python:3.11-slim

# curl is used by the image fetcher's fallback path; the rest are runtime libs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/opt/hf-cache

WORKDIR /app

# --- Heavy ML deps first, as their own cache layer ---------------------------
# CUDA build of torch (PyPI default wheel bundles the CUDA runtime) so CLIP runs
# on the Cloud Run NVIDIA L4 GPU. torch + torchvision pinned to the known-good
# set; they must come from the same source so their compiled ops match (a
# mismatched torchvision fails with "operator torchvision::nms does not exist").
# open-clip is installed here so the model bake below is a stable cache layer.
RUN pip install torch==2.10.0 torchvision==0.25.0 \
    && pip install "open-clip-torch==3.3.0" \
       "transformers==5.12.1" "sentencepiece==0.2.1" "protobuf==6.33.5"

# Bake the Marqo-FashionSigLIP weights into the image so N cold-starting tasks
# don't all re-download ~1GB from Hugging Face on every job execution.
RUN python -c "import open_clip; open_clip.create_model_and_transforms('hf-hub:Marqo/marqo-fashionSigLIP'); open_clip.get_tokenizer('hf-hub:Marqo/marqo-fashionSigLIP')"

# --- Application -------------------------------------------------------------
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

# Branch fan-out reads the TSVs at enrich time; bake them in.
COPY data/stores ./data/stores
COPY cloud ./cloud

# Cloud Run config defaults (override per-execution with --set-env-vars).
# The container filesystem is in-memory (tmpfs), so the on-disk image cache would
# just consume RAM — disable it and rely on the in-memory byte cache instead.
ENV PERSISTENCE_TARGET=supabase \
    INPUT_TSV_DIR=/app/data/stores \
    FETCHER_DISK_CACHE_ENABLED=false \
    LOG_LEVEL=INFO \
    DB_CONNECT_TIMEOUT_SEC=30 \
    CRAWL_GLOBAL_CONCURRENCY=4 \
    IMAGE_VALIDATION_CONCURRENCY=8 \
    PHASE2_UPLOAD_CONCURRENCY=6 \
    PHASE2_STORE_BATCH_SIZE=6 \
    CLIP_MODEL_NAME=hf-hub:Marqo/marqo-fashionSigLIP \
    # Model is baked above — load it from the image cache, never hit HF at runtime
    # (an unauthenticated HF Hub revision check can stall the model load).
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    # --- Safety defaults (override consciously) ---
    # Never delete stores on a Phase 1 crawl, and don't re-geocode branches that
    # already have coordinates. Image uploads were removed in code.
    PRUNE_NONTSV_STORES=false \
    GEOCODING_ENABLED=false

ENTRYPOINT ["python", "cloud/run_pipeline.py"]
