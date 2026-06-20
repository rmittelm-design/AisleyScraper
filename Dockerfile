# Phase 2 enrichment worker for Cloud Run Jobs (CPU, scaled wide).
# Build: gcloud builds submit (see cloud/deploy_phase2_job.sh)
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
# Install the CPU build of torch (the default PyPI wheel pulls CUDA = ~5x larger
# and useless on Cloud Run CPU). open-clip is installed here too so the model
# bake below is a cache layer that survives application code changes.
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu \
    && pip install "open-clip-torch>=2.26.0"

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
    CRAWL_GLOBAL_CONCURRENCY=4 \
    IMAGE_VALIDATION_CONCURRENCY=4 \
    PHASE2_UPLOAD_CONCURRENCY=6 \
    PHASE2_STORE_BATCH_SIZE=6 \
    CLIP_MODEL_NAME=hf-hub:Marqo/marqo-fashionSigLIP

ENTRYPOINT ["python", "cloud/run_phase2_shard.py"]
