#!/usr/bin/env bash
# Dataproc cluster for the Olist pipeline.
# 1 driver + 2 workers = 8 vCPU / 32 GB available to executors.
# Run from Cloud Shell, or anywhere with gcloud authenticated.
set -euo pipefail

PROJECT="olist-pipeline-509804"
REGION="us-central1"
BUCKET="olist-sujinan-2026"

gcloud dataproc clusters create olist-cluster \
  --project="$PROJECT" \
  --region="$REGION" \
  --image-version=2.2-debian12 \
  --master-machine-type=n4d-standard-4 \
  --master-boot-disk-type=hyperdisk-balanced --master-boot-disk-size=32 \
  --num-workers=2 \
  --worker-machine-type=n4d-standard-4 \
  --worker-boot-disk-type=hyperdisk-balanced --worker-boot-disk-size=32 \
  --optional-components=JUPYTER \
  --enable-component-gateway \
  --bucket="$BUCKET" \
  --scopes=https://www.googleapis.com/auth/cloud-platform

# Notes on the choices above:
#
# --image-version=2.2-debian12
#   Pinned deliberately. Image 2.3 auto-loads a BigLake catalog extension that
#   intercepts every catalog operation (CREATE DATABASE, saveAsTable) and fails
#   unless the Lakehouse API is enabled. It cannot be disabled from the notebook —
#   spark.sql.extensions set on SparkSession.builder has no effect because the
#   extension is bound at cluster level.
#
# --num-workers=2 (fixed, no autoscaling)
#   Autoscaling would resize the cluster mid-run and invalidate the join
#   benchmarks in notebook 05. Also, Dataproc secondary workers are preemptible
#   by default and can disappear during a job.
#
# 32 GB boot disks
#   The console defaults to 500 GB SSD. Actual usage is ~12 GB OS + ~1 GB data,
#   leaving ample room for shuffle spill on a dataset this size.
#
# --bucket
#   Makes Jupyter write notebooks to gs://$BUCKET/notebooks/jupyter/ instead of
#   an auto-generated staging bucket, so notebooks survive cluster deletion.
#
# Prerequisites (one time per project):
#   gcloud services enable dataproc.googleapis.com cloudresourcemanager.googleapis.com \
#     compute.googleapis.com storage.googleapis.com
#
#   PROJNUM=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
#   gcloud projects add-iam-policy-binding "$PROJECT" \
#     --member="serviceAccount:${PROJNUM}-compute@developer.gserviceaccount.com" \
#     --role="roles/dataproc.worker"
#
# Idle policy: gcloud only offers --max-idle, which DELETES the cluster. To have
# it STOP instead (keeping HDFS and the Hive metastore), set it in the Console:
# Cluster details -> Cost control -> 2 hours / Becoming idle / Stop.
