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
  --master-machine-type=e2-standard-4 \
  --master-boot-disk-type=pd-balanced --master-boot-disk-size=32 \
  --num-workers=2 \
  --worker-machine-type=e2-standard-4 \
  --worker-boot-disk-type=pd-balanced --worker-boot-disk-size=32 \
  --optional-components=JUPYTER \
  --enable-component-gateway \
  --bucket="$BUCKET" \
  --max-idle=2h \
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
# e2-standard-4, no --zone
#   The first cluster ran on n4d-standard-4 and was stopped between sessions. A
#   stopped cluster is pinned to its zone and machine type, and us-central1-b ran
#   out of n4d capacity (ZONE_RESOURCE_POOL_EXHAUSTED) for several days, so it
#   could not start again. E2 is the most widely available family, and leaving
#   out --zone lets Dataproc place the cluster in any zone with capacity. Same
#   4 vCPU / 16 GB per node, so the Spark config is unchanged.
#
# --max-idle=2h (deletes, does not stop)
#   Every layer is mirrored to GCS, so nothing is lost when the cluster goes.
#   Deleting avoids both the disk charges of a stopped cluster and the zone
#   lock-in above. Notebook 05 has a cell that restores silver/gold from GCS.
#   Shut down all Jupyter kernels when finished — an open kernel holds a Spark
#   session, the cluster never counts as idle, and the timer never fires.
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
