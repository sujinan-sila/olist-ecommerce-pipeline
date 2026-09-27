#!/usr/bin/env bash
# Load the Olist dataset into HDFS on a freshly created cluster.
# Run from an SSH session on the master node (olist-cluster-m).
#
# Assumes archive.zip (the Kaggle download) already sits in the bucket.
# Kaggle requires a login, so curl straight from the dataset URL does not work —
# download it locally once and upload it through the Cloud Storage console.
set -euo pipefail

BUCKET="gs://olist-sujinan-2026"

mkdir -p olist/data
gcloud storage cp "$BUCKET/archive.zip" olist/
unzip -q -o olist/archive.zip -d olist/data
ls -lh olist/data                       # expect 9 CSVs, ~126 MB total

# Local disk on the master is not visible to the workers. This is the step that
# splits the files into blocks and distributes them across both workers with
# replication factor 2.
hdfs dfs -mkdir -p /data/olist/raw
hdfs dfs -put -f olist/data/*.csv /data/olist/raw
hdfs dfs -ls -h /data/olist/raw

# Keep the extracted CSVs in the bucket too, so the next cluster skips the unzip.
gcloud storage cp olist/data/*.csv "$BUCKET/raw/"

echo
echo "Verify replication and block placement:"
echo "  hdfs fsck /data/olist/raw -files -blocks -locations"
echo "  hdfs dfsadmin -report"
