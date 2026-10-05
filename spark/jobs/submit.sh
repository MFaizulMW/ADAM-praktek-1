#!/bin/bash
# submit.sh <driver-hostname> <job.py>
# Driver berjalan di container ini (client mode), executor di spark-worker.
# Setiap aplikasi dibatasi 2 core agar bronze-stream & pipeline bisa jalan bersamaan.
set -euo pipefail

DRIVER_HOST="$1"
JOB="$2"

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host="$DRIVER_HOST" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.cores.max=2 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.hadoop.fs.s3a.access.key="$MINIO_ROOT_USER" \
  --conf spark.hadoop.fs.s3a.secret.key="$MINIO_ROOT_PASSWORD" \
  --py-files /opt/jobs/common.py,/opt/jobs/silver.py,/opt/jobs/gold.py \
  "$JOB"
