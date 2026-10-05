"""
BRONZE — Kafka (Debezium CDC) -> Parquet di MinIO.

Prinsip bronze: raw, append-only, immutable. Event CDC disimpan apa adanya
(payload JSON utuh) + metadata Kafka untuk lineage & replay. Parsing dan
deduplikasi dilakukan di silver.

Layout: s3a://lakehouse/bronze/cdc_events/table_name=<tabel>/ingest_date=<yyyy-mm-dd>/*.parquet
"""
from pyspark.sql import functions as F

from common import (BRONZE_CDC, BRONZE_CHECKPOINT, CDC_TOPIC_PATTERN,
                    KAFKA_BOOTSTRAP, get_spark, log)


def main():
    spark = get_spark("bpjs-bronze-cdc-stream")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", CDC_TOPIC_PATTERN)
        .option("kafka.metadata.max.age.ms", "10000")   # cepat menemukan topic tabel baru
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )

    bronze = (
        raw.where(F.col("value").isNotNull())
        .select(
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("key").cast("string").alias("record_key"),
            F.col("value").cast("string").alias("payload"),
        )
        .withColumn("table_name", F.regexp_extract("topic", r"^bpjs\.public\.(\w+)$", 1))
        .withColumn("op", F.get_json_object("payload", "$.op"))
        .withColumn("source_ts_ms", F.get_json_object("payload", "$.source.ts_ms").cast("long"))
        .withColumn("source_lsn", F.get_json_object("payload", "$.source.lsn").cast("long"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("kafka_ts"))
    )

    query = (
        bronze.writeStream.format("parquet")
        .option("path", BRONZE_CDC)
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .partitionBy("table_name", "ingest_date")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )
    log(f"bronze stream started -> {BRONZE_CDC}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
