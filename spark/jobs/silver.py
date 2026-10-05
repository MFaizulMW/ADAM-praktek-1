"""
SILVER — Bronze CDC events -> tabel bersih (state terkini per primary key).

Langkah per tabel:
  1. Parse envelope Debezium (before / after / op) dengan schema eksplisit.
  2. Deduplikasi: ambil event terakhir per PK (urut source_lsn, kafka_offset).
  3. Buang record yang event terakhirnya delete (op = 'd').
  4. Cleansing & standardisasi (trim, casing, konversi tipe tanggal Debezium).
  5. Masking PII (NIK, no HP) — data lake tidak menyimpan PII mentah di silver.

Output: s3a://lakehouse/silver/<tabel>/ (parquet, overwrite snapshot)
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType)

from common import BRONZE_CDC, SILVER, log

# Tipe mengikuti output Debezium JsonConverter (time.precision.mode=connect):
#   DATE -> int (hari sejak epoch), TIMESTAMPTZ -> string ISO-8601
SCHEMAS = {
    "faskes": StructType([
        StructField("id", IntegerType()),
        StructField("kode_faskes", StringType()),
        StructField("nama", StringType()),
        StructField("jenis", StringType()),
        StructField("tingkat", StringType()),
        StructField("alamat", StringType()),
        StructField("kota", StringType()),
        StructField("provinsi", StringType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
    ]),
    "peserta": StructType([
        StructField("id", IntegerType()),
        StructField("no_kartu", StringType()),
        StructField("nik", StringType()),
        StructField("nama", StringType()),
        StructField("tanggal_lahir", IntegerType()),
        StructField("jenis_kelamin", StringType()),
        StructField("alamat", StringType()),
        StructField("no_hp", StringType()),
        StructField("segmen", StringType()),
        StructField("kelas_rawat", IntegerType()),
        StructField("faskes_id", IntegerType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
    ]),
    "kunjungan": StructType([
        StructField("id", LongType()),
        StructField("no_kunjungan", StringType()),
        StructField("peserta_id", IntegerType()),
        StructField("faskes_id", IntegerType()),
        StructField("tanggal_kunjungan", StringType()),
        StructField("poli", StringType()),
        StructField("jenis_kunjungan", StringType()),
        StructField("diagnosis_awal", StringType()),
        StructField("status", StringType()),
        StructField("created_at", StringType()),
        StructField("updated_at", StringType()),
    ]),
}


def _clean_text(col):
    """trim + rapikan spasi ganda."""
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def latest_state(spark: SparkSession, table: str) -> DataFrame:
    """Rekonstruksi state terkini tabel dari event CDC di bronze."""
    row_schema = SCHEMAS[table]
    envelope = StructType([
        StructField("before", row_schema),
        StructField("after", row_schema),
        StructField("op", StringType()),
    ])

    events = (
        spark.read.parquet(BRONZE_CDC)
        .where(F.col("table_name") == table)
        .select(
            "kafka_offset", "source_lsn", "source_ts_ms", "op",
            F.from_json("payload", envelope).alias("e"),
        )
        # delete hanya membawa 'before'; selain itu pakai 'after'
        .withColumn("row", F.when(F.col("op") == "d", F.col("e.before")).otherwise(F.col("e.after")))
        .where(F.col("row.id").isNotNull())
    )

    w = Window.partitionBy(F.col("row.id")).orderBy(
        F.col("source_lsn").desc_nulls_last(), F.col("kafka_offset").desc()
    )
    return (
        events.withColumn("_rn", F.row_number().over(w))
        .where((F.col("_rn") == 1) & (F.col("op") != "d"))
        .select(
            "row.*",
            F.col("op").alias("_cdc_op"),
            F.timestamp_millis(F.col("source_ts_ms")).alias("_cdc_ts"),
        )
    )


def transform_faskes(df: DataFrame) -> DataFrame:
    return df.select(
        "id",
        F.upper(F.trim("kode_faskes")).alias("kode_faskes"),
        _clean_text(F.col("nama")).alias("nama"),
        _clean_text(F.col("jenis")).alias("jenis"),
        F.upper(F.trim("tingkat")).alias("tingkat"),
        _clean_text(F.col("alamat")).alias("alamat"),
        F.initcap(_clean_text(F.col("kota"))).alias("kota"),
        F.initcap(_clean_text(F.col("provinsi"))).alias("provinsi"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "_cdc_op", "_cdc_ts",
    )


def transform_peserta(df: DataFrame) -> DataFrame:
    tgl_lahir = F.date_add(F.lit("1970-01-01").cast("date"), F.col("tanggal_lahir"))
    return df.select(
        "id",
        F.trim("no_kartu").alias("no_kartu"),
        # masking PII: 6 digit awal (kode wilayah) dipertahankan untuk analitik
        F.concat(F.substring("nik", 1, 6), F.lit("**********")).alias("nik_masked"),
        F.initcap(_clean_text(F.col("nama"))).alias("nama"),
        tgl_lahir.alias("tanggal_lahir"),
        F.upper(F.trim("jenis_kelamin")).alias("jenis_kelamin"),
        F.when(F.upper(F.trim("jenis_kelamin")) == "L", "Laki-laki")
         .otherwise("Perempuan").alias("jenis_kelamin_desc"),
        _clean_text(F.col("alamat")).alias("alamat"),
        F.concat(F.substring("no_hp", 1, 4), F.lit("****"),
                 F.substring(F.col("no_hp"), -3, 3)).alias("no_hp_masked"),
        F.upper(F.trim("segmen")).alias("segmen"),
        "kelas_rawat",
        "faskes_id",
        F.to_timestamp("created_at").alias("created_at"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "_cdc_op", "_cdc_ts",
    )


def transform_kunjungan(df: DataFrame) -> DataFrame:
    tgl = F.to_timestamp("tanggal_kunjungan")
    diagnosis = _clean_text(F.col("diagnosis_awal"))
    return df.select(
        "id",
        "no_kunjungan",
        "peserta_id",
        "faskes_id",
        tgl.alias("tanggal_kunjungan"),
        F.to_date(tgl).alias("tanggal"),
        F.date_format(tgl, "yyyy-MM").alias("periode"),
        F.initcap(_clean_text(F.col("poli"))).alias("poli"),
        _clean_text(F.col("jenis_kunjungan")).alias("jenis_kunjungan"),
        diagnosis.alias("diagnosis_awal"),
        # versi ternormalisasi untuk text mining (word cloud)
        F.trim(F.regexp_replace(F.lower(diagnosis), r"[^a-z\s]", " ")).alias("diagnosis_norm"),
        _clean_text(F.col("status")).alias("status"),
        F.to_timestamp("created_at").alias("created_at"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "_cdc_op", "_cdc_ts",
    )


TRANSFORMS = {
    "faskes": transform_faskes,
    "peserta": transform_peserta,
    "kunjungan": transform_kunjungan,
}


def run(spark: SparkSession) -> dict:
    """Bangun ulang seluruh tabel silver. Return {tabel: DataFrame (cached)} untuk dipakai gold."""
    result, counts = {}, {}
    for table, transform in TRANSFORMS.items():
        df = transform(latest_state(spark, table)).cache()
        counts[table] = df.count()
        writer = df.write.mode("overwrite").option("partitionOverwriteMode", "static")
        if table == "kunjungan":
            writer = writer.partitionBy("periode")
        writer.parquet(f"{SILVER}/{table}")
        result[table] = df
    log(f"silver done: {counts}")
    return result
