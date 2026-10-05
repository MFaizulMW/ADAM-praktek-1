"""
GOLD — Silver -> data mart siap saji.

Output ganda:
  * Parquet  s3a://lakehouse/gold/<mart>/        (arsip analitik, bisa di-query Spark/Trino)
  * Elasticsearch index (serving layer untuk FE):
        kunjungan                   -> dokumen kunjungan terdenormalisasi (full-text search)
        mart_kunjungan_per_poli     -> agregat per poli
        mart_kunjungan_per_faskes   -> agregat per faskes
        mart_diagnosis_wordcloud    -> frekuensi kata diagnosis awal
        mart_ringkasan              -> KPI ringkas

Publikasi ke ES memakai pola *index versioning + alias swap*: data ditulis ke
index baru `<alias>_v<timestamp>`, lalu alias dipindah secara atomik. FE tidak
pernah melihat index setengah jadi, dan baris yang dihapus di sumber ikut hilang.
"""
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import GOLD, SILVER, es_request, log

ISO_TS = "yyyy-MM-dd'T'HH:mm:ssXXX"

# Stopword bahasa Indonesia + kata pengisi yang umum di catatan klinis
STOPWORDS = sorted(set("""
ada adalah agak akan aku anda antara apa atau bagian bahwa baik banyak baru beberapa belum
berat bila bisa boleh bukan cukup dalam dan dapat dari demikian dengan di dia diri disertai
dirasakan hari harus hingga ia ini itu jam jika juga kadang kali kalau kami karena ke
kemarin kembali keluhan kurang lalu lama lagi lebih mulai malam masih mau melalui mengalami
mengeluh mengeluhkan merasa minggu namun oleh pada pagi pasien per perlu pernah pula saat
saja sama sampai sangat satu sebelah sebelumnya sedang sehingga sejak sekitar sekali selama
semakin semua sering setelah sudah suspek tahun tampak tanpa tapi telah terasa terus tidak
tiap untuk waktu yaitu yang bulan dua tiga empat lima kiri kanan hilang timbul
datang diagnosis awal utama saat lebih kali
""".split()))

KUNJUNGAN_INDEX = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                "id_stop": {"type": "stop", "stopwords": "_indonesian_"},
                "id_stemmer": {"type": "stemmer", "language": "indonesian"},
            },
            "analyzer": {
                "diagnosis_analyzer": {
                    "type": "custom", "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding", "id_stop", "id_stemmer"],
                },
                "name_analyzer": {
                    "type": "custom", "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                },
            },
        },
    },
    "mappings": {
        "dynamic": False,
        "properties": {
            "id": {"type": "long"},
            "no_kunjungan": {"type": "keyword"},
            "tanggal_kunjungan": {"type": "date"},
            "periode": {"type": "keyword"},
            "poli": {"type": "keyword"},
            "jenis_kunjungan": {"type": "keyword"},
            "status": {"type": "keyword"},
            "diagnosis_awal": {
                "type": "text", "analyzer": "diagnosis_analyzer",
                # tanpa stemming: typo seperti "berdrah" tetap dekat ke "berdarah"
                "fields": {"plain": {"type": "text", "analyzer": "name_analyzer"}},
            },
            "peserta_id": {"type": "long"},
            "no_kartu": {"type": "keyword"},
            "nama_pasien": {
                "type": "text", "analyzer": "name_analyzer",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "jenis_kelamin": {"type": "keyword"},
            "umur": {"type": "integer"},
            "segmen": {"type": "keyword"},
            "kelas_rawat": {"type": "integer"},
            "faskes_id": {"type": "long"},
            "kode_faskes": {"type": "keyword"},
            "nama_faskes": {
                "type": "text", "analyzer": "name_analyzer",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "jenis_faskes": {"type": "keyword"},
            "tingkat_faskes": {"type": "keyword"},
            "kota": {"type": "keyword"},
            "provinsi": {"type": "keyword"},
            "updated_at": {"type": "date"},
        },
    },
}


def _mart_index(properties: dict) -> dict:
    return {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"dynamic": False, "properties": properties},
    }


MART_INDEXES = {
    "mart_kunjungan_per_poli": _mart_index({
        "poli": {"type": "keyword"},
        "jumlah_kunjungan": {"type": "long"},
        "jumlah_pasien_unik": {"type": "long"},
        "generated_at": {"type": "date"},
    }),
    "mart_kunjungan_per_faskes": _mart_index({
        "faskes_id": {"type": "long"},
        "kode_faskes": {"type": "keyword"},
        "nama_faskes": {"type": "keyword"},
        "jenis_faskes": {"type": "keyword"},
        "kota": {"type": "keyword"},
        "provinsi": {"type": "keyword"},
        "jumlah_kunjungan": {"type": "long"},
        "jumlah_pasien_unik": {"type": "long"},
        "generated_at": {"type": "date"},
    }),
    "mart_diagnosis_wordcloud": _mart_index({
        "term": {"type": "keyword"},
        "frekuensi": {"type": "long"},
        "generated_at": {"type": "date"},
    }),
    "mart_ringkasan": _mart_index({
        "total_kunjungan": {"type": "long"},
        "total_peserta": {"type": "long"},
        "total_faskes": {"type": "long"},
        "total_pasien_berkunjung": {"type": "long"},
        "generated_at": {"type": "date"},
    }),
}


# ---------------------------------------------------------------------
# Build marts
# ---------------------------------------------------------------------
def build_kunjungan_doc(k: DataFrame, p: DataFrame, f: DataFrame) -> DataFrame:
    p = p.select(
        F.col("id").alias("p_id"), "no_kartu", F.col("nama").alias("nama_pasien"),
        "jenis_kelamin", "tanggal_lahir", "segmen", "kelas_rawat",
    )
    f = f.select(
        F.col("id").alias("f_id"), "kode_faskes", F.col("nama").alias("nama_faskes"),
        F.col("jenis").alias("jenis_faskes"), F.col("tingkat").alias("tingkat_faskes"),
        "kota", "provinsi",
    )
    return (
        k.join(p, k.peserta_id == p.p_id, "left")
        .join(f, k.faskes_id == f.f_id, "left")
        .select(
            "id", "no_kunjungan",
            F.date_format("tanggal_kunjungan", ISO_TS).alias("tanggal_kunjungan"),
            "periode", "poli", "jenis_kunjungan", "status", "diagnosis_awal",
            "peserta_id", "no_kartu", "nama_pasien", "jenis_kelamin",
            F.floor(F.months_between(F.col("tanggal"), F.col("tanggal_lahir")) / 12)
             .cast("int").alias("umur"),
            "segmen", "kelas_rawat",
            "faskes_id", "kode_faskes", "nama_faskes", "jenis_faskes", "tingkat_faskes",
            "kota", "provinsi",
            F.date_format("updated_at", ISO_TS).alias("updated_at"),
        )
    )


def build_per_poli(k: DataFrame) -> DataFrame:
    return (
        k.groupBy("poli")
        .agg(F.count("*").alias("jumlah_kunjungan"),
             F.countDistinct("peserta_id").alias("jumlah_pasien_unik"))
    )


def build_per_faskes(k: DataFrame, f: DataFrame) -> DataFrame:
    agg = (
        k.groupBy("faskes_id")
        .agg(F.count("*").alias("jumlah_kunjungan"),
             F.countDistinct("peserta_id").alias("jumlah_pasien_unik"))
    )
    return agg.join(
        f.select(F.col("id").alias("faskes_id"), "kode_faskes",
                 F.col("nama").alias("nama_faskes"), F.col("jenis").alias("jenis_faskes"),
                 "kota", "provinsi"),
        "faskes_id", "left",
    )


def build_wordcloud(spark: SparkSession, k: DataFrame, top_n: int = 200) -> DataFrame:
    stop = spark.createDataFrame([(w,) for w in STOPWORDS], ["term"])
    terms = (
        k.select(F.explode(F.split(F.col("diagnosis_norm"), r"\s+")).alias("term"))
        .where(F.length("term") >= 3)
        .join(F.broadcast(stop), "term", "left_anti")
    )
    return (
        terms.groupBy("term").agg(F.count("*").alias("frekuensi"))
        .orderBy(F.col("frekuensi").desc()).limit(top_n)
    )


def build_ringkasan(spark: SparkSession, k: DataFrame, p: DataFrame, f: DataFrame) -> DataFrame:
    row = (
        k.agg(F.count("*").alias("total_kunjungan"),
              F.countDistinct("peserta_id").alias("total_pasien_berkunjung"))
        .first()
    )
    return spark.createDataFrame(
        [("all", row["total_kunjungan"], p.count(), f.count(), row["total_pasien_berkunjung"])],
        "id string, total_kunjungan long, total_peserta long, total_faskes long, "
        "total_pasien_berkunjung long",
    )


# ---------------------------------------------------------------------
# Publish ke Elasticsearch (versioned index + atomic alias swap)
# ---------------------------------------------------------------------
def publish_to_es(df: DataFrame, alias: str, index_body: dict, id_col: str) -> str:
    version = datetime.now().strftime("%Y%m%d%H%M%S")
    new_index = f"{alias}_v{version}"

    es_request("PUT", f"/{new_index}", index_body)
    (
        df.write.format("org.elasticsearch.spark.sql")
        .option("es.resource", new_index)
        .option("es.mapping.id", id_col)
        .option("es.write.operation", "index")
        .option("es.index.auto.create", "false")
        .option("es.batch.write.refresh", "false")
        .mode("append")
        .save()
    )
    es_request("POST", f"/{new_index}/_refresh")

    current = es_request("GET", f"/_alias/{alias}", ok_404=True) or {}
    actions = [{"remove": {"index": old, "alias": alias}} for old in current]
    actions.append({"add": {"index": new_index, "alias": alias}})
    es_request("POST", "/_aliases", {"actions": actions})

    # bersihkan versi lama (termasuk sisa run yang gagal)
    indices = es_request("GET", f"/_cat/indices/{alias}_v*?format=json", ok_404=True) or []
    for idx in indices:
        if idx["index"] != new_index:
            es_request("DELETE", f"/{idx['index']}", ok_404=True)
    return new_index


def _with_generated_at(df: DataFrame) -> DataFrame:
    return df.withColumn("generated_at", F.date_format(F.current_timestamp(), ISO_TS))


def run(spark: SparkSession, silver: dict | None = None) -> dict:
    if silver is None:
        silver = {t: spark.read.parquet(f"{SILVER}/{t}") for t in ("faskes", "peserta", "kunjungan")}
    k, p, f = silver["kunjungan"], silver["peserta"], silver["faskes"]

    marts = {
        "kunjungan": (build_kunjungan_doc(k, p, f), KUNJUNGAN_INDEX, "id"),
        "mart_kunjungan_per_poli": (_with_generated_at(build_per_poli(k)),
                                    MART_INDEXES["mart_kunjungan_per_poli"], "poli"),
        "mart_kunjungan_per_faskes": (_with_generated_at(build_per_faskes(k, f)),
                                      MART_INDEXES["mart_kunjungan_per_faskes"], "faskes_id"),
        "mart_diagnosis_wordcloud": (_with_generated_at(build_wordcloud(spark, k)),
                                     MART_INDEXES["mart_diagnosis_wordcloud"], "term"),
        "mart_ringkasan": (_with_generated_at(build_ringkasan(spark, k, p, f)),
                           MART_INDEXES["mart_ringkasan"], "id"),
    }

    published = {}
    for name, (df, index_body, id_col) in marts.items():
        df = df.cache()
        df.write.mode("overwrite").parquet(f"{GOLD}/{name}")
        published[name] = publish_to_es(df, name, index_body, id_col)
        df.unpersist()
    log(f"gold done: {published}")
    return published
