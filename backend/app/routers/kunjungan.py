"""
Kunjungan:
  * POST /api/kunjungan          -> INSERT ke primary (OLTP)
  * GET  /api/kunjungan          -> list terbaru dari replica (OLTP read)
  * GET  /api/kunjungan/search   -> full-text search di Elasticsearch (gold)
"""
from typing import Literal, Optional

from fastapi import APIRouter, Query

from ..db import execute_returning, fetch_all
from ..es import safe_search
from ..schemas import KunjunganIn

router = APIRouter(prefix="/api/kunjungan", tags=["kunjungan"])

SEARCH_FIELDS = {
    "semua": ["nama_pasien^3", "nama_faskes^2", "diagnosis_awal", "diagnosis_awal.plain"],
    "pasien": ["nama_pasien"],
    "faskes": ["nama_faskes"],
    "diagnosis": ["diagnosis_awal", "diagnosis_awal.plain"],
}


@router.post("", status_code=201)
def create_kunjungan(body: KunjunganIn):
    data = body.model_dump()
    row = execute_returning(
        """INSERT INTO kunjungan (peserta_id, faskes_id, tanggal_kunjungan, poli,
                                  jenis_kunjungan, diagnosis_awal, status)
           VALUES (%(peserta_id)s, %(faskes_id)s, COALESCE(%(tanggal_kunjungan)s, now()),
                   %(poli)s, %(jenis_kunjungan)s, %(diagnosis_awal)s, %(status)s)
           RETURNING id, no_kunjungan, tanggal_kunjungan""",
        data,
    )
    return {"item": row, "source": "pg-primary"}


@router.get("")
def list_kunjungan(limit: int = Query(20, le=200)):
    rows = fetch_all(
        """SELECT k.id, k.no_kunjungan, k.tanggal_kunjungan, k.poli, k.jenis_kunjungan,
                  k.status, k.diagnosis_awal, p.nama AS nama_pasien, f.nama AS nama_faskes
           FROM kunjungan k
           JOIN peserta p ON p.id = k.peserta_id
           JOIN faskes  f ON f.id = k.faskes_id
           ORDER BY k.id DESC LIMIT %s""",
        (limit,),
    )
    return {"items": rows, "source": "pg-replica"}


@router.get("/search")
def search_kunjungan(
    q: Optional[str] = None,
    field: Literal["semua", "pasien", "faskes", "diagnosis"] = "semua",
    poli: Optional[str] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    filters = [{"term": {"poli": poli}}] if poli else []

    if q and q.strip():
        text = q.strip()
        fields = SEARCH_FIELDS[field]
        must = [{
            "bool": {
                "should": [
                    # toleran salah ketik di semua kata: "demam berdrah" -> "demam berdarah"
                    {"multi_match": {"query": text, "fields": fields, "type": "best_fields",
                                     "fuzziness": "AUTO", "operator": "and"}},
                    # ketik sebagian (kata terakhir sebagai prefix): "sit" -> "Siti"
                    {"multi_match": {"query": text, "fields": fields, "type": "bool_prefix",
                                     "operator": "and"}},
                ],
                "minimum_should_match": 1,
            }
        }]
        sort = ["_score", {"tanggal_kunjungan": "desc"}]
    else:
        must = [{"match_all": {}}]
        sort = [{"tanggal_kunjungan": "desc"}]

    res = safe_search(
        "kunjungan",
        query={"bool": {"must": must, "filter": filters}},
        sort=sort,
        from_=(page - 1) * size,
        size=size,
        track_total_hits=True,
        highlight={
            "pre_tags": ["<mark>"], "post_tags": ["</mark>"],
            "fields": {"nama_pasien": {}, "nama_faskes": {},
                       "diagnosis_awal": {"number_of_fragments": 0},
                       "diagnosis_awal.plain": {"number_of_fragments": 0}},
        },
        aggs={"poli": {"terms": {"field": "poli", "size": 30}}},
    )

    hits = res["hits"]
    return {
        "total": hits["total"]["value"],
        "page": page,
        "size": size,
        "items": [{**h["_source"], "highlight": h.get("highlight", {})} for h in hits["hits"]],
        "facets": {
            "poli": [
                {"key": b["key"], "count": b["doc_count"]}
                for b in res.get("aggregations", {}).get("poli", {}).get("buckets", [])
            ]
        },
        "source": "elasticsearch:kunjungan (gold)",
    }
