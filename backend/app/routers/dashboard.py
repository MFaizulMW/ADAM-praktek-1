"""Dashboard: membaca data mart (gold) di Elasticsearch — tidak ada agregasi ke Postgres."""
from fastapi import APIRouter, Query

from ..es import safe_search

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _sources(res):
    return [h["_source"] for h in res["hits"]["hits"]]


@router.get("/ringkasan")
def ringkasan():
    docs = _sources(safe_search("mart_ringkasan", size=1))
    return docs[0] if docs else {}


@router.get("/per-poli")
def per_poli():
    res = safe_search("mart_kunjungan_per_poli", size=100,
                      sort=[{"jumlah_kunjungan": "desc"}])
    return {"items": _sources(res)}


@router.get("/per-faskes")
def per_faskes(top: int = Query(15, ge=1, le=500)):
    res = safe_search("mart_kunjungan_per_faskes", size=top,
                      sort=[{"jumlah_kunjungan": "desc"}], track_total_hits=True)
    return {"total_faskes": res["hits"]["total"]["value"], "items": _sources(res)}


@router.get("/wordcloud")
def wordcloud(top: int = Query(100, ge=10, le=200)):
    res = safe_search("mart_diagnosis_wordcloud", size=top,
                      sort=[{"frekuensi": "desc"}])
    return {"items": _sources(res)}
