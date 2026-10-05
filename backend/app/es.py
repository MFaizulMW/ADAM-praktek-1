from elasticsearch import Elasticsearch, NotFoundError

from .config import ES_URL

es = Elasticsearch(ES_URL, request_timeout=10)


def safe_search(index: str, **body):
    """Search yang mengembalikan hasil kosong jika index gold belum dipublikasikan."""
    try:
        return es.search(index=index, **body)
    except NotFoundError:
        return {"hits": {"total": {"value": 0}, "hits": []}}
