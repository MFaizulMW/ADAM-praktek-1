"""CRUD master data: faskes & peserta. Tulis -> primary, baca -> replica."""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..db import execute_returning, fetch_all, fetch_one
from ..schemas import FaskesIn, PesertaIn

router = APIRouter(prefix="/api", tags=["master"])


# ------------------------------- FASKES -------------------------------
@router.get("/faskes")
def list_faskes(q: Optional[str] = None, limit: int = Query(50, le=500), offset: int = 0):
    where, params = "", {"limit": limit, "offset": offset}
    if q:
        where = "WHERE nama ILIKE %(q)s OR kode_faskes ILIKE %(q)s OR kota ILIKE %(q)s"
        params["q"] = f"%{q}%"
    rows = fetch_all(
        f"""SELECT id, kode_faskes, nama, jenis, tingkat, kota, provinsi, updated_at
            FROM faskes {where} ORDER BY nama LIMIT %(limit)s OFFSET %(offset)s""",
        params,
    )
    total = fetch_one(f"SELECT count(*) AS n FROM faskes {where}", params)["n"]
    return {"total": total, "items": rows, "source": "pg-replica"}


@router.post("/faskes", status_code=201)
def create_faskes(body: FaskesIn):
    row = execute_returning(
        """INSERT INTO faskes (kode_faskes, nama, jenis, tingkat, alamat, kota, provinsi)
           VALUES (%(kode_faskes)s, %(nama)s, %(jenis)s, %(tingkat)s, %(alamat)s, %(kota)s, %(provinsi)s)
           RETURNING *""",
        body.model_dump(),
    )
    return {"item": row, "source": "pg-primary"}


# ------------------------------- PESERTA ------------------------------
@router.get("/peserta")
def list_peserta(q: Optional[str] = None, limit: int = Query(50, le=500), offset: int = 0):
    where, params = "", {"limit": limit, "offset": offset}
    if q:
        where = "WHERE p.nama ILIKE %(q)s OR p.no_kartu LIKE %(q)s"
        params["q"] = f"%{q}%"
    rows = fetch_all(
        f"""SELECT p.id, p.no_kartu, p.nama, p.tanggal_lahir, p.jenis_kelamin, p.segmen,
                   p.kelas_rawat, f.nama AS faskes_terdaftar, p.updated_at
            FROM peserta p LEFT JOIN faskes f ON f.id = p.faskes_id
            {where} ORDER BY p.id DESC LIMIT %(limit)s OFFSET %(offset)s""",
        params,
    )
    total = fetch_one(f"SELECT count(*) AS n FROM peserta p {where}", params)["n"]
    return {"total": total, "items": rows, "source": "pg-replica"}


@router.get("/peserta/{peserta_id}")
def get_peserta(peserta_id: int):
    row = fetch_one("SELECT * FROM peserta WHERE id = %s", (peserta_id,))
    if not row:
        raise HTTPException(404, "Peserta tidak ditemukan")
    return row


@router.post("/peserta", status_code=201)
def create_peserta(body: PesertaIn):
    row = execute_returning(
        """INSERT INTO peserta (no_kartu, nik, nama, tanggal_lahir, jenis_kelamin, alamat,
                                no_hp, segmen, kelas_rawat, faskes_id)
           VALUES (%(no_kartu)s, %(nik)s, %(nama)s, %(tanggal_lahir)s, %(jenis_kelamin)s,
                   %(alamat)s, %(no_hp)s, %(segmen)s, %(kelas_rawat)s, %(faskes_id)s)
           RETURNING id, no_kartu, nama""",
        body.model_dump(),
    )
    return {"item": row, "source": "pg-primary"}
