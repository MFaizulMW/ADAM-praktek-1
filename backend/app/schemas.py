from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


class FaskesIn(BaseModel):
    kode_faskes: str = Field(min_length=3, max_length=12)
    nama: str = Field(min_length=3, max_length=150)
    jenis: Literal["Puskesmas", "Klinik Pratama", "Dokter Praktik Perorangan", "Rumah Sakit"]
    tingkat: Literal["FKTP", "FKRTL"]
    alamat: Optional[str] = None
    kota: str
    provinsi: str


class PesertaIn(BaseModel):
    no_kartu: str = Field(pattern=r"^\d{13}$")
    nik: str = Field(pattern=r"^\d{16}$")
    nama: str = Field(min_length=2, max_length=150)
    tanggal_lahir: date
    jenis_kelamin: Literal["L", "P"]
    alamat: Optional[str] = None
    no_hp: Optional[str] = None
    segmen: Literal["PBI", "PPU", "PBPU", "BP"]
    kelas_rawat: Literal[1, 2, 3]
    faskes_id: Optional[int] = None


class KunjunganIn(BaseModel):
    peserta_id: int
    faskes_id: int
    tanggal_kunjungan: Optional[datetime] = None
    poli: str = Field(min_length=2, max_length=50)
    jenis_kunjungan: Literal["Rawat Jalan", "Rawat Inap", "Gawat Darurat"] = "Rawat Jalan"
    diagnosis_awal: str = Field(min_length=5)
    status: Literal["Menunggu", "Diperiksa", "Selesai", "Dirujuk"] = "Selesai"
