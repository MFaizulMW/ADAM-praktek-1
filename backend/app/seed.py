"""
Generator data dummy BPJS (menulis ke PRIMARY -> ter-replikasi & ter-capture CDC).

  docker compose exec backend python -m app.seed --faskes 40 --peserta 1000 --kunjungan 5000
  docker compose exec backend python -m app.seed --stream --rate 2     # simulasi kunjungan real-time
"""
import argparse
import random
import time
from datetime import datetime, timedelta

import psycopg
from faker import Faker

from .config import PRIMARY_DSN

fake = Faker("id_ID")

JENIS_FASKES = [
    ("Puskesmas", "FKTP", 0.40), ("Klinik Pratama", "FKTP", 0.25),
    ("Dokter Praktik Perorangan", "FKTP", 0.15), ("Rumah Sakit", "FKRTL", 0.20),
]
KOTA = [
    ("Jakarta Selatan", "DKI Jakarta"), ("Jakarta Timur", "DKI Jakarta"), ("Bandung", "Jawa Barat"),
    ("Bekasi", "Jawa Barat"), ("Bogor", "Jawa Barat"), ("Semarang", "Jawa Tengah"),
    ("Surakarta", "Jawa Tengah"), ("Surabaya", "Jawa Timur"), ("Malang", "Jawa Timur"),
    ("Yogyakarta", "DI Yogyakarta"), ("Medan", "Sumatera Utara"), ("Makassar", "Sulawesi Selatan"),
    ("Denpasar", "Bali"), ("Palembang", "Sumatera Selatan"), ("Balikpapan", "Kalimantan Timur"),
]

# poli -> [(keluhan utama, keluhan penyerta, dugaan diagnosis)]
KASUS = {
    "Umum": [
        ("demam tinggi", "nyeri kepala dan mual", "suspek demam berdarah dengue"),
        ("demam naik turun", "nyeri perut dan lidah kotor", "suspek demam tifoid"),
        ("batuk pilek", "nyeri tenggorokan", "infeksi saluran pernapasan akut"),
        ("diare cair lebih dari lima kali sehari", "lemas dan mual", "gastroenteritis akut dengan dehidrasi ringan"),
        ("nyeri ulu hati", "perut kembung dan mual", "dispepsia"),
        ("pusing berputar", "tekanan darah tinggi", "hipertensi esensial"),
    ],
    "Gigi": [
        ("gigi geraham berlubang dan nyeri", "gusi bengkak", "pulpitis ireversibel"),
        ("gusi mudah berdarah", "bau mulut", "gingivitis kronis"),
        ("gigi goyang", "nyeri saat mengunyah", "periodontitis"),
    ],
    "KIA": [
        ("kontrol kehamilan trimester kedua", "mual ringan", "kehamilan normal"),
        ("bengkak pada kaki saat hamil", "tekanan darah meningkat", "suspek preeklampsia"),
        ("imunisasi dasar bayi", "demam ringan", "pemeriksaan tumbuh kembang"),
    ],
    "Anak": [
        ("demam dan batuk pada anak", "sesak napas ringan", "suspek pneumonia anak"),
        ("diare pada balita", "muntah dan rewel", "diare akut pada anak"),
        ("berat badan tidak naik", "nafsu makan menurun", "suspek gizi kurang"),
        ("ruam kemerahan", "demam dan gatal", "suspek campak"),
    ],
    "Penyakit Dalam": [
        ("sering haus dan sering buang air kecil", "berat badan turun", "diabetes melitus tipe dua"),
        ("nyeri kepala tengkuk", "tekanan darah tinggi", "hipertensi tidak terkontrol"),
        ("nyeri sendi lutut", "kaku di pagi hari", "osteoartritis"),
        ("mata dan kulit kuning", "mual dan lemas", "suspek hepatitis"),
    ],
    "Mata": [
        ("mata merah berair", "gatal dan belekan", "konjungtivitis"),
        ("penglihatan kabur perlahan", "silau saat malam", "katarak senilis"),
    ],
    "THT": [
        ("nyeri telinga", "pendengaran berkurang", "otitis media akut"),
        ("hidung tersumbat", "nyeri wajah dan pilek kental", "sinusitis"),
        ("nyeri menelan", "amandel membesar", "tonsilitis"),
    ],
    "Kulit Dan Kelamin": [
        ("gatal pada lipatan kulit", "ruam kemerahan bersisik", "dermatitis jamur"),
        ("gatal hebat di malam hari", "bintil pada sela jari", "skabies"),
    ],
    "Saraf": [
        ("nyeri kepala sebelah berdenyut", "mual dan silau", "migrain"),
        ("kesemutan pada tangan", "kaki terasa kebas", "neuropati perifer"),
        ("lemah separuh badan mendadak", "bicara pelo", "suspek stroke"),
    ],
    "Jantung": [
        ("nyeri dada kiri menjalar ke lengan", "keringat dingin", "suspek sindrom koroner akut"),
        ("sesak napas saat aktivitas", "kaki bengkak", "gagal jantung kongestif"),
        ("jantung berdebar", "pusing", "aritmia"),
    ],
    "Paru": [
        ("batuk berdahak lebih dari dua minggu", "keringat malam dan berat badan turun", "suspek tuberkulosis paru"),
        ("sesak napas berulang", "mengi", "asma bronkial"),
    ],
    "Bedah": [
        ("nyeri perut kanan bawah", "demam dan mual", "suspek apendisitis akut"),
        ("benjolan di lipat paha", "nyeri saat mengejan", "hernia inguinalis"),
        ("luka robek di tangan", "perdarahan aktif", "vulnus laceratum"),
    ],
}
POLI_WEIGHT = {"Umum": 30, "Gigi": 8, "KIA": 8, "Anak": 10, "Penyakit Dalam": 12, "Mata": 5,
               "THT": 5, "Kulit Dan Kelamin": 5, "Saraf": 5, "Jantung": 5, "Paru": 4, "Bedah": 3}
POLI_FKTP = {"Umum", "Gigi", "KIA", "Anak"}


def diagnosis_sentence(poli: str) -> str:
    keluhan, penyerta, dugaan = random.choice(KASUS[poli])
    n = random.randint(1, 14)
    satuan = random.choice(["hari", "hari", "minggu"])
    pembuka = random.choice(["Pasien datang dengan keluhan", "Pasien mengeluhkan",
                             "Keluhan utama pasien adalah"])
    return (f"{pembuka} {keluhan} sejak {n} {satuan} yang lalu disertai {penyerta}, "
            f"diagnosis awal {dugaan}.")


def seed_faskes(cur, n):
    rows = []
    for i in range(n):
        jenis, tingkat, _ = random.choices(JENIS_FASKES, weights=[j[2] for j in JENIS_FASKES])[0]
        kota, prov = random.choice(KOTA)
        prefix = {"Puskesmas": "Puskesmas", "Klinik Pratama": "Klinik",
                  "Dokter Praktik Perorangan": "dr.", "Rumah Sakit": "RS"}[jenis]
        nama = (f"{prefix} {fake.last_name()}" if jenis == "Dokter Praktik Perorangan"
                else f"{prefix} {fake.street_name()} {kota}")
        rows.append((f"{random.randint(1000, 9999)}{i:04d}", nama, jenis, tingkat,
                     fake.street_address(), kota, prov))
    cur.executemany(
        "INSERT INTO faskes (kode_faskes, nama, jenis, tingkat, alamat, kota, provinsi) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", rows)


def seed_peserta(cur, n):
    fktp = [r[0] for r in cur.execute("SELECT id FROM faskes WHERE tingkat='FKTP'").fetchall()]
    rows = []
    for _ in range(n):
        jk = random.choice(["L", "P"])
        nama = fake.name_male() if jk == "L" else fake.name_female()
        rows.append((
            f"000{random.randint(10**9, 10**10 - 1)}", str(random.randint(10**15, 10**16 - 1)),
            nama, fake.date_of_birth(minimum_age=0, maximum_age=85), jk, fake.address(),
            fake.phone_number()[:20], random.choices(["PBI", "PPU", "PBPU", "BP"], [45, 30, 20, 5])[0],
            random.choice([1, 2, 3]), random.choice(fktp) if fktp else None,
        ))
    cur.executemany(
        "INSERT INTO peserta (no_kartu, nik, nama, tanggal_lahir, jenis_kelamin, alamat, no_hp, "
        "segmen, kelas_rawat, faskes_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING", rows)


def _kunjungan_row(peserta, faskes, when):
    pid, faskes_terdaftar = random.choice(peserta)
    poli = random.choices(list(POLI_WEIGHT), weights=list(POLI_WEIGHT.values()))[0]
    if poli in POLI_FKTP and faskes_terdaftar and random.random() < 0.8:
        fid = faskes_terdaftar
    else:
        fid = random.choice(faskes)
    jenis = random.choices(["Rawat Jalan", "Rawat Inap", "Gawat Darurat"], [85, 8, 7])[0]
    status = random.choices(["Selesai", "Dirujuk", "Diperiksa"], [80, 12, 8])[0]
    return (pid, fid, when, poli, jenis, diagnosis_sentence(poli), status)


INSERT_KUNJUNGAN = (
    "INSERT INTO kunjungan (peserta_id, faskes_id, tanggal_kunjungan, poli, jenis_kunjungan, "
    "diagnosis_awal, status) VALUES (%s,%s,%s,%s,%s,%s,%s)"
)


def _refs(cur):
    peserta = cur.execute("SELECT id, faskes_id FROM peserta").fetchall()
    faskes = [r[0] for r in cur.execute("SELECT id FROM faskes").fetchall()]
    return peserta, faskes


def seed_kunjungan(cur, n, days=180):
    peserta, faskes = _refs(cur)
    now = datetime.now().astimezone()
    rows = [_kunjungan_row(peserta, faskes,
                           now - timedelta(days=random.uniform(0, days)))
            for _ in range(n)]
    cur.executemany(INSERT_KUNJUNGAN, rows)


def stream_kunjungan(conn, rate: float):
    peserta, faskes = _refs(conn.cursor())
    print(f"streaming kunjungan ~{rate}/detik, Ctrl+C untuk berhenti")
    i = 0
    while True:
        conn.execute(INSERT_KUNJUNGAN, _kunjungan_row(peserta, faskes, datetime.now().astimezone()))
        conn.commit()
        i += 1
        if i % 10 == 0:
            print(f"  {i} kunjungan ditulis")
        time.sleep(1 / rate)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--faskes", type=int, default=40)
    ap.add_argument("--peserta", type=int, default=1000)
    ap.add_argument("--kunjungan", type=int, default=5000)
    ap.add_argument("--stream", action="store_true")
    ap.add_argument("--rate", type=float, default=1.0)
    args = ap.parse_args()

    with psycopg.connect(PRIMARY_DSN) as conn:
        if args.stream:
            stream_kunjungan(conn, args.rate)
            return
        with conn.cursor() as cur:
            seed_faskes(cur, args.faskes)
            seed_peserta(cur, args.peserta)
            seed_kunjungan(cur, args.kunjungan)
        conn.commit()
    print(f"seed selesai: {args.faskes} faskes, {args.peserta} peserta, {args.kunjungan} kunjungan")


if __name__ == "__main__":
    main()
