-- =====================================================================
-- Skema OLTP aplikasi BPJS Kesehatan
-- =====================================================================

CREATE TABLE IF NOT EXISTS faskes (
    id              SERIAL PRIMARY KEY,
    kode_faskes     VARCHAR(12)  NOT NULL UNIQUE,
    nama            VARCHAR(150) NOT NULL,
    jenis           VARCHAR(40)  NOT NULL,           -- Puskesmas, Klinik Pratama, Dokter Praktik Perorangan, Rumah Sakit
    tingkat         VARCHAR(10)  NOT NULL CHECK (tingkat IN ('FKTP', 'FKRTL')),
    alamat          TEXT,
    kota            VARCHAR(80)  NOT NULL,
    provinsi        VARCHAR(80)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS peserta (
    id              SERIAL PRIMARY KEY,
    no_kartu        VARCHAR(13)  NOT NULL UNIQUE,    -- nomor kartu JKN 13 digit
    nik             VARCHAR(16)  NOT NULL UNIQUE,
    nama            VARCHAR(150) NOT NULL,
    tanggal_lahir   DATE         NOT NULL,
    jenis_kelamin   CHAR(1)      NOT NULL CHECK (jenis_kelamin IN ('L', 'P')),
    alamat          TEXT,
    no_hp           VARCHAR(20),
    segmen          VARCHAR(10)  NOT NULL CHECK (segmen IN ('PBI', 'PPU', 'PBPU', 'BP')),
    kelas_rawat     SMALLINT     NOT NULL CHECK (kelas_rawat IN (1, 2, 3)),
    faskes_id       INT          REFERENCES faskes(id),   -- FKTP terdaftar
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS kunjungan_no_seq;

CREATE TABLE IF NOT EXISTS kunjungan (
    id                  BIGSERIAL PRIMARY KEY,
    no_kunjungan        VARCHAR(30)  NOT NULL UNIQUE
                        DEFAULT ('KJ' || to_char(now(), 'YYYYMMDD') || lpad(nextval('kunjungan_no_seq')::text, 8, '0')),
    peserta_id          INT          NOT NULL REFERENCES peserta(id),
    faskes_id           INT          NOT NULL REFERENCES faskes(id),
    tanggal_kunjungan   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    poli                VARCHAR(50)  NOT NULL,
    jenis_kunjungan     VARCHAR(20)  NOT NULL DEFAULT 'Rawat Jalan'
                        CHECK (jenis_kunjungan IN ('Rawat Jalan', 'Rawat Inap', 'Gawat Darurat')),
    diagnosis_awal      TEXT         NOT NULL,          -- kalimat lengkap (free text)
    status              VARCHAR(20)  NOT NULL DEFAULT 'Selesai'
                        CHECK (status IN ('Menunggu', 'Diperiksa', 'Selesai', 'Dirujuk')),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_peserta_nama        ON peserta (lower(nama));
CREATE INDEX IF NOT EXISTS idx_kunjungan_peserta   ON kunjungan (peserta_id);
CREATE INDEX IF NOT EXISTS idx_kunjungan_faskes    ON kunjungan (faskes_id);
CREATE INDEX IF NOT EXISTS idx_kunjungan_tanggal   ON kunjungan (tanggal_kunjungan DESC);

-- updated_at otomatis
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_faskes_updated    BEFORE UPDATE ON faskes    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_peserta_updated   BEFORE UPDATE ON peserta   FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_kunjungan_updated BEFORE UPDATE ON kunjungan FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Publication untuk Debezium (logical decoding, plugin pgoutput)
CREATE PUBLICATION dbz_publication FOR TABLE faskes, peserta, kunjungan;
