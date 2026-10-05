from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg import errors as pg_errors

from .db import close_pools, open_pools
from .routers import dashboard, kunjungan, master, system


@asynccontextmanager
async def lifespan(app: FastAPI):
    open_pools()
    yield
    close_pools()


app = FastAPI(title="BPJS Kesehatan API", version="1.0.0", lifespan=lifespan)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(pg_errors.UniqueViolation)
async def unique_violation(_: Request, exc: pg_errors.UniqueViolation):
    return JSONResponse(status_code=409, content={"detail": f"Data duplikat: {exc.diag.message_detail}"})


@app.exception_handler(pg_errors.ForeignKeyViolation)
async def fk_violation(_: Request, exc: pg_errors.ForeignKeyViolation):
    return JSONResponse(status_code=400, content={"detail": f"Referensi tidak valid: {exc.diag.message_detail}"})


@app.get("/api/health")
def health():
    return {"status": "ok"}


app.include_router(master.router)
app.include_router(kunjungan.router)
app.include_router(dashboard.router)
app.include_router(system.router)
