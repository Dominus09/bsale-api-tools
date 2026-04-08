#!/usr/bin/env python3
"""
Asigna dia_atencion (Lunes–Sábado) a clientes con georreferencia, por vendedor.

- Usa bsale_id como clave de actualización.
- Orden geográfico (lat, lon) y reparto en 6 bloques de tamaño ~igual (sin KMeans).

Requisitos: pip install pandas psycopg2-binary python-dotenv
Variables de entorno (mismo esquema que sync_clients): PG_HOST, PG_DB, PG_USER, PG_PASSWORD

Ejecución manual:
  python generate_rutero.py
  python generate_rutero.py --dry-run   # no escribe en BD
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("rutero")

DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado"]

FETCH_SQL = """
SELECT bsale_id, code, rut_clean, lat, lon, vendedor
FROM bsale.clients
WHERE lat IS NOT NULL
  AND lon IS NOT NULL
  AND vendedor IS NOT NULL
  AND TRIM(COALESCE(vendedor, '')) <> ''
"""

UPDATE_SQL = """
UPDATE bsale.clients
SET dia_atencion = %s
WHERE bsale_id = %s
"""


def connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", "5432"),
    )


def assign_days_modulo(n: int) -> list[str]:
    """Menos de 6 clientes: reparto circular Lunes→Sábado."""
    return [DIAS[i % 6] for i in range(n)]


def assign_days_equal_chunks(n: int) -> list[str]:
    """
    n >= 6: orden ya aplicado fuera; asignar secuencialmente por bloques.
    chunk_size = n // 6 → Lunes..Viernes reciben chunk_size; Sábado absorbe el resto.
    """
    if n == 0:
        return []
    if n < 6:
        return assign_days_modulo(n)

    chunk_size = n // 6
    out: list[str] = []
    pos = 0
    for d in range(6):
        if d < 5:
            take = chunk_size
        else:
            take = n - pos
        out.extend([DIAS[d]] * take)
        pos += take
    assert len(out) == n, (len(out), n)
    return out


def process_vendor(sub: pd.DataFrame) -> pd.DataFrame:
    """Ordena por lat, lon y asigna dia_atencion con bloques equitativos."""
    sub = sub.copy()
    n = len(sub)
    if n == 0:
        sub["dia_atencion"] = []
        return sub

    sorted_sub = sub.sort_values(by=["lat", "lon"], kind="mergesort")
    dias = assign_days_equal_chunks(n)
    sorted_sub = sorted_sub.assign(dia_atencion=dias)
    # Devolver en el orden original del grupo (índice original)
    sub["dia_atencion"] = sorted_sub["dia_atencion"].reindex(sub.index)
    return sub


def log_vendor_balance(vendedor: str, sub: pd.DataFrame) -> None:
    """Cuentas por día; avisa si min/max difieren mucho."""
    c = sub.groupby("dia_atencion", sort=False).size()
    counts = pd.Series({d: int(c.get(d, 0)) for d in DIAS})
    lo, hi = int(counts.min()), int(counts.max())
    rango = hi - lo
    log.info(
        "  %r → por día %s | min=%d max=%d rango=%d",
        vendedor,
        dict(zip(DIAS, counts.tolist())),
        lo,
        hi,
        rango,
    )
    if rango > 1 and len(sub) >= 6:
        log.warning(
            "  %r: rango %d (esperable cuando n%%6!=0 el sábado concentra el remanente)",
            vendedor,
            rango,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generar rutero (dia_atencion) por vendedor y georef.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calcula y muestra resumen sin ejecutar UPDATE.",
    )
    args = parser.parse_args()

    for var in ("PG_HOST", "PG_DB", "PG_USER", "PG_PASSWORD"):
        if not os.getenv(var):
            log.error("Falta variable de entorno: %s", var)
            return 1

    conn = connect()
    try:
        df = pd.read_sql_query(FETCH_SQL, conn)
    finally:
        conn.close()

    if df.empty:
        log.warning("No hay filas que cumplan los filtros (lat, lon, vendedor).")
        return 0

    df["vendedor"] = df["vendedor"].astype(str).str.strip()
    df = df.dropna(subset=["lat", "lon", "bsale_id"])

    log.info("Total filas cargadas: %d", len(df))

    counts = df.groupby("vendedor").size().sort_values(ascending=False)
    log.info("Clientes por vendedor (top 20):")
    for v, c in counts.head(20).items():
        log.info("  %s: %d", v, c)
    if len(counts) > 20:
        log.info("  ... (%d vendedores en total)", len(counts))

    out_frames: list[pd.DataFrame] = []
    log.info("Distribución por vendedor y día (validación equilibrio):")
    for vendedor, sub in df.groupby("vendedor", sort=True):
        processed = process_vendor(sub)
        out_frames.append(processed)
        log_vendor_balance(vendedor, processed)
        log.info(
            "Vendedor %r: %d clientes → orden lat/lon + bloques (chunk=n//6, sábado=resto)",
            vendedor,
            len(processed),
        )

    result = pd.concat(out_frames, ignore_index=True)
    updates = list(
        zip(result["dia_atencion"].tolist(), result["bsale_id"].tolist()),
    )

    log.info("Total registros a actualizar: %d", len(updates))

    dist = (
        result.groupby(["vendedor", "dia_atencion"])
        .size()
        .reset_index(name="cantidad")
        .sort_values(["vendedor", "dia_atencion"])
    )
    log.info("Tabla distribución (fragmento, hasta 48 filas):")
    with pd.option_context("display.max_rows", 48, "display.width", 120):
        log.info("\n%s", dist.head(48).to_string(index=False))

    # Resumen global por día (todos los vendedores)
    global_day = result.groupby("dia_atencion").size().reindex(DIAS, fill_value=0)
    log.info(
        "Totales globales por día: %s | min=%d max=%d",
        dict(zip(DIAS, global_day.tolist())),
        int(global_day.min()),
        int(global_day.max()),
    )

    if args.dry_run:
        log.info("Dry-run: no se aplicaron cambios en la base de datos.")
    else:
        conn = connect()
        try:
            cur = conn.cursor()
            execute_batch(cur, UPDATE_SQL, updates, page_size=500)
            conn.commit()
            cur.close()
            log.info("UPDATE completado (%d filas).", len(updates))
        except Exception:
            conn.rollback()
            log.exception("Error al actualizar; rollback.")
            raise
        finally:
            conn.close()

    print("\n--- Resumen: vendedor | dia_atencion | cantidad ---")
    print(dist.to_string(index=False))
    print(f"\nTotal procesados: {len(updates)}")
    print("\n--- Totales por día (todos los vendedores) ---")
    for d in DIAS:
        print(f"  {d}: {int(global_day[d])}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
