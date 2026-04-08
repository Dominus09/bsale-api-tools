#!/usr/bin/env python3
"""
Asigna dia_atencion (Lunes–Sábado) a clientes con georreferencia, por vendedor.

- Usa bsale_id como clave de actualización.
- KMeans (6 clusters) si el vendedor tiene ≥6 clientes; reparto por módulo si tiene menos.

Requisitos: pip install pandas scikit-learn psycopg2-binary python-dotenv
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
from collections import defaultdict

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sklearn.cluster import KMeans

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
    return [DIAS[i % 6] for i in range(n)]


def assign_days_kmeans(lat: np.ndarray, lon: np.ndarray) -> list[str]:
    """KMeans con 6 clusters; días asignados según orden geográfico de centroides (norte→sur, luego lon)."""
    n = len(lat)
    if n == 0:
        return []
    coords = np.column_stack([lat, lon])
    k = min(6, n)
    if k < 6:
        return assign_days_modulo(n)

    kmeans = KMeans(n_clusters=6, random_state=42, n_init=10)
    labels = kmeans.fit_predict(coords)
    centers = kmeans.cluster_centers_

    # Orden estable: latitud descendente (norte primero), luego longitud ascendente.
    # lexsort: última fila del tuple es la clave primaria → (-lat, lon).
    sort_order = np.lexsort((centers[:, 1], -centers[:, 0]))
    rank_by_cluster = {int(cluster_idx): rank for rank, cluster_idx in enumerate(sort_order)}

    return [DIAS[rank_by_cluster[int(lab)]] for lab in labels]


def process_vendor(sub: pd.DataFrame) -> pd.DataFrame:
    """Devuelve sub con columna dia_atencion."""
    sub = sub.copy()
    n = len(sub)
    if n == 0:
        sub["dia_atencion"] = []
        return sub

    lat = sub["lat"].astype(float).values
    lon = sub["lon"].astype(float).values

    if n >= 6:
        dias = assign_days_kmeans(lat, lon)
    else:
        dias = assign_days_modulo(n)

    sub["dia_atencion"] = dias
    return sub


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

    # Log clientes por vendedor
    counts = df.groupby("vendedor").size().sort_values(ascending=False)
    log.info("Clientes por vendedor (top 20):")
    for v, c in counts.head(20).items():
        log.info("  %s: %d", v, c)
    if len(counts) > 20:
        log.info("  ... (%d vendedores en total)", len(counts))

    out_frames: list[pd.DataFrame] = []
    for vendedor, sub in df.groupby("vendedor", sort=True):
        processed = process_vendor(sub)
        out_frames.append(processed)
        log.info(
            "Vendedor %r: %d clientes → %s",
            vendedor,
            len(processed),
            "KMeans(6)" if len(processed) >= 6 else "módulo 6",
        )

    result = pd.concat(out_frames, ignore_index=True)
    updates = list(
        zip(result["dia_atencion"].tolist(), result["bsale_id"].tolist()),
    )

    log.info("Total registros a actualizar: %d", len(updates))

    # Distribución por día y vendedor
    dist = (
        result.groupby(["vendedor", "dia_atencion"])
        .size()
        .reset_index(name="cantidad")
        .sort_values(["vendedor", "dia_atencion"])
    )
    log.info("Distribución por vendedor y día (fragmento, hasta 40 filas):")
    with pd.option_context("display.max_rows", 40, "display.width", 120):
        log.info("\n%s", dist.head(40).to_string(index=False))

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

    return 0


if __name__ == "__main__":
    sys.exit(main())
