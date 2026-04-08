#!/usr/bin/env python3
"""
Asigna dia_atencion según calendario fijo por vendedor y municipality (reglas de negocio).

- Usa bsale_id como clave de actualización.
- Normaliza municipality (trim, minúsculas, sin tildes) para comparar.
- Reparte geográficamente (lat, lon) cuando una comuna cae en varios días.
- Casos especiales: vendedor_1 quellón sábado máx. 12; vendedor_2 excluye Melinka de ruta física;
  vendedor_3 sábado castro/chonchi según reglas por comuna.

Requisitos: pip install pandas psycopg2-binary python-dotenv
Variables de entorno: PG_HOST, PG_DB, PG_USER, PG_PASSWORD

Ejecución:
  python generate_rutero.py
  python generate_rutero.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import unicodedata

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
SELECT bsale_id, vendedor, municipality, lat, lon
FROM bsale.clients
WHERE vendedor IS NOT NULL
  AND municipality IS NOT NULL
  AND lat IS NOT NULL
  AND lon IS NOT NULL
  AND TRIM(COALESCE(vendedor::text, '')) <> ''
  AND TRIM(COALESCE(municipality::text, '')) <> ''
"""

UPDATE_SQL = """
UPDATE bsale.clients
SET dia_atencion = %s
WHERE bsale_id = %s
"""


def norm_municipality(s: str) -> str:
    """trim, lower, sin marcas diacríticas (á->a, ñ->n)."""
    t = str(s).strip().lower()
    if not t:
        return t
    nk = unicodedata.normalize("NFD", t)
    t = "".join(c for c in nk if unicodedata.category(c) != "Mn")
    # Sinónimos → clave usada en VENDOR_MUNI_DAYS
    if t == "isla puqueldon":
        return "puqueldon"
    return t


def norm_vendedor(s: str) -> str:
    return str(s).strip().lower()


def connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", "5432"),
    )


# municipality normalizado -> lista de días (orden importa para troceo geográfico)
VENDOR_MUNI_DAYS: dict[str, dict[str, list[str]]] = {
    "vendedor_1": {
        "puqueldon": ["Lunes"],
        "quellon": ["Martes", "Miércoles", "Jueves", "Viernes", "Sábado"],
    },
    "vendedor_2": {
        "castro": ["Lunes", "Viernes", "Sábado"],
        "dalcahue": ["Martes", "Jueves"],
        "chonchi": ["Miércoles"],
    },
    "vendedor_3": {
        "castro": ["Lunes", "Viernes", "Sábado"],
        "queilen": ["Martes"],
        "chonchi": ["Miércoles", "Sábado"],
        "achao": ["Jueves"],
    },
    "vendedor_4": {
        "ancud": ["Lunes", "Martes", "Viernes", "Sábado"],
        "dalcahue": ["Miércoles"],
        "quemchi": ["Jueves"],
    },
}

# (vendedor, municipality_norm) excluidos de asignación automática (no ruta física / manual)
EXCLUDED_MUNI_BY_VENDOR: dict[str, set[str]] = {
    "vendedor_2": {"melinka"},
}

# Tras asignación inicial: tope por día y comuna, y días de destino del excedente
SAT_CAP_RULES: list[dict] = [
    {
        "vendor": "vendedor_1",
        "muni": "quellon",
        "day": "Sábado",
        "max_clients": 12,
        "overflow_days": ["Martes", "Miércoles", "Jueves", "Viernes"],
    },
]


def geo_assign_equal_chunks(sub: pd.DataFrame, days: list[str]) -> pd.Series:
    """
    sub: filas de un mismo vendedor+comuna; debe tener lat, lon.
    Orden estable lat/lon y reparto en len(days) trozos contiguos de tamaños lo más iguales posible.
    Devuelve Series dia_atencion alineada al índice de sub.
    """
    if sub.empty:
        return pd.Series(dtype=object, index=sub.index)
    sorted_sub = sub.sort_values(by=["lat", "lon"], kind="mergesort")
    n = len(sorted_sub)
    k = len(days)
    base = n // k
    rem = n % k
    out_list: list[str] = []
    for i, d in enumerate(days):
        take = base + (1 if i < rem else 0)
        out_list.extend([d] * take)
    assert len(out_list) == n
    ser = pd.Series(out_list, index=sorted_sub.index)
    return ser.reindex(sub.index)


def apply_sat_cap_overflow(df: pd.DataFrame, rule: dict) -> pd.DataFrame:
    """Ajusta dia_atencion en df (copia) para regla de tope en un día."""
    vn = rule["vendor"]
    muni = rule["muni"]
    day_cap = rule["day"]
    max_c = rule["max_clients"]
    targets = rule["overflow_days"]

    mask = (
        (df["vendedor_norm"] == vn)
        & (df["municipality_norm"] == muni)
        & (df["dia_atencion"] == day_cap)
    )
    sat_rows = df.loc[mask]
    if len(sat_rows) <= max_c:
        return df

    sat_sorted = sat_rows.sort_values(by=["lat", "lon"], kind="mergesort")
    keep_idx = sat_sorted.index[:max_c]
    excess_idx = sat_sorted.index[max_c:]

    excess = df.loc[excess_idx].copy()
    new_days = geo_assign_equal_chunks(excess, targets)
    df.loc[excess_idx, "dia_atencion"] = new_days.loc[excess_idx]
    log.info(
        "  Tope %s %s %s: %d clientes; %d reasignados a %s",
        vn,
        muni,
        day_cap,
        max_c,
        len(excess_idx),
        targets,
    )
    return df


def assign_vendor_calendar(sub: pd.DataFrame, vendor_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    sub: clientes de un vendedor con municipality_norm, lat, lon.
    Devuelve (asignados, no_asignados) con columna dia_atencion en asignados.
    """
    rules = VENDOR_MUNI_DAYS.get(vendor_key)
    if rules is None:
        na = sub.copy()
        na["dia_atencion"] = pd.NA
        log.warning("Vendedor %r sin reglas de calendario; %d clientes sin asignar.", vendor_key, len(na))
        return sub.iloc[0:0].copy(), na

    excluded = EXCLUDED_MUNI_BY_VENDOR.get(vendor_key, set())
    assigned_parts: list[pd.Series] = []
    unassigned_parts: list[pd.DataFrame] = []

    for muni_norm, group in sub.groupby("municipality_norm", sort=False):
        if muni_norm in excluded:
            u = group.copy()
            u["dia_atencion"] = pd.NA
            unassigned_parts.append(u)
            log.info(
                "  %d clientes municipality=%r excluidos (no ruta física / manual).",
                len(group),
                muni_norm,
            )
            continue
        days = rules.get(muni_norm)
        if days is None:
            u = group.copy()
            u["dia_atencion"] = pd.NA
            unassigned_parts.append(u)
            log.warning(
                "  %d clientes: municipality=%r sin regla para %r.",
                len(group),
                muni_norm,
                vendor_key,
            )
            continue
        if len(days) == 1:
            s = pd.Series(days[0], index=group.index)
        else:
            s = geo_assign_equal_chunks(group, days)
        assigned_parts.append(s)

    if not assigned_parts:
        if unassigned_parts:
            unassigned_df = pd.concat(unassigned_parts, ignore_index=False).sort_index()
        else:
            unassigned_df = sub.copy()
            unassigned_df["dia_atencion"] = pd.NA
        return sub.iloc[0:0].copy(), unassigned_df

    dia_series = pd.concat(assigned_parts).sort_index()
    assigned_df = sub.loc[dia_series.index].copy()
    assigned_df["dia_atencion"] = dia_series

    if unassigned_parts:
        unassigned_df = pd.concat(unassigned_parts, ignore_index=False).sort_index()
    else:
        unassigned_df = sub.iloc[0:0].copy()
        unassigned_df["dia_atencion"] = pd.NA

    return assigned_df, unassigned_df


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Asignar dia_atencion por calendario fijo vendedor + municipality.",
    )
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
        log.warning("No hay filas que cumplan los filtros.")
        return 0

    df["vendedor"] = df["vendedor"].astype(str).str.strip()
    df["municipality"] = df["municipality"].astype(str).str.strip()
    df["vendedor_norm"] = df["vendedor"].map(norm_vendedor)
    df["municipality_norm"] = df["municipality"].map(norm_municipality)
    df = df.dropna(subset=["lat", "lon", "bsale_id"])

    n_read = len(df)
    log.info("Total clientes leídos: %d", n_read)

    log.info("--- Por vendedor ---")
    for v, c in df.groupby("vendedor_norm").size().sort_values(ascending=False).items():
        log.info("  %s: %d", v, c)

    log.info("--- Por vendedor + municipality (normalizado) ---")
    vm = df.groupby(["vendedor_norm", "municipality_norm"]).size().reset_index(name="cantidad")
    for _, r in vm.sort_values(["vendedor_norm", "municipality_norm"]).iterrows():
        log.info("  %s | %s: %d", r["vendedor_norm"], r["municipality_norm"], r["cantidad"])

    assigned_list: list[pd.DataFrame] = []
    unassigned_list: list[pd.DataFrame] = []

    for vendor_key, sub in df.groupby("vendedor_norm", sort=True):
        log.info("Procesando %r (%d clientes)...", vendor_key, len(sub))
        a, u = assign_vendor_calendar(sub, vendor_key)
        if not a.empty:
            assigned_list.append(a)
        if not u.empty:
            unassigned_list.append(u)

    if assigned_list:
        result = pd.concat(assigned_list, ignore_index=False)
    else:
        result = df.iloc[0:0].copy()

    for rule in SAT_CAP_RULES:
        if rule["vendor"] in result["vendedor_norm"].values:
            result = apply_sat_cap_overflow(result, rule)

    if unassigned_list:
        unassigned = pd.concat(unassigned_list, ignore_index=False)
        unassigned = unassigned[~unassigned.index.duplicated(keep="first")]
    else:
        unassigned = df.iloc[0:0].copy()

    log.info("--- Por vendedor + dia_atencion (tras reglas y topes) ---")
    if not result.empty:
        vd = result.groupby(["vendedor_norm", "dia_atencion"]).size().reset_index(name="cantidad")
        for _, r in vd.sort_values(["vendedor_norm", "dia_atencion"]).iterrows():
            log.info("  %s | %s: %d", r["vendedor_norm"], r["dia_atencion"], r["cantidad"])
    else:
        log.info("  (sin asignaciones)")

    n_unassigned = len(unassigned)
    if n_unassigned:
        log.warning("Clientes sin asignar: %d", n_unassigned)
        unknown_muni = unassigned[
            unassigned["municipality_norm"].notna()
            & ~unassigned.apply(
                lambda r: r["municipality_norm"] in VENDOR_MUNI_DAYS.get(r["vendedor_norm"], {}),
                axis=1,
            )
        ]
        excl = unassigned[
            unassigned.apply(
                lambda r: r["municipality_norm"] in EXCLUDED_MUNI_BY_VENDOR.get(r["vendedor_norm"], set()),
                axis=1,
            )
        ]
        if len(excl):
            log.info("  Excluidos por regla (ej. Melinka): %d", len(excl))
        if len(unknown_muni):
            log.warning(
                "  Municipality sin regla para ese vendedor: %d",
                len(unknown_muni),
            )
            for vn, g in unknown_muni.groupby("vendedor_norm"):
                for m, g2 in g.groupby("municipality_norm"):
                    log.warning("    %s + %s: %d clientes", vn, m, len(g2))
    else:
        log.info("Todos los clientes leídos quedaron con dia_atencion.")

    updates = []
    if not result.empty:
        updates = list(zip(result["dia_atencion"].tolist(), result["bsale_id"].tolist()))

    log.info("Registros a actualizar en BD: %d", len(updates))

    summary = (
        result.groupby(["vendedor", "municipality", "dia_atencion"])
        .size()
        .reset_index(name="cantidad")
        .sort_values(["vendedor", "municipality", "dia_atencion"])
    )

    if args.dry_run:
        log.info("Dry-run: no se aplicaron cambios en la base de datos.")
    elif updates:
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

    print("\n--- Resumen: vendedor | municipality | dia_atencion | cantidad ---")
    if summary.empty:
        print("(vacío)")
    else:
        print(summary.to_string(index=False))

    print("\n--- Clientes sin asignación (bsale_id, vendedor, municipality) ---")
    if unassigned.empty:
        print("(ninguno)")
    else:
        cols = ["bsale_id", "vendedor", "municipality"]
        print(unassigned[cols].to_string(index=False))

    print(f"\nTotal leídos: {n_read} | Asignados (UPDATE): {len(updates)} | Sin asignar: {n_unassigned}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
