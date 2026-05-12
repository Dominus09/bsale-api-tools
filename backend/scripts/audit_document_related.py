#!/usr/bin/env python3
"""
Auditoría READ-ONLY de ``distribuidora.document_related``.

Ejecuta consultas de integridad, cuenta anomalías y exporta métricas a JSON y Excel.
No ejecuta DELETE, TRUNCATE ni INSERT.

Uso (desde la raíz del repo, con variables PG_* o .env y dependencias instaladas, p. ej. ``pandas`` / ``openpyxl``):

    python backend/scripts/audit_document_related.py

Salidas por defecto:

    exports/document_related_audit.json
    exports/document_related_audit.xlsx

Tipos relacionados considerados válidos (alineado con ``sync_related_service``): 1, 6, 9.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# Raíz del repositorio (…/backend/scripts → …/)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

from backend.db import get_connection

RELATED_TYPES_ALLOWED = frozenset({1, 6, 9})
OC_TYPE = 33

RECONSTRUCTION_PLAN_LINES = [
    "=== FASE 3 — Plan técnico de reconstrucción completa (NO ejecutar sin ventana aprobada) ===",
    "",
    "Objetivo: que document_related refleje solo filas reproducibles vía",
    "details.json → detail.id → GET /v1/documents.json?relateddetailid= (sync_related_service).",
    "",
    "1) Pre-requisitos",
    "   - Backup lógico de document_related (pg_dump -t o COPY … TO).",
    "   - BSALE_TOKEN válido; API estable; mismo COMPANY_ID/OFFICE_ID que el sync.",
    "   - Ejecutar este script de auditoría y revisar hojas Excel antes de escribir.",
    "",
    "2) Advisory lock",
    "   - Usar el mismo advisory lock que sync_related_service (ADVISORY_LOCK_RELATED)",
    "     para impedir corridas concurrentes de sync related mientras se vacía/repuebla.",
    "",
    "3) Truncate vs staging",
    "   - Opción A (simple): TRUNCATE distribuidora.document_related; luego repoblado.",
    "     Riesgo: ventana sin filas; vistas ERP muestran “sin factura” hasta terminar el repoblado.",
    "   - Opción B (staging): CREATE TABLE …_new LIKE …; repoblar en _new; validar conteos;",
    "     BEGIN; LOCK TABLE … IN EXCLUSIVE MODE; swap (rename) o DELETE+INSERT desde staging;",
    "     COMMIT. Más pasos pero permite validación previa.",
    "",
    "4) Batch sync por ventana de fecha",
    "   - Llamar sync_related_documents_range(start_date, end_date) día a día o en bloques",
    "     (UTC según implementación) para no saturar API y permitir checkpoints.",
    "   - Registrar sync_status sync_type='related' como ya hace el servicio.",
    "",
    "5) Retry / deadlock",
    "   - Reutilizar sync_related_service (incluye _with_deadlock_retry en inserts).",
    "   - Si un día falla, reanudar desde ese día sin TRUNCATE adicional (INSERT … ON CONFLICT).",
    "",
    "6) Validación final",
    "   - Re-ejecutar este script: anomalías en cero (salvo datos Bsale inconsistentes).",
    "   - Muestreo manual: N OCs con factura en ERP vs EXISTS en v_orders_purchase_status.",
    "   - Comparar COUNT(document_related) con expectativa de volumen por negocio.",
    "",
    "7) Fuera de alcance de este script",
    "   - No ejecutar TRUNCATE/DELETE/INSERT masivo desde audit_document_related.",
]


def _rows(cur) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    openpyxl no admite datetimes con timezone. Convierte columnas datetime
    con tz a naive (UTC) antes de ``to_excel``. Columnas no datetime: sin cambios.
    """
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            tz = getattr(s.dtype, "tz", None)
            if tz is not None:
                # Serie tz-aware: primero UTC, luego naive (``tz_localize(None)`` solo no aplica en aware).
                out[col] = s.dt.tz_convert("UTC").dt.tz_localize(None)
            continue
        if s.dtype != object:
            continue
        sample = s.dropna().head(20)
        needs_strip = any(
            isinstance(v, datetime) and v.tzinfo is not None for v in sample
        )
        if not needs_strip:
            continue

        def _cell_naive_utc(v: Any) -> Any:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return v
            if isinstance(v, datetime) and v.tzinfo is not None:
                return v.astimezone(timezone.utc).replace(tzinfo=None)
            return v

        out[col] = s.map(_cell_naive_utc)
    return out


def _scalar(cur, sql: str, params: tuple | None = None) -> Any:
    cur.execute(sql, params or ())
    row = cur.fetchone()
    return row[0] if row else None


def run_audit(cur) -> dict[str, Any]:
    total = int(_scalar(cur, "SELECT COUNT(*) FROM distribuidora.document_related") or 0)

    cur.execute(
        """
        SELECT dr.id, dr.detail_id, dr.related_document_id, dr.related_document_type, dr.created_at,
               dd.document_id AS parent_document_id, d.document_type_id AS parent_type
        FROM distribuidora.document_related dr
        INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
        INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
        WHERE d.document_type_id <> %s
        ORDER BY dr.id
        """,
        (OC_TYPE,),
    )
    invalid_parent_type = _rows(cur)

    cur.execute(
        """
        SELECT dr.id, dr.detail_id, dr.related_document_id, dr.related_document_type, dr.created_at
        FROM distribuidora.document_related dr
        WHERE dr.related_document_type NOT IN (1, 6, 9)
        ORDER BY dr.id
        """
    )
    invalid_related_types = _rows(cur)

    cur.execute(
        """
        SELECT dr.id, dr.detail_id, dr.related_document_id, dr.related_document_type, dr.created_at
        FROM distribuidora.document_related dr
        LEFT JOIN distribuidora.documents rel ON rel.document_id = dr.related_document_id
        WHERE rel.document_id IS NULL
        ORDER BY dr.id
        """
    )
    orphan_related_documents = _rows(cur)

    cur.execute(
        """
        SELECT dr.id, dr.detail_id, dr.related_document_id, dr.related_document_type, dr.created_at,
               oc.document_id AS oc_document_id, oc.company_id AS oc_company_id, oc.office_id AS oc_office_id,
               rel.company_id AS related_company_id, rel.office_id AS related_office_id
        FROM distribuidora.document_related dr
        INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
        INNER JOIN distribuidora.documents oc ON oc.document_id = dd.document_id
        INNER JOIN distribuidora.documents rel ON rel.document_id = dr.related_document_id
        WHERE oc.company_id IS DISTINCT FROM rel.company_id
           OR oc.office_id IS DISTINCT FROM rel.office_id
        ORDER BY dr.id
        """
    )
    cross_office_relations = _rows(cur)

    cur.execute(
        """
        SELECT dr.id, dr.detail_id, dr.related_document_id,
               dr.related_document_type AS stored_related_type,
               rel.document_type_id AS actual_document_type_id,
               dr.created_at
        FROM distribuidora.document_related dr
        INNER JOIN distribuidora.documents rel ON rel.document_id = dr.related_document_id
        WHERE dr.related_document_type IS DISTINCT FROM rel.document_type_id
        ORDER BY dr.id
        """
    )
    type_mismatches = _rows(cur)

    cur.execute(
        """
        SELECT dr.id, dr.detail_id, dr.related_document_id, dr.related_document_type, dr.created_at
        FROM distribuidora.document_related dr
        LEFT JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
        WHERE dd.detail_id IS NULL
        ORDER BY dr.id
        """
    )
    orphan_detail_rows = _rows(cur)

    cur.execute(
        """
        SELECT detail_id, related_document_id, COUNT(*) AS row_count
        FROM distribuidora.document_related
        GROUP BY detail_id, related_document_id
        HAVING COUNT(*) > 1
        """
    )
    duplicate_pairs = _rows(cur)

    cur.execute(
        """
        SELECT related_document_type, COUNT(*)::bigint AS n
        FROM distribuidora.document_related
        GROUP BY related_document_type
        ORDER BY n DESC
        """
    )
    related_type_distribution = _rows(cur)

    metrics = {
        "total_rows": total,
        "count_invalid_parent_type": len(invalid_parent_type),
        "count_invalid_related_types": len(invalid_related_types),
        "count_orphan_related_documents": len(orphan_related_documents),
        "count_cross_office_relations": len(cross_office_relations),
        "count_type_mismatches": len(type_mismatches),
        "count_orphan_detail_id": len(orphan_detail_rows),
        "count_duplicate_logical_pairs": len(duplicate_pairs),
        "related_type_distribution": related_type_distribution,
    }

    return {
        "metrics": metrics,
        "invalid_parent_type": invalid_parent_type,
        "invalid_related_types": invalid_related_types,
        "orphan_related_documents": orphan_related_documents,
        "cross_office_relations": cross_office_relations,
        "type_mismatches": type_mismatches,
        "orphan_detail_rows": orphan_detail_rows,
        "duplicate_pairs": duplicate_pairs,
    }


def _summary_dataframe(metrics: dict[str, Any]) -> pd.DataFrame:
    rows_out = [
        ("total_rows", metrics["total_rows"]),
        ("count_invalid_parent_type", metrics["count_invalid_parent_type"]),
        ("count_invalid_related_types", metrics["count_invalid_related_types"]),
        ("count_orphan_related_documents", metrics["count_orphan_related_documents"]),
        ("count_cross_office_relations", metrics["count_cross_office_relations"]),
        ("count_type_mismatches", metrics["count_type_mismatches"]),
        ("count_orphan_detail_id", metrics["count_orphan_detail_id"]),
        ("count_duplicate_logical_pairs", metrics["count_duplicate_logical_pairs"]),
        ("related_types_allowed", ",".join(str(x) for x in sorted(RELATED_TYPES_ALLOWED))),
        ("expected_oc_parent_type", OC_TYPE),
    ]
    dist = metrics.get("related_type_distribution") or []
    for d in dist:
        rows_out.append(
            (f"type_{d.get('related_document_type')}_count", int(d.get("n") or 0)),
        )
    return pd.DataFrame(rows_out, columns=["metric", "value"])


def _write_excel(path: Path, audit: dict[str, Any], metrics: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary_df = _make_excel_safe(_summary_dataframe(metrics))

    def _df(key: str) -> pd.DataFrame:
        data = audit.get(key) or []
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)
        _make_excel_safe(_df("invalid_parent_type")).to_excel(
            writer, sheet_name="invalid_parent_type", index=False
        )
        _make_excel_safe(_df("invalid_related_types")).to_excel(
            writer, sheet_name="invalid_related_types", index=False
        )
        _make_excel_safe(_df("orphan_related_documents")).to_excel(
            writer, sheet_name="orphan_related_documents", index=False
        )
        _make_excel_safe(_df("cross_office_relations")).to_excel(
            writer, sheet_name="cross_office_relations", index=False
        )
        _make_excel_safe(_df("type_mismatches")).to_excel(
            writer, sheet_name="type_mismatches", index=False
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Auditoría read-only de document_related.")
    parser.add_argument(
        "--json-out",
        type=Path,
        default=REPO_ROOT / "exports" / "document_related_audit.json",
        help="Ruta del JSON de métricas.",
    )
    parser.add_argument(
        "--xlsx-out",
        type=Path,
        default=REPO_ROOT / "exports" / "document_related_audit.xlsx",
        help="Ruta del Excel de detalle.",
    )
    parser.add_argument(
        "--no-plan-print",
        action="store_true",
        help="No imprimir el plan de reconstrucción en consola.",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    load_dotenv()

    conn = get_connection()
    try:
        cur = conn.cursor()
        audit = run_audit(cur)
    finally:
        conn.close()

    metrics = audit["metrics"]
    generated_at = datetime.now(timezone.utc).isoformat()

    json_payload: dict[str, Any] = {
        "generated_at_utc": generated_at,
        "metrics": metrics,
        "reconstruction_plan_notes": RECONSTRUCTION_PLAN_LINES,
    }

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.json_out, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, indent=2, ensure_ascii=False, default=str)

    _write_excel(args.xlsx_out, audit, metrics)

    print(f"JSON:  {args.json_out.resolve()}")
    print(f"Excel: {args.xlsx_out.resolve()}")
    print("Métricas:", json.dumps(metrics, indent=2, ensure_ascii=False, default=str))

    if not args.no_plan_print:
        print()
        for line in RECONSTRUCTION_PLAN_LINES:
            print(line)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
