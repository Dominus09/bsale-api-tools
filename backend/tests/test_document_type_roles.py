"""Roles ERP de tipos de documento Bsale (049 + helper ``document_type_roles``).

049 se ejecuta en SQLite (schemas ``distribuidora`` y ``bsale`` adjuntos) con el mismo
troceo del runner; el helper corre contra ese SQLite vía un cursor mínimo.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

from backend.repositories.distribuidora import document_type_roles as dtr
from backend.repositories.distribuidora import sync_repo
from backend.repositories.distribuidora.document_type_roles import (
    DOCUMENTOS_EVIDENCIA_RELACION,
    DOCUMENTOS_FACTURACION,
    DOCUMENTOS_RELACION_PERMITIDA,
    DOCUMENTOS_VENTA_NETA,
    DocumentRole,
    DocumentTypeRoleMissing,
)

BACKEND = Path(sync_repo.__file__).resolve().parents[2]
SQL_DIR = BACKEND / "sql" / "distribuidora"
ROLES_FILE = "049_document_type_roles.sql"
ROLES_SQL = (SQL_DIR / ROLES_FILE).read_text(encoding="utf-8")

ROLES_ES = {"BOLETA", "FACTURA", "GUIA_DESPACHO", "NOTA_CREDITO", "COTIZACION", "ORDEN_COMPRA"}
ROLES_EN = {"RECEIPT", "INVOICE", "DISPATCH_GUIDE", "CREDIT_NOTE", "QUOTE", "PURCHASE_ORDER"}

SEED_ACTIVE_C3 = {
    (1, "BOLETA", 39),
    (6, "FACTURA", 33),
    (8, "GUIA_DESPACHO", 52),
    (9, "NOTA_CREDITO", 61),
    (26, "COTIZACION", None),
    (33, "ORDEN_COMPRA", None),
}
SEED_LEGACY_C3 = {
    (10, "BOLETA"),
    (15, "FACTURA"),
    (27, "BOLETA"),
    (30, "ORDEN_COMPRA"),
    (32, "NOTA_CREDITO"),
}

# Metadata Bsale company 3 tal como está almacenada (incluye espacio final en 23).
BSALE_DOCUMENT_TYPES_C3 = {
    1: ("BOLETA ELECTRÓNICA T", 39),
    6: ("FACTURA ELECTRÓNICA T", 33),
    8: ("GUÍA DE DESPACHO ELECTRÓNICA", 52),
    9: ("NOTA DE CRÉDITO ELECTRÓNICA T", 61),
    10: ("BOLETA MANUAL", 35),
    15: ("FACTURA NO AFECTA O EXENTA ELECTRÓNICA", 34),
    23: ("NOTA VENTA ", None),
    25: ("VALE VENTA", None),
    26: ("COTIZACION", None),
    27: ("BOLETA EXENTA ELECTRONICA T", 41),
    30: ("ORDEN DE COMPRA VIEJA", None),
    32: ("nota de credito manual", None),
    33: ("ORDEN DE COMPRA", None),
}


# ------------------------------------------------------------------ SQLite


def _sqlite_chunks(sql: str) -> list[str]:
    out = []
    for chunk in sync_repo._STMT_SPLIT_GO.split(sql):
        if not sync_repo._sql_chunk_has_executable_sql(chunk):
            continue
        if re.search(r"^\s*COMMENT\s+ON", chunk, re.M | re.I):
            continue
        out.append(chunk.replace("NOW()", "CURRENT_TIMESTAMP"))
    return out


def _apply_049(conn: sqlite3.Connection) -> None:
    for chunk in _sqlite_chunks(ROLES_SQL):
        conn.execute(chunk)
    conn.commit()


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH DATABASE ':memory:' AS distribuidora")
    conn.execute("ATTACH DATABASE ':memory:' AS bsale")
    conn.execute(
        "CREATE TABLE bsale.document_types (company_id INTEGER, bsale_id INTEGER, name TEXT, "
        "code_sii INTEGER, PRIMARY KEY (company_id, bsale_id))"
    )
    conn.executemany(
        "INSERT INTO bsale.document_types VALUES (3, ?, ?, ?)",
        [(k, n, c) for k, (n, c) in BSALE_DOCUMENT_TYPES_C3.items()],
    )
    conn.commit()
    _apply_049(conn)
    yield conn
    conn.close()


class _Cur:
    """Cursor psycopg-like sobre SQLite para el SQL del helper."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.queries: list[str] = []
        self._rows: list[tuple] = []

    def execute(self, sql, params=()):
        self.queries.append(sql)
        params = list(params)
        if "to_regclass" in sql:
            schema, table = str(params[0]).split(".", 1)
            found = self.conn.execute(
                f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?", (table,)
            ).fetchone()
            self._rows = [(found is not None,)]
            return
        if "= ANY(%s)" in sql:
            values = list(params.pop())
            sql = sql.replace("= ANY(%s)", f"IN ({','.join('?' * len(values))})")
            params += values
        self._rows = self.conn.execute(sql.replace("%s", "?"), params).fetchall()

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


@pytest.fixture
def cur(db):
    dtr.clear_cache()
    yield _Cur(db)
    dtr.clear_cache()


def _rows(conn) -> set[tuple]:
    return set(
        conn.execute(
            "SELECT company_id, document_type_id, role, active, expected_code_sii "
            "FROM distribuidora.document_type_roles"
        ).fetchall()
    )


# ------------------------------------------------------------- migración


def test_049_registered_after_048_and_last():
    files = sync_repo.DISTRIBUIDORA_SCHEMA_FILES
    assert files[-1] == ROLES_FILE
    assert files.index("048_document_reissue_lineage.sql") == len(files) - 2


def test_049_does_not_touch_bsale_metadata():
    body = re.sub(r"'[^']*'", "''", re.sub(r"--[^\n]*", "", ROLES_SQL))
    assert "bsale." not in body and "bsale_raw." not in body
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|ALTER|DROP)\b[^;]*\bbsale(_raw)?\.", body, re.I)
    assert not re.search(r"\b(UPDATE|DELETE|DROP|TRUNCATE)\b", body, re.I)


def test_049_is_idempotent_and_never_overwrites_manual_changes(db):
    first = _rows(db)
    _apply_049(db)
    assert _rows(db) == first
    db.execute(
        "UPDATE distribuidora.document_type_roles SET active = 1 "
        "WHERE company_id = 3 AND document_type_id = 10"
    )
    db.commit()
    _apply_049(db)
    assert (3, 10, "BOLETA", 1, 35) in _rows(db)


def test_role_table_has_no_name_column():
    create = ROLES_SQL.split("CREATE TABLE IF NOT EXISTS distribuidora.document_type_roles", 1)[1]
    create = create.split("-- +go", 1)[0]
    assert not re.search(r"^\s*(name|nombre|label|display_name)\b", create, re.M | re.I)
    assert "notes" in create


# ------------------------------------------------------------------ roles


def test_roles_are_spanish_everywhere():
    assert {r.value for r in DocumentRole} == ROLES_ES
    check = re.search(r"CHECK \(role IN \((.*?)\)\)", ROLES_SQL, re.S).group(1)
    assert set(re.findall(r"'(\w+)'", check)) == ROLES_ES
    seeded = set(re.findall(r"\(\s*\d+,\s*\d+,\s*'(\w+)'", ROLES_SQL))
    assert seeded <= ROLES_ES
    assert not (ROLES_EN & (set(re.findall(r"'(\w+)'", ROLES_SQL)) | {r.value for r in DocumentRole}))


@pytest.mark.parametrize("english", sorted(ROLES_EN))
def test_english_role_rejected_by_enum_helper_and_check(db, cur, english):
    with pytest.raises(ValueError):
        DocumentRole(english)
    with pytest.raises(ValueError):
        dtr.type_ids_for(cur, 3, english)
    with pytest.raises(ValueError):
        dtr.has_role(cur, 3, 6, english)
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO distribuidora.document_type_roles (company_id, document_type_id, role) "
            "VALUES (3, 999, ?)",
            (english,),
        )


def test_main_seed_company_3_is_exact(db):
    active = {(t, r, c) for comp, t, r, a, c in _rows(db) if comp == 3 and a}
    assert active == SEED_ACTIVE_C3


def test_legacy_variants_exist_but_inactive(db, cur):
    legacy = {(t, r) for comp, t, r, a, _ in _rows(db) if comp == 3 and not a}
    assert legacy == SEED_LEGACY_C3
    assert dtr.type_ids_for(cur, 3, DocumentRole.BOLETA) == (1,)
    assert dtr.type_ids_for(cur, 3, DocumentRole.BOLETA, include_inactive=True) == (1, 10, 27)
    assert dtr.role_of(cur, 3, 15) is None
    assert dtr.role_of(cur, 3, 15, include_inactive=True) is DocumentRole.FACTURA
    assert not dtr.has_role(cur, 3, 30, DocumentRole.ORDEN_COMPRA)


def test_vale_venta_25_has_no_role(db, cur):
    assert not [row for row in _rows(db) if row[1] == 25]
    assert dtr.role_of(cur, 3, 25) is None
    assert dtr.role_of(cur, 3, 25, include_inactive=True) is None


def test_code_sii_and_document_type_id_never_swapped(db, cur):
    rows = {t: c for comp, t, _, _, c in _rows(db) if comp == 3}
    assert rows[6] == 33
    assert rows[33] is None
    assert all(c is None or c != t for t, c in rows.items())
    assert dtr.role_of(cur, 3, 33) is DocumentRole.ORDEN_COMPRA
    assert dtr.role_of(cur, 3, 6) is DocumentRole.FACTURA
    assert not dtr.has_role(cur, 3, 33, DocumentRole.FACTURA)
    for type_id, code in rows.items():
        if code is not None:
            assert BSALE_DOCUMENT_TYPES_C3[type_id][1] == code, type_id


def test_seed_types_exist_in_bsale_metadata(db):
    for _, type_id, *_ in _rows(db):
        assert type_id in BSALE_DOCUMENT_TYPES_C3


# ------------------------------------------------------------------ helper


def test_groups_are_explicit_and_distinct():
    assert DOCUMENTOS_FACTURACION == (DocumentRole.BOLETA, DocumentRole.FACTURA)
    assert DOCUMENTOS_VENTA_NETA == (
        DocumentRole.BOLETA,
        DocumentRole.FACTURA,
        DocumentRole.NOTA_CREDITO,
    )
    assert DOCUMENTOS_RELACION_PERMITIDA == DOCUMENTOS_VENTA_NETA
    assert DocumentRole.FACTURA in DOCUMENTOS_FACTURACION
    assert DOCUMENTOS_FACTURACION != (DocumentRole.FACTURA,)


def test_type_ids_for_groups_company_3(cur):
    assert dtr.type_ids_for(cur, 3, DocumentRole.FACTURA) == (6,)
    assert dtr.type_ids_for(cur, 3, *DOCUMENTOS_FACTURACION) == (1, 6)
    assert dtr.type_ids_for(cur, 3, *DOCUMENTOS_VENTA_NETA) == (1, 6, 9)
    assert dtr.type_ids_for(cur, 3, DocumentRole.ORDEN_COMPRA) == (33,)


def test_guia_despacho_not_persistible_relation(cur):
    assert DocumentRole.GUIA_DESPACHO not in DOCUMENTOS_RELACION_PERMITIDA
    assert 8 not in dtr.type_ids_for(cur, 3, *DOCUMENTOS_RELACION_PERMITIDA)


def test_f1c_can_use_guia_despacho_as_evidence(cur):
    assert DocumentRole.GUIA_DESPACHO in DOCUMENTOS_EVIDENCIA_RELACION
    assert set(DOCUMENTOS_RELACION_PERMITIDA) < set(DOCUMENTOS_EVIDENCIA_RELACION)
    assert dtr.type_ids_for(cur, 3, *DOCUMENTOS_EVIDENCIA_RELACION) == (1, 6, 8, 9)
    assert dtr.has_role(cur, 3, 8, *DOCUMENTOS_EVIDENCIA_RELACION)
    assert not dtr.has_role(cur, 3, 8, *DOCUMENTOS_RELACION_PERMITIDA)


def test_identity_is_company_scoped(db, cur):
    db.execute(
        "INSERT INTO distribuidora.document_type_roles (company_id, document_type_id, role) "
        "VALUES (1, 48, 'ORDEN_COMPRA'), (1, 7, 'GUIA_DESPACHO')"
    )
    db.commit()
    assert dtr.type_ids_for(cur, 1, DocumentRole.ORDEN_COMPRA) == (48,)
    assert dtr.type_ids_for(cur, 3, DocumentRole.ORDEN_COMPRA) == (33,)
    assert dtr.role_of(cur, 1, 33) is None
    assert dtr.role_of(cur, 1, 7) is DocumentRole.GUIA_DESPACHO
    assert dtr.role_of(cur, 3, 7) is None
    with pytest.raises(DocumentTypeRoleMissing, match="company_id=2"):
        dtr.type_ids_for(cur, 2, DocumentRole.ORDEN_COMPRA)


def test_missing_role_or_table_fails_fast(db, cur):
    with pytest.raises(DocumentTypeRoleMissing, match="FACTURA"):
        dtr.type_ids_for(cur, 1, DocumentRole.FACTURA)
    db.execute("DROP TABLE distribuidora.document_type_roles")
    dtr.clear_cache()
    with pytest.raises(DocumentTypeRoleMissing, match="049_document_type_roles"):
        dtr.type_ids_for(cur, 3, DocumentRole.FACTURA)
    with pytest.raises(ValueError):
        dtr.type_ids_for(cur, 3)


def test_cache_reuses_positive_result_only(cur):
    dtr.type_ids_for(cur, 3, DocumentRole.FACTURA)
    n = len(cur.queries)
    dtr.type_ids_for(cur, 3, DocumentRole.BOLETA)
    assert len(cur.queries) == n
    dtr.role_of(cur, 4, 1)
    dtr.role_of(cur, 4, 1)
    assert len(cur.queries) == n + 4


def test_helper_is_read_only():
    src = Path(dtr.__file__).read_text(encoding="utf-8")
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)\s", src)


# ---------------------------------------------------------- nombre visible


def test_visible_name_comes_verbatim_from_bsale_document_types(cur):
    names = dtr.document_type_names(cur, 3, [1, 6, 8, 9, 26, 33, 23])
    assert names == {k: BSALE_DOCUMENT_TYPES_C3[k][0] for k in (1, 6, 8, 9, 26, 33, 23)}
    assert names[23].endswith(" ")
    assert "FROM bsale.document_types" in dtr._NAMES_SQL
    assert re.search(r"SELECT bsale_id, name\b", dtr._NAMES_SQL)
    assert not re.search(r"(upper|lower|trim|initcap|replace)\s*\(", dtr._NAMES_SQL, re.I)
    assert dtr.document_type_names(cur, 3, []) == {}


def test_role_is_not_used_as_display_name(cur):
    names = dtr.document_type_names(cur, 3, [6])
    assert names[6] != DocumentRole.FACTURA.value


# ------------------------------------------------- paridad con constantes


def test_seed_matches_current_hardcoded_constants(cur):
    from backend.config import commercial_scope
    from backend.repositories.distribuidora import document_related_repo
    from backend.services.analytics import document_source
    from backend.services.distribuidora import (
        oc_operational_status,
        oc_related_discovery_service,
        oc_source_resolver,
        probable_invoice_service,
        sync_missing_related_documents_service,
        sync_related_service,
        sync_service,
    )

    facturacion = set(dtr.type_ids_for(cur, 3, *DOCUMENTOS_FACTURACION))
    venta_neta = set(dtr.type_ids_for(cur, 3, *DOCUMENTOS_VENTA_NETA))
    relacion = set(dtr.type_ids_for(cur, 3, *DOCUMENTOS_RELACION_PERMITIDA))
    (oc,) = dtr.type_ids_for(cur, 3, DocumentRole.ORDEN_COMPRA)
    (nc,) = dtr.type_ids_for(cur, 3, DocumentRole.NOTA_CREDITO)
    (factura,) = dtr.type_ids_for(cur, 3, DocumentRole.FACTURA)
    (boleta,) = dtr.type_ids_for(cur, 3, DocumentRole.BOLETA)

    for value in (
        probable_invoice_service.DOC_TYPES_INVOICE,
        oc_operational_status.INVOICE_DOC_TYPES,
        oc_related_discovery_service.CONFIRMED_INVOICE_TYPES,
        sync_missing_related_documents_service.INVOICE_TYPES,
        commercial_scope.SALE_DOCUMENT_TYPES,
        document_source.SALE_DOCUMENT_TYPES,
    ):
        assert set(value) == facturacion
    assert set(sync_service.DOC_TYPES_SALES) == venta_neta
    assert set(commercial_scope.ALLOWED_DOCUMENT_TYPES) == venta_neta
    assert set(sync_related_service.RELATED_DOCUMENT_TYPES_ALLOWED) == relacion
    assert set(document_related_repo.ORPHAN_RELATED_DOCUMENT_TYPES) == relacion
    assert set(sync_service.DOC_TYPES_OC) == {oc}
    assert oc_source_resolver.OC_DOCUMENT_TYPE_ID == oc
    assert sync_related_service.DOC_TYPE_OC == oc
    assert oc_operational_status.CREDIT_NOTE_DOC_TYPE == nc == commercial_scope.DOC_NC
    assert set(document_source.CREDIT_NOTE_DOCUMENT_TYPES) == {nc}
    assert (commercial_scope.DOC_BOLETA, commercial_scope.DOC_FACTURA) == (boleta, factura)


def test_literals_that_cannot_use_the_table_match_orden_compra(cur):
    """Predicados de índice y scopes de bsale_raw no pueden consultar la tabla."""
    from backend.services.bsale_raw.resources import documents as raw_documents

    (oc,) = dtr.type_ids_for(cur, 3, DocumentRole.ORDEN_COMPRA)
    for fn in ("028_planning_rows_indexes.sql", "029_planning_rows_sort_index.sql"):
        assert f"company_id = 3 AND office_id = 1 AND document_type_id = {oc}" in (
            SQL_DIR / fn
        ).read_text(encoding="utf-8")
    assert raw_documents.PRIORITY_DOCUMENT_SCOPES == ((3, oc),)
    assert raw_documents.POINT_DOCUMENT_TYPE_IDS == frozenset({oc})


# ------------------------------------------- guardia contra números mágicos


_MAGIC_USE = re.compile(
    r"(document_type_id|related_document_type|doc_type(_id)?|documentTypeId|document_type|tipo_doc\w*)"
    r"\s*(=|==|!=|<>|IN|in|NOT IN|not in)\s*[\(\[\{]?\s*\d",
    re.I,
)
_MAGIC_CONST = re.compile(
    r"^\s*_?[A-Z0-9_]*(DOC|TYPE)[A-Z0-9_]*\s*(:[^=]+)?=\s*(frozenset|tuple|set)?\(?\s*[\(\[\{]?\s*\d{1,2}\b"
)
_MAGIC_EXCLUDED_PREFIXES = ("backend/tests/", "backend/debug/", "backend/sql/diagnostics/")

# Inventario al introducir 049. Solo puede bajar: lo nuevo debe usar document_type_roles.
MAGIC_NUMBER_BASELINE = {
    "backend/audits/audit_document_related.py": 3,
    "backend/config/commercial_scope.py": 5,
    "backend/jobs/diagnose_oc_bsale_vs_pg.py": 1,
    "backend/jobs/diagnose_oc_header_drift.py": 3,
    "backend/jobs/diagnose_oc_operational_status_45d.py": 11,
    "backend/jobs/rebuild_order_weight_snapshots.py": 1,
    "backend/maintenance/cleanup_document_related_invalid_types.py": 7,
    "backend/repositories/distribuidora/dispatch_plan_load_batch_repo.py": 1,
    "backend/repositories/distribuidora/document_related_repo.py": 4,
    "backend/services/analytics/cost_tax_resolution.py": 1,
    "backend/services/analytics/document_source.py": 3,
    "backend/services/bsale_raw/resources/documents.py": 4,
    "backend/services/commercial_analytics_validation.py": 2,
    "backend/services/distribuidora/clientes_analisis_completo_service.py": 7,
    "backend/services/distribuidora/dispatch_commercial_margin_service.py": 2,
    "backend/services/distribuidora/dispatch_planning_list_service.py": 3,
    "backend/services/distribuidora/document_relation_sync_service.py": 12,
    "backend/services/distribuidora/live_sync_service.py": 2,
    "backend/services/distribuidora/oc_document_chain_resolver.py": 3,
    "backend/services/distribuidora/oc_operational_status.py": 3,
    "backend/services/distribuidora/oc_reconciliation_service.py": 3,
    "backend/services/distribuidora/oc_related_discovery_service.py": 3,
    "backend/services/distribuidora/oc_source_resolver.py": 1,
    "backend/services/distribuidora/orders_service.py": 10,
    "backend/services/distribuidora/probable_invoice_service.py": 7,
    "backend/services/distribuidora/sync_missing_related_documents_service.py": 1,
    "backend/services/distribuidora/sync_related_service.py": 3,
    "backend/services/distribuidora/sync_service.py": 12,
    "backend/services/logistics_weight_audit_service.py": 2,
    "backend/services/order_weight_service.py": 5,
    "backend/services/returns_analytics_service.py": 1,
    "backend/sql/bsale_raw/006_documents.sql": 1,
    "backend/sql/distribuidora/003_views.sql": 4,
    "backend/sql/distribuidora/007_document_related_sync_status_views.sql": 3,
    "backend/sql/distribuidora/009_v_sales_with_credit_notes.sql": 4,
    "backend/sql/distribuidora/011_v_sales_document_sellers.sql": 4,
    "backend/sql/distribuidora/015_v_purchase_document_status_full.sql": 1,
    "backend/sql/distribuidora/026_dispatch_plan_invoiced_view_perf.sql": 2,
    "backend/sql/distribuidora/026_MANUAL_pgAdmin_apply_and_validate.sql": 2,
    "backend/sql/distribuidora/028_planning_rows_indexes.sql": 1,
    "backend/sql/distribuidora/029_planning_rows_sort_index.sql": 1,
    "backend/sql/distribuidora/044_documents_source_sync_metadata.sql": 1,
    "backend/sql/distribuidora/048_document_reissue_lineage.sql": 6,
    "backend/sql/distribuidora_documents_sync_schema.sql": 2,
    "backend/sql/purchase_intelligence_module.sql": 3,
    "backend/utils/distribuidora_oc_sql.py": 1,
}


def _magic_number_counts() -> dict[str, int]:
    root = BACKEND.parent
    out: dict[str, int] = {}
    for path in sorted(BACKEND.rglob("*")):
        if path.suffix not in (".py", ".sql") or not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if rel.startswith(_MAGIC_EXCLUDED_PREFIXES):
            continue
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        n = sum(1 for line in lines if _MAGIC_USE.search(line) or _MAGIC_CONST.search(line))
        if n:
            out[rel] = n
    return out


def test_no_new_document_type_magic_numbers():
    grown = {
        rel: (n, MAGIC_NUMBER_BASELINE.get(rel, 0))
        for rel, n in _magic_number_counts().items()
        if n > MAGIC_NUMBER_BASELINE.get(rel, 0)
    }
    assert not grown, (
        "document_type_id hardcodeado nuevo (actual, baseline): "
        f"{grown}. Usar backend.repositories.distribuidora.document_type_roles."
    )


def test_guard_detects_a_reintroduced_magic_number():
    assert _MAGIC_USE.search("AND d.document_type_id IN (1, 6)")
    assert _MAGIC_USE.search("if document_type_id == 33:")
    assert _MAGIC_CONST.search("NEW_INVOICE_TYPES = frozenset({1, 6})")
    assert not _MAGIC_USE.search("AND d.document_type_id = ANY(%s)")
