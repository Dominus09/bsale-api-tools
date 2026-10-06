"""F1B: relaciones OC→factura/boleta/guía/NC a través de reemisiones Bsale.

Las vistas ``v_document_detail_lineage`` / ``v_document_related_resolved`` se cargan
desde el archivo real del runner (048) en SQLite, y los consumidores ejecutan su
SQL real sobre ese estado (sin PostgreSQL ni Bsale).
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from backend.repositories.distribuidora import details_repo, document_related_repo, schema_preconditions
from backend.repositories.distribuidora.sync_repo import DISTRIBUIDORA_SCHEMA_FILES
from backend.services import order_weight_service
from backend.services.distribuidora import (
    document_relation_sync_service,
    oc_document_chain_resolver,
    oc_related_discovery_service,
    orders_service,
    probable_invoice_service,
)
from backend.utils.distribuidora_oc_sql import (
    OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL,
    OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL,
)
from test_bsale_reissue_hardening_f1a import (
    FakeDetailsCursor,
    _fake_details_execute_values,
    _line,
)

BACKEND = Path(details_repo.__file__).resolve().parents[2]
SQL_DIR = BACKEND / "sql" / "distribuidora"
REISSUE_FILE = "048_document_reissue_lineage.sql"

OC_L = 4_100_001
OC_OTHER = 4_100_002
INVOICE_F = 4_200_001
INVOICE_F2 = 4_200_002
BOLETA_B = 4_200_010
GUIA_G = 4_200_020
NC_N = 4_200_030
DET_A, DET_B, DET_C, DET_D, DET_E = 9_300_001, 9_300_002, 9_300_003, 9_300_004, 9_300_005
INV_LINE = 9_400_001
GUIA_LINE = 9_400_101
NC_LINE = 9_400_201


# ----------------------------------------------------------------- sqlite harness


def _view_sql(filename: str, view: str) -> str:
    text = (SQL_DIR / filename).read_text(encoding="utf-8")
    marker = f"CREATE OR REPLACE VIEW distribuidora.{view} AS"
    for chunk in text.split("-- +go"):
        if marker in chunk:
            body = "\n".join(
                ln for ln in chunk.splitlines() if not ln.strip().startswith("--")
            )
            body = body.replace("CREATE OR REPLACE VIEW", "CREATE VIEW")
            return body.replace("distribuidora.", "").strip().rstrip(";")
    raise AssertionError(f"{view} no encontrada en {filename}")


_ANY_RE = re.compile(r"=\s*ANY\(%s(?:::bigint\[\])?\)")


class SqliteCursor:
    """Traduce el SQL PostgreSQL de los consumidores a SQLite (solo lo necesario)."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._cur = conn.cursor()
        self.sql: list[str] = []

    @property
    def description(self):
        return self._cur.description

    def execute(self, sql: str, params: Any = None) -> None:
        self.sql.append(sql)
        q = sql.replace("distribuidora.", "")
        q = _ANY_RE.sub("IN (SELECT value FROM json_each(%s))", q)
        q = q.replace("%s", "?")
        args = [json.dumps(list(p)) if isinstance(p, (list, tuple)) else p for p in (params or ())]
        self._cur.execute(q, args)

    def fetchall(self):
        return self._cur.fetchall()

    def fetchone(self):
        return self._cur.fetchone()


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE documents (
            document_id INTEGER PRIMARY KEY, document_type_id INTEGER, number INTEGER,
            company_id INTEGER DEFAULT 3, office_id INTEGER DEFAULT 1, state INTEGER DEFAULT 0,
            total_amount NUMERIC, raw_data TEXT, emission_date TEXT
        );
        CREATE TABLE document_details (
            detail_id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL,
            quantity NUMERIC, related_detail_id INTEGER
        );
        CREATE TABLE document_detail_history (
            detail_id INTEGER PRIMARY KEY, document_id INTEGER NOT NULL, quantity NUMERIC
        );
        CREATE TABLE document_related (
            id INTEGER PRIMARY KEY AUTOINCREMENT, detail_id INTEGER NOT NULL,
            related_document_id INTEGER NOT NULL, related_document_type INTEGER NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (detail_id, related_document_id)
        );
        """
    )
    conn.execute(_view_sql(REISSUE_FILE, "v_document_detail_lineage"))
    conn.execute(_view_sql(REISSUE_FILE, "v_document_related_resolved"))
    return conn


def _doc(conn, document_id: int, type_id: int, number: int, *, total: float = 1190.0) -> None:
    conn.execute(
        "INSERT INTO documents (document_id, document_type_id, number, total_amount, raw_data, "
        "emission_date) VALUES (?, ?, ?, ?, '{}', '2026-09-01')",
        (document_id, type_id, number, total),
    )


def _relate(conn, detail_id: int, related_id: int, related_type: int) -> None:
    conn.execute(
        "INSERT INTO document_related (detail_id, related_document_id, related_document_type) "
        "VALUES (?, ?, ?)",
        (detail_id, related_id, related_type),
    )


def _load_lines(conn, fake: FakeDetailsCursor) -> None:
    for did, row in fake.details.items():
        conn.execute(
            "INSERT INTO document_details (detail_id, document_id, quantity) VALUES (?, ?, ?)",
            (did, row["document_id"], row["quantity"]),
        )
    for did, row in fake.history.items():
        conn.execute(
            "INSERT INTO document_detail_history (detail_id, document_id, quantity) VALUES (?, ?, ?)",
            (did, row["document_id"], row["quantity"]),
        )


@pytest.fixture
def lines(monkeypatch):
    monkeypatch.setattr(schema_preconditions, "_REISSUE_SCHEMA_OK", False)
    monkeypatch.setattr(details_repo, "execute_values", _fake_details_execute_values)
    return FakeDetailsCursor()


def _replace(fake: FakeDetailsCursor, doc: int, ids: list[int], qty: dict[int, float] | None = None) -> None:
    qty = qty or {}
    details_repo.replace_document_details(
        fake, doc, [_line(i, qty.get(i, 1.0)) for i in ids], invalidate_cache=False
    )


def _scalar(conn, sql: str, params: tuple = ()) -> Any:
    q = sql.replace("distribuidora.", "").replace("%s", "?")
    row = conn.execute(q, params).fetchone()
    return row[0] if row else None


def _oc_flag(conn, fragment: str, oc_id: int) -> bool:
    return bool(_scalar(conn, f"SELECT {fragment} FROM documents d WHERE d.document_id = ?", (oc_id,)))


def _resolved(conn) -> list[tuple]:
    return conn.execute(
        "SELECT detail_id, related_document_id, origin_document_id, detail_is_current, "
        "detail_is_historical FROM v_document_related_resolved ORDER BY id"
    ).fetchall()


def _reissue_abc_to_abd(conn, fake: FakeDetailsCursor, *, related_type: int = 6, related_id: int = INVOICE_F):
    _doc(conn, OC_L, 33, 70001)
    _replace(fake, OC_L, [DET_A, DET_B, DET_C], {DET_C: 5.0})
    _replace(fake, OC_L, [DET_A, DET_B, DET_D])
    _load_lines(conn, fake)
    _relate(conn, DET_C, related_id, related_type)


# ------------------------------------------------- critical case A+B+C → A+B+D


def test_critical_reissue_keeps_oc_invoiced_without_mixing_lines(db, lines):
    _doc(db, INVOICE_F, 6, 501)
    _reissue_abc_to_abd(db, lines)

    assert lines.current_ids(OC_L) == [DET_A, DET_B, DET_D]
    assert set(lines.history) == {DET_C}
    assert _resolved(db) == [(DET_C, INVOICE_F, OC_L, 0, 1)]

    assert _oc_flag(db, OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL, OC_L)
    assert not _oc_flag(db, OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL, OC_L)
    assert _oc_flag(db, orders_service._OC_IS_INVOICED_SQL, OC_L)
    assert _scalar(
        db, f"SELECT {orders_service.OC_PURCHASE_ESTADO_REAL_SQL} FROM documents d WHERE d.document_id = ?", (OC_L,)
    ) == "Facturada"
    assert _oc_flag(db, order_weight_service.OC_PURCHASE_INVOICED_BY_RELATED_SQL, OC_L)
    not_exists = document_relation_sync_service._confirmed_invoice_not_exists_sql("d")
    assert not _oc_flag(db, not_exists, OC_L)

    # Peso / picking / planificación: solo document_details (C tenía qty 5).
    current = db.execute(
        "SELECT detail_id, quantity FROM document_details WHERE document_id = ? ORDER BY detail_id", (OC_L,)
    ).fetchall()
    assert [r[0] for r in current] == [DET_A, DET_B, DET_D]
    assert sum(r[1] for r in current) == 3


def test_critical_reissue_chain_keeps_invoice(db, lines):
    _doc(db, INVOICE_F, 6, 501)
    _reissue_abc_to_abd(db, lines)
    cur = SqliteCursor(db)

    chains = oc_document_chain_resolver.resolve_oc_document_chains_batch(cur, [OC_L])
    chain = chains[OC_L]
    assert chain.confirmed_invoice_ids == [INVOICE_F]
    assert chain.evidence_source == "direct_related"

    pairs, keys = oc_related_discovery_service.load_existing_invoice_relations_for_oc(cur, OC_L)
    assert pairs == {(DET_C, INVOICE_F)}
    assert keys == {(INVOICE_F, 6)}


def test_unrelated_oc_stays_pending(db, lines):
    _doc(db, INVOICE_F, 6, 501)
    _reissue_abc_to_abd(db, lines)
    _doc(db, OC_OTHER, 33, 70002)
    _replace(lines, OC_OTHER, [DET_E])
    db.execute("INSERT INTO document_details (detail_id, document_id, quantity) VALUES (?, ?, 1)", (DET_E, OC_OTHER))

    assert not _oc_flag(db, OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL, OC_OTHER)
    assert _oc_flag(db, OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL, OC_OTHER)
    chain = oc_document_chain_resolver.resolve_oc_document_chains_batch(SqliteCursor(db), [OC_OTHER])[OC_OTHER]
    assert chain.confirmed_invoice_ids == []


def test_orphan_detail_relation_resolves_to_no_document(db):
    _doc(db, OC_L, 33, 70001)
    _relate(db, 9_999_999, INVOICE_F, 6)
    assert _resolved(db) == [(9_999_999, INVOICE_F, None, 0, 0)]
    assert not _oc_flag(db, OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL, OC_L)


# ---------------------------------------------- current / historical / mixed


def test_current_detail_relation(db, lines):
    _doc(db, OC_L, 33, 70001)
    _doc(db, INVOICE_F, 6, 501)
    _replace(lines, OC_L, [DET_A, DET_B])
    _load_lines(db, lines)
    _relate(db, DET_A, INVOICE_F, 6)
    assert _resolved(db) == [(DET_A, INVOICE_F, OC_L, 1, 0)]
    assert _oc_flag(db, OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL, OC_L)


def test_current_and_historical_relations_same_invoice_no_duplicate_chain(db, lines):
    _doc(db, INVOICE_F, 6, 501)
    _reissue_abc_to_abd(db, lines)
    _relate(db, DET_A, INVOICE_F, 6)

    rows = _resolved(db)
    assert sorted(rows) == sorted([(DET_C, INVOICE_F, OC_L, 0, 1), (DET_A, INVOICE_F, OC_L, 1, 0)])
    chain = oc_document_chain_resolver.resolve_oc_document_chains_batch(SqliteCursor(db), [OC_L])[OC_L]
    assert chain.confirmed_invoice_ids == [INVOICE_F]
    pairs, keys = oc_related_discovery_service.load_existing_invoice_relations_for_oc(SqliteCursor(db), OC_L)
    assert keys == {(INVOICE_F, 6)}
    assert len(pairs) == 2


def test_lineage_never_reports_current_line_as_historical(db):
    db.execute("INSERT INTO document_details (detail_id, document_id, quantity) VALUES (?, ?, 1)", (DET_A, OC_L))
    db.execute("INSERT INTO document_detail_history (detail_id, document_id, quantity) VALUES (?, ?, 1)", (DET_A, OC_L))
    _relate(db, DET_A, INVOICE_F, 6)
    assert db.execute("SELECT detail_id, detail_is_current FROM v_document_detail_lineage").fetchall() == [
        (DET_A, 1)
    ]
    assert _resolved(db) == [(DET_A, INVOICE_F, OC_L, 1, 0)]


def test_lineage_exposes_only_identity_columns(db):
    cols = [d[0] for d in db.execute("SELECT * FROM v_document_detail_lineage").description]
    assert cols == ["detail_id", "document_id", "detail_is_current"]
    cols = [d[0] for d in db.execute("SELECT * FROM v_document_related_resolved").description]
    assert cols == [
        "id",
        "detail_id",
        "related_document_id",
        "related_document_type",
        "created_at",
        "origin_document_id",
        "detail_is_current",
        "detail_is_historical",
    ]


# ------------------------------------------------ multiple revisions / reappear


def test_multiple_reissues_resolve_to_same_local_document(db, lines):
    _doc(db, OC_L, 33, 70001)
    _doc(db, INVOICE_F, 6, 501)
    _doc(db, INVOICE_F2, 6, 502)
    _replace(lines, OC_L, [DET_A, DET_B])  # revisión A
    _replace(lines, OC_L, [DET_B, DET_C])  # revisión B
    _replace(lines, OC_L, [DET_C, DET_D])  # revisión C
    _replace(lines, OC_L, [DET_D, DET_E])  # revisión D
    _load_lines(db, lines)
    _relate(db, DET_A, INVOICE_F, 6)  # línea de la revisión A
    _relate(db, DET_C, INVOICE_F2, 6)  # línea de la revisión C

    assert lines.current_ids(OC_L) == [DET_D, DET_E]
    assert set(lines.history) == {DET_A, DET_B, DET_C}
    assert sorted(_resolved(db)) == sorted(
        [(DET_A, INVOICE_F, OC_L, 0, 1), (DET_C, INVOICE_F2, OC_L, 0, 1)]
    )
    chain = oc_document_chain_resolver.resolve_oc_document_chains_batch(SqliteCursor(db), [OC_L])[OC_L]
    assert sorted(chain.confirmed_invoice_ids) == [INVOICE_F, INVOICE_F2]


def test_reappearing_detail_resolves_once_as_current(db, lines):
    _doc(db, OC_L, 33, 70001)
    _doc(db, INVOICE_F, 6, 501)
    _replace(lines, OC_L, [DET_A, DET_C])
    _replace(lines, OC_L, [DET_A, DET_D])
    _replace(lines, OC_L, [DET_A, DET_C])
    _load_lines(db, lines)
    _relate(db, DET_C, INVOICE_F, 6)

    assert _resolved(db) == [(DET_C, INVOICE_F, OC_L, 1, 0)]
    assert db.execute("SELECT COUNT(*) FROM v_document_related_resolved").fetchone()[0] == 1


# --------------------------------------------- invoice / boleta / guía / NC


def test_boleta_relation_on_historical_detail(db, lines):
    _doc(db, BOLETA_B, 1, 801)
    _reissue_abc_to_abd(db, lines, related_type=1, related_id=BOLETA_B)
    assert _oc_flag(db, OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL, OC_L)
    chain = oc_document_chain_resolver.resolve_oc_document_chains_batch(SqliteCursor(db), [OC_L])[OC_L]
    assert chain.confirmed_invoice_ids == [BOLETA_B]


def test_guia_relation_on_historical_detail_reaches_invoice(db, lines):
    _doc(db, GUIA_G, 8, 901)
    _doc(db, INVOICE_F, 6, 501)
    _reissue_abc_to_abd(db, lines, related_type=8, related_id=GUIA_G)
    db.execute("INSERT INTO document_details (detail_id, document_id, quantity) VALUES (?, ?, 1)", (GUIA_LINE, GUIA_G))
    _relate(db, GUIA_LINE, INVOICE_F, 6)

    # Guía sola no factura la OC (regla vigente: solo 1/6).
    assert not _oc_flag(db, OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL, OC_L)
    chain = oc_document_chain_resolver.resolve_oc_document_chains_batch(SqliteCursor(db), [OC_L])[OC_L]
    assert [p.document_id for p in chain.pickings] == [GUIA_G]
    assert chain.confirmed_invoice_ids == [INVOICE_F]
    assert ("oc", "picking", "invoice") in chain.relation_paths


def test_credit_note_relation_on_historical_invoice_detail(db, lines):
    """Factura reemitida: la NC ligada a una línea histórica de la factura sigue en la cadena."""
    _doc(db, OC_L, 33, 70001)
    _doc(db, INVOICE_F, 6, 501)
    _doc(db, NC_N, 9, 601)
    _replace(lines, OC_L, [DET_A])
    _replace(lines, INVOICE_F, [INV_LINE])
    _replace(lines, INVOICE_F, [INV_LINE + 1])
    _load_lines(db, lines)
    _relate(db, DET_A, INVOICE_F, 6)
    _relate(db, INV_LINE, NC_N, 9)

    assert (INV_LINE, NC_N, INVOICE_F, 0, 1) in _resolved(db)
    chain = oc_document_chain_resolver.resolve_oc_document_chains_batch(SqliteCursor(db), [OC_L])[OC_L]
    assert chain.confirmed_invoice_ids == [INVOICE_F]
    assert [c.document_id for c in chain.credit_notes] == [NC_N]


def test_credit_note_by_related_detail_id_to_historical_invoice_line(db, lines):
    _doc(db, INVOICE_F, 6, 501)
    _doc(db, NC_N, 9, 601)
    _replace(lines, INVOICE_F, [INV_LINE])
    _replace(lines, INVOICE_F, [INV_LINE + 1])
    _load_lines(db, lines)
    db.execute(
        "INSERT INTO document_details (detail_id, document_id, quantity, related_detail_id) VALUES (?, ?, 1, ?)",
        (NC_LINE, NC_N, INV_LINE),
    )
    cur = SqliteCursor(db)
    cur.execute(oc_document_chain_resolver._CN_FROM_RELATED_DETAIL_SQL, ([INVOICE_F],))
    assert [(r[0], r[1]) for r in cur.fetchall()] == [(INVOICE_F, NC_N)]

    links = document_related_repo.fetch_credit_note_links_for_invoice(
        cur, company_id=3, office_id=1, invoice_document_id=INVOICE_F
    )
    assert [(lk["invoice_detail_id"], lk["nc_document_id"]) for lk in links] == [(INV_LINE, NC_N)]


def test_nc_alone_does_not_invoice_oc(db, lines):
    _doc(db, NC_N, 9, 601)
    _reissue_abc_to_abd(db, lines, related_type=9, related_id=NC_N)
    assert not _oc_flag(db, OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL, OC_L)
    assert _scalar(
        db, f"SELECT {orders_service.OC_PURCHASE_ESTADO_REAL_SQL} FROM documents d WHERE d.document_id = ?", (OC_L,)
    ) == "Pendiente"


# -------------------------------------------------------- probable invoice


def test_probable_sees_invoice_related_to_other_oc_via_historical_detail(db, lines):
    _doc(db, INVOICE_F, 6, 501)
    _reissue_abc_to_abd(db, lines)
    cur = SqliteCursor(db)
    out = probable_invoice_service._fetch_related_source_oc_ids_for_invoices(cur, [INVOICE_F])
    assert out == {INVOICE_F: {OC_L}}


def test_orphan_candidates_resolve_oc_via_historical_detail(db, lines):
    _reissue_abc_to_abd(db, lines)  # INVOICE_F sin header → huérfano
    cur = SqliteCursor(db)
    sql = document_related_repo._ORPHAN_CANDIDATES_SQL.format(related_filter="")
    sql = sql.replace("array_agg(DISTINCT oc.document_id ORDER BY oc.document_id)", "json_group_array(DISTINCT oc.document_id)")
    sql = sql.replace("array_agg(DISTINCT oc.number ORDER BY oc.number)", "json_group_array(DISTINCT oc.number)")
    sql = sql.replace("array_agg(DISTINCT dr.detail_id ORDER BY dr.detail_id)", "json_group_array(DISTINCT dr.detail_id)")
    sql = sql.replace("COUNT(*)::int", "COUNT(*)")
    cur.execute(sql, ([1, 6, 9], 3, 1, 50, 0))
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == INVOICE_F
    assert json.loads(rows[0][3]) == [OC_L]
    assert json.loads(rows[0][5]) == [DET_C]


# ----------------------------------------------------- static guarantees


_LEGACY_JOIN_RE = re.compile(
    r"document_details\s+\w+\s+(?:INNER\s+)?JOIN\s+distribuidora\.document_related"
    r"|document_related\s+\w+\s+(?:INNER\s+)?JOIN\s+distribuidora\.document_details",
    re.I,
)


def test_production_consumers_do_not_join_related_to_current_details_only():
    files = [
        *(BACKEND / "services").rglob("*.py"),
        *(BACKEND / "utils").rglob("*.py"),
        *(BACKEND / "repositories").rglob("*.py"),
        *(BACKEND / "routers").rglob("*.py"),
        BACKEND / "audits" / "audit_document_related.py",
        SQL_DIR / REISSUE_FILE,
        SQL_DIR / "026_MANUAL_pgAdmin_apply_and_validate.sql",
    ]
    # 003/007/026 conservan la versión previa y 048 la reemplaza en cada corrida:
    # la definición efectiva de cada vista la valida test_schema_bootstrap.
    offenders = [
        str(p.relative_to(BACKEND))
        for p in files
        if _LEGACY_JOIN_RE.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_status_views_use_resolved_relations():
    mig = (SQL_DIR / REISSUE_FILE).read_text(encoding="utf-8")
    for view, predicate in (
        ("v_orders_purchase_status", "dr.origin_document_id = oc.document_id"),
        ("v_orders", "dr.origin_document_id = d.document_id"),
        ("v_dispatch_plan_invoiced_documents", "dr.origin_document_id = dpo.oc_document_id"),
    ):
        body = mig.split(f"CREATE OR REPLACE VIEW distribuidora.{view} AS", 1)[1].split("-- +go", 1)[0]
        assert "distribuidora.v_document_related_resolved dr" in body, view
        assert predicate in body, view
    order = DISTRIBUIDORA_SCHEMA_FILES
    for earlier in ("003_views.sql", "007_document_related_sync_status_views.sql",
                    "026_dispatch_plan_invoiced_view_perf.sql"):
        assert order.index(earlier) < order.index(REISSUE_FILE)


def test_weight_picking_planning_lines_never_read_history():
    """Clase A: cantidades/peso/picking solo desde document_details."""
    paths = [
        BACKEND / "services" / "order_weight_service.py",
        BACKEND / "services" / "distribuidora" / "dispatch_plan_service.py",
        BACKEND / "services" / "distribuidora" / "dispatch_commercial_margin_service.py",
        BACKEND / "services" / "logistics_weight_audit_service.py",
        BACKEND / "utils" / "planning_sql_fragments.py",
        BACKEND / "utils" / "dashboard_stage.py",
    ]
    for p in paths:
        text = p.read_text(encoding="utf-8")
        assert "document_detail_history" not in text, p.name
        assert "v_document_detail_lineage" not in text, p.name
    weight = (BACKEND / "services" / "order_weight_service.py").read_text(encoding="utf-8")
    assert "FROM distribuidora.document_details dd\nLEFT JOIN bsale.variants v" in weight
