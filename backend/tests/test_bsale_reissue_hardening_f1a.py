"""F1A: reemisiones Bsale (líneas no destructivas, revisión vigente, folio 0, frescura, related)."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from backend.repositories.distribuidora import details_repo, documents_repo, schema_preconditions
from backend.repositories.distribuidora.document_source_repo import (
    children_source_is_current,
    load_current_source,
)
from backend.services.distribuidora import sync_related_service as srs
from backend.services.distribuidora.sync_missing_related_documents_service import (
    _persist_document_from_bsale,
)
from backend.services.distribuidora.sync_service import (
    _process_one_pending_document_row,
    _refresh_document_children,
)
from backend.utils.bsale_document_ids import (
    DocumentSourceEvidence,
    is_revision_not_older,
    resolve_current_source_document_id,
)

FOLIO = 70001
LOCAL_A = 3_900_001
SOURCE_B = 3_900_377
T_A = datetime(2026, 9, 1, 10, 0, 0, tzinfo=timezone.utc)
T_B = T_A + timedelta(minutes=7)


# --------------------------------------------------------------------------- fakes


class FakeDetailsCursor:
    """``document_details`` + ``document_detail_history`` + ``document_related`` en memoria."""

    def __init__(self, *, history_table: bool = True) -> None:
        self.history_table = history_table
        self.details: dict[int, dict[str, Any]] = {}
        self.history: dict[int, dict[str, Any]] = {}
        self.related: list[dict[str, Any]] = []
        self.sql: list[str] = []
        self._result: list[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.sql.append(sql)
        s = " ".join(sql.split())
        self._result = []
        if "to_regclass(name) IS NULL" in s:
            self._result = [] if self.history_table else [("distribuidora.document_detail_history",)]
        elif s.startswith("SELECT detail_id FROM distribuidora.document_details WHERE document_id"):
            doc = params[0]
            self._result = [(d,) for d, r in self.details.items() if r["document_id"] == doc]
        elif s.startswith("INSERT INTO distribuidora.document_detail_history"):
            superseded_by, doc, ids = params
            for did in ids:
                row = self.details.get(did)
                if row and row["document_id"] == doc:
                    self.history[did] = {
                        **row,
                        "superseded_by_source_document_id": superseded_by,
                    }
        elif s.startswith(
            "DELETE FROM distribuidora.document_details WHERE document_id = %s AND detail_id"
        ):
            doc, ids = params
            for did in ids:
                if self.details.get(did, {}).get("document_id") == doc:
                    del self.details[did]
        elif s.startswith("DELETE FROM distribuidora.document_details WHERE document_id = %s"):
            doc = params[0]
            for did in [d for d, r in self.details.items() if r["document_id"] == doc]:
                del self.details[did]
                # FK ON DELETE CASCADE legacy (el runner ya lo elimina en 007).
                self.related = [r for r in self.related if r["detail_id"] != did]
        elif s.startswith("DELETE FROM distribuidora.document_detail_history"):
            doc, ids = params
            for did in ids:
                if self.history.get(did, {}).get("document_id") == doc:
                    del self.history[did]
        else:
            raise AssertionError(f"SQL no esperado en fake: {s[:120]}")

    def upsert_details(self, sql: str, values: list[tuple]) -> list[tuple]:
        cols = _insert_columns(sql)
        written: list[tuple] = []
        for v in values:
            row = dict(zip(cols, v))
            did = row["detail_id"]
            prev = self.details.get(did)
            if "ON CONFLICT (detail_id)" not in sql and prev is not None:
                raise psycopg2.IntegrityError("duplicate key document_details_pkey")
            if prev is not None and prev["document_id"] != row["document_id"]:
                continue
            self.details[did] = row
            written.append((did,))
        return written

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result)

    def current_ids(self, doc: int) -> list[int]:
        return sorted(d for d, r in self.details.items() if r["document_id"] == doc)

    def resolved_relations(self) -> list[tuple[int, int, int | None, bool]]:
        """Réplica de ``v_document_related_resolved``."""
        out = []
        for r in self.related:
            cur_row = self.details.get(r["detail_id"])
            hist_row = self.history.get(r["detail_id"])
            origin = (cur_row or hist_row or {}).get("document_id")
            out.append((r["detail_id"], r["related_document_id"], origin, cur_row is not None))
        return out


def _insert_columns(sql: str) -> list[str]:
    m = re.search(r"INSERT INTO [\w.]+ \((.*?)\)\s*VALUES", sql, re.S)
    assert m, sql
    return [c.strip() for c in m.group(1).split(",") if c.strip() not in ("created_at", "updated_at")]


def _fake_details_execute_values(cur, sql, values, template=None, page_size=100, fetch=False):
    written = cur.upsert_details(sql, values)
    return written if fetch else None


def _line(detail_id: int, qty: float = 1.0) -> dict[str, Any]:
    return {
        "id": detail_id,
        "lineNumber": detail_id % 100,
        "quantity": qty,
        "netUnitValue": 1000,
        "totalUnitValue": 1190,
        "netAmount": 1000 * qty,
        "taxAmount": 190 * qty,
        "totalAmount": 1190 * qty,
        "variant": {"id": 500 + detail_id % 100, "code": f"V{detail_id}", "description": "X"},
    }


@pytest.fixture
def details_db(monkeypatch):
    monkeypatch.setattr(schema_preconditions, "_REISSUE_SCHEMA_OK", False)
    monkeypatch.setattr(details_repo, "execute_values", _fake_details_execute_values)
    return FakeDetailsCursor()


# ------------------------------------------------------------------ V2 líneas


DET_A, DET_B, DET_C, DET_D = 9_100_001, 9_100_002, 9_100_003, 9_100_004
INVOICE_ID = 3_950_000


def test_reissue_abc_to_abd_keeps_relation_to_c_and_no_duplicates(details_db):
    cur = details_db
    details_repo.replace_document_details(
        cur, LOCAL_A, [_line(DET_A), _line(DET_B), _line(DET_C)], invalidate_cache=False
    )
    cur.related.append({"detail_id": DET_C, "related_document_id": INVOICE_ID})

    written = details_repo.replace_document_details(
        cur,
        LOCAL_A,
        [_line(DET_A, 2), _line(DET_B), _line(DET_D)],
        invalidate_cache=False,
        superseded_by_source_document_id=SOURCE_B,
    )

    assert written == 3
    assert cur.current_ids(LOCAL_A) == [DET_A, DET_B, DET_D]
    assert DET_C not in cur.details
    assert set(cur.history) == {DET_C}
    assert cur.history[DET_C]["superseded_by_source_document_id"] == SOURCE_B
    assert cur.details[DET_A]["quantity"] == 2
    # La factura ligada a C conserva la relación y resuelve al mismo document_id estable.
    assert cur.resolved_relations() == [(DET_C, INVOICE_ID, LOCAL_A, False)]
    # Sin duplicados entre vigente e historial.
    assert not set(cur.history) & set(cur.details)
    assert not any("DELETE FROM distribuidora.document_details WHERE document_id = %s\n" in q for q in cur.sql)


def test_line_that_reappears_leaves_history(details_db):
    cur = details_db
    details_repo.replace_document_details(cur, LOCAL_A, [_line(DET_A), _line(DET_C)], invalidate_cache=False)
    details_repo.replace_document_details(cur, LOCAL_A, [_line(DET_A), _line(DET_D)], invalidate_cache=False)
    details_repo.replace_document_details(cur, LOCAL_A, [_line(DET_A), _line(DET_C)], invalidate_cache=False)

    assert cur.current_ids(LOCAL_A) == [DET_A, DET_C]
    assert set(cur.history) == {DET_D}


def test_same_lines_twice_is_idempotent(details_db):
    cur = details_db
    lines = [_line(DET_A), _line(DET_B)]
    details_repo.replace_document_details(cur, LOCAL_A, lines, invalidate_cache=False)
    details_repo.replace_document_details(cur, LOCAL_A, lines, invalidate_cache=False)
    assert cur.current_ids(LOCAL_A) == [DET_A, DET_B]
    assert cur.history == {}


def test_detail_owned_by_other_document_is_not_moved(details_db):
    cur = details_db
    details_repo.replace_document_details(cur, 111, [_line(DET_A)], invalidate_cache=False)
    with pytest.raises(ValueError, match="otro documento local"):
        details_repo.replace_document_details(cur, LOCAL_A, [_line(DET_A)], invalidate_cache=False)
    assert cur.details[DET_A]["document_id"] == 111


def test_without_reissue_schema_replace_fails_without_touching_lines(details_db):
    cur = details_db
    details_repo.replace_document_details(cur, LOCAL_A, [_line(DET_A), _line(DET_C)], invalidate_cache=False)
    cur.history_table = False
    schema_preconditions._REISSUE_SCHEMA_OK = False
    before = len(cur.sql)
    with pytest.raises(schema_preconditions.SchemaPreconditionError, match="apply_distribuidora_schema"):
        details_repo.replace_document_details(cur, LOCAL_A, [_line(DET_A), _line(DET_D)], invalidate_cache=False)
    assert cur.current_ids(LOCAL_A) == [DET_A, DET_C]
    after = cur.sql[before:]
    assert after and all("to_regclass" in q for q in after)


def test_runner_schema_drops_cascade_fk_and_restricts_folio_index():
    """El runner reaplica todos los archivos: el DDL de reemisiones vive en 048 (registrado)."""
    from backend.repositories.distribuidora.sync_repo import DISTRIBUIDORA_SCHEMA_FILES

    sql_dir = Path(details_repo.__file__).resolve().parents[2] / "sql" / "distribuidora"
    order = DISTRIBUIDORA_SCHEMA_FILES
    assert order.index("048_document_reissue_lineage.sql") > order.index(
        "026_dispatch_plan_invoiced_view_perf.sql"
    )
    mig = (sql_dir / "048_document_reissue_lineage.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS distribuidora.document_detail_history" in mig
    assert "DROP CONSTRAINT IF EXISTS fk_distribuidora_document_related_detail" in mig
    assert "WHERE document_type_id IS NOT NULL AND number > 0" in mig
    assert "CREATE OR REPLACE VIEW distribuidora.v_document_related_resolved" in mig
    related = (sql_dir / "007_document_related_sync_status_views.sql").read_text(encoding="utf-8")
    assert "ADD CONSTRAINT fk_distribuidora_document_related_detail" not in related


# --------------------------------------------------------- V1/V6 revisión vigente


def test_resolver_local_a_source_a_raw_b_returns_b():
    ev = DocumentSourceEvidence(
        local_document_id=LOCAL_A,
        folio=FOLIO,
        source_document_id=LOCAL_A,
        source_updated_at=T_A,
        raw_data_id=SOURCE_B,
        raw_number=FOLIO,
        raw_revision_at=T_B,
    )
    assert resolve_current_source_document_id(ev) == SOURCE_B


def test_resolver_never_prefers_stale_source_without_timestamps():
    ev = DocumentSourceEvidence(
        local_document_id=LOCAL_A,
        folio=FOLIO,
        source_document_id=LOCAL_A,
        raw_data_id=SOURCE_B,
        raw_number=FOLIO,
    )
    assert resolve_current_source_document_id(ev) == SOURCE_B


def test_resolver_keeps_newer_source_over_older_raw():
    ev = DocumentSourceEvidence(
        local_document_id=LOCAL_A,
        folio=FOLIO,
        source_document_id=SOURCE_B,
        source_updated_at=T_B,
        raw_data_id=LOCAL_A,
        raw_number=FOLIO,
        raw_revision_at=T_A,
    )
    assert resolve_current_source_document_id(ev) == SOURCE_B


@pytest.mark.parametrize("raw_number", [0, -1, FOLIO + 1])
def test_resolver_ignores_raw_without_matching_positive_folio(raw_number):
    ev = DocumentSourceEvidence(
        local_document_id=LOCAL_A,
        folio=FOLIO,
        source_document_id=LOCAL_A,
        raw_data_id=SOURCE_B,
        raw_number=raw_number,
    )
    assert resolve_current_source_document_id(ev) == LOCAL_A


def test_resolver_falls_back_to_local_pk():
    assert resolve_current_source_document_id(DocumentSourceEvidence(local_document_id=LOCAL_A)) == LOCAL_A


class EvidenceCursor:
    def __init__(self, row: tuple | None) -> None:
        self.row = row
        self.sql: list[str] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.sql.append(sql)

    def fetchone(self):
        return self.row


def _evidence_row(*, source=LOCAL_A, source_at=T_A, raw=SOURCE_B, raw_number=FOLIO, raw_at=T_B):
    return (LOCAL_A, FOLIO, str(source), source_at, str(raw), str(raw_number), raw_at)


def test_related_and_self_heal_use_canonical_resolver():
    cur = EvidenceCursor(_evidence_row())
    assert srs._bsale_source_id_from_pg(cur, LOCAL_A) == (SOURCE_B, FOLIO)
    assert load_current_source(cur, LOCAL_A) == (SOURCE_B, FOLIO)


def test_children_guard_locks_header_and_rejects_stale_source():
    cur = EvidenceCursor(_evidence_row())
    assert children_source_is_current(cur, LOCAL_A, LOCAL_A) is False
    assert "FOR UPDATE" in cur.sql[-1]
    assert children_source_is_current(cur, LOCAL_A, SOURCE_B) is True


def test_children_guard_missing_header_skips():
    assert children_source_is_current(EvidenceCursor(None), LOCAL_A, LOCAL_A) is False


def _refresh_patches(guard_result: bool, replace_mock: MagicMock):
    return (
        patch("backend.services.distribuidora.sync_service.release_transaction"),
        patch("backend.services.distribuidora.sync_service.log_tx"),
        patch("backend.services.distribuidora.sync_service.safe_rollback"),
        patch(
            "backend.services.distribuidora.sync_service.children_source_is_current",
            return_value=guard_result,
        ),
        patch("backend.services.distribuidora.sync_service.replace_document_details", replace_mock),
        patch("backend.services.distribuidora.sync_service.replace_document_attributes", return_value=0),
        patch("backend.services.distribuidora.sync_service.replace_document_references", return_value=0),
        patch("backend.services.distribuidora.sync_service.replace_document_sellers", return_value=0),
        patch(
            "backend.services.order_weight_service.recalculate_order_weight_in_transaction",
            return_value={},
        ),
    )


def _run_refresh(guard_result: bool) -> tuple[MagicMock, dict[str, Any], MagicMock]:
    replace_mock = MagicMock(return_value=1)
    client = MagicMock()
    client.get.side_effect = lambda path, params=None, **kw: (
        {"items": [_line(DET_A)], "count": 1} if path.endswith("/details.json") else {"items": []}
    )
    conn = MagicMock()
    stats: dict[str, Any] = {}
    patches = _refresh_patches(guard_result, replace_mock)
    for p in patches:
        p.start()
    try:
        _refresh_document_children(
            client,
            MagicMock(),
            conn,
            LOCAL_A,
            33,
            stats,
            raw_document={"id": LOCAL_A, "number": FOLIO, "totalAmount": 1190},
            folio=FOLIO,
        )
    finally:
        for p in reversed(patches):
            p.stop()
    return replace_mock, stats, conn


def test_refresh_children_skips_when_newer_revision_is_current():
    replace_mock, stats, conn = _run_refresh(guard_result=False)
    replace_mock.assert_not_called()
    conn.commit.assert_not_called()
    assert stats["children_skipped_stale_source"] == 1
    assert stats["last_children_skipped_stale_source"] is True


def test_refresh_children_passes_revision_to_line_archive():
    replace_mock, stats, conn = _run_refresh(guard_result=True)
    replace_mock.assert_called_once()
    assert replace_mock.call_args.kwargs["superseded_by_source_document_id"] == LOCAL_A
    conn.commit.assert_called_once()


# ------------------------------------------------- upsert de headers (fake tabla)


class FakeDocumentsCursor:
    """``distribuidora.documents`` en memoria con la semántica del upsert por folio/PK."""

    def __init__(self) -> None:
        self.rows: dict[int, dict[str, Any]] = {}
        self.sql: list[str] = []
        self._result: list[tuple] = []
        self.connection = MagicMock()

    def execute(self, sql: str, params: Any = None) -> None:
        self.sql.append(sql)
        s = " ".join(sql.split())
        if "column_name = 'bsale_modified_at'" in s:
            self._result = [(True,)]
        elif "column_name = ANY" in s:
            self._result = [(4,)]
        elif s.startswith("SELECT company_id, office_id, document_type_id, number FROM"):
            keys = set(params[0])
            self._result = [
                (r["company_id"], r["office_id"], r["document_type_id"], r["number"])
                for r in self.rows.values()
                if self._key(r) in keys
            ]
        elif s.startswith("SELECT document_id, company_id, office_id, document_type_id, number"):
            keys = set(params[0])
            self._result = [
                (r["document_id"], r["company_id"], r["office_id"], r["document_type_id"], r["number"])
                for r in self.rows.values()
                if self._key(r) in keys
            ]
        elif s.startswith("SELECT document_id FROM distribuidora.documents WHERE document_id IN"):
            self._result = [(d,) for d in params[0] if d in self.rows]
        else:
            raise AssertionError(f"SQL no esperado en fake: {s[:120]}")

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result)

    @staticmethod
    def _key(r: dict[str, Any]) -> tuple:
        return (r["company_id"], r["office_id"], r["document_type_id"], r["number"])

    def _current_revision_id(self, r: dict[str, Any]) -> int:
        raw = r.get("raw_data") or {}
        return int(raw.get("id") or r.get("source_document_id") or r["document_id"])

    def upsert(self, sql: str, values: list[tuple]) -> list[tuple]:
        cols = _insert_columns(sql)
        by_folio = "ON CONFLICT (company_id, office_id, document_type_id, number)" in sql
        if by_folio:
            assert "number > 0" in sql
        guarded = "(EXCLUDED.bsale_modified_at, EXCLUDED.document_id)" in sql
        out: list[tuple] = []
        for v in values:
            new = dict(zip(cols, v))
            new["raw_data"] = getattr(new["raw_data"], "adapted", new["raw_data"])
            existing = None
            if by_folio:
                existing = next((r for r in self.rows.values() if self._key(r) == self._key(new)), None)
            else:
                existing = self.rows.get(new["document_id"])
            if existing is None:
                self.rows[new["document_id"]] = dict(new)
                out.append((str(new["raw_data"]["id"]),))
                continue
            if guarded and not is_revision_not_older(
                new.get("bsale_modified_at"),
                new["document_id"],
                existing.get("bsale_modified_at"),
                self._current_revision_id(existing),
            ):
                continue
            if existing.get("source_document_id") != new.get("source_document_id"):
                existing["source_hash"] = None
            for c, val in new.items():
                if c != "document_id":
                    existing[c] = val
            out.append((str(new["raw_data"]["id"]),))
        return out


def _fake_docs_execute_values(cur, sql, values, template=None, page_size=100, fetch=False):
    out = cur.upsert(sql, values)
    return out if fetch else None


@pytest.fixture
def docs_db(monkeypatch):
    monkeypatch.setattr(documents_repo, "_DOCS_BSALE_MODIFIED_COL", None)
    monkeypatch.setattr(documents_repo, "_DOCS_SOURCE_COLS", None)
    monkeypatch.setattr(documents_repo, "execute_values", _fake_docs_execute_values)
    return FakeDocumentsCursor()


def _bsale(doc_id: int, *, number: Any = FOLIO, at: datetime = T_A, state: int = 0, total: int = 1190):
    return {
        "id": doc_id,
        "number": number,
        "state": state,
        "generationDate": int(at.timestamp()),
        "emissionDate": int(T_A.timestamp()),
        "totalAmount": total,
        "document_type": {"id": 33},
        "company": {"id": 3},
        "office": {"id": 1},
    }


def _row(doc: dict[str, Any], stats: dict[str, Any] | None = None) -> dict[str, Any] | None:
    return documents_repo.document_dict_from_bsale(doc, company_id=3, default_office_id=1, sync_stats=stats)


def test_header_upsert_writes_source_metadata_coherently(docs_db):
    documents_repo.upsert_documents(docs_db, [_row(_bsale(LOCAL_A, at=T_A))])
    docs_db.rows[LOCAL_A]["source_hash"] = "hash-of-A"
    row_b = _row(_bsale(SOURCE_B, at=T_B, total=2380))
    documents_repo.upsert_documents(docs_db, [row_b])

    stored = docs_db.rows[LOCAL_A]
    assert list(docs_db.rows) == [LOCAL_A]
    assert row_b["document_id"] == LOCAL_A
    assert row_b["stale_revision_skipped"] is False
    assert stored["source_document_id"] == SOURCE_B
    assert stored["source_updated_at"] == T_B
    assert stored["raw_data"]["id"] == SOURCE_B
    assert stored["source_hash"] is None
    assert stored["last_synced_at"] is not None


def test_worker_with_old_revision_cannot_overwrite_newer_commit(docs_db):
    documents_repo.upsert_documents(docs_db, [_row(_bsale(LOCAL_A, at=T_A))])
    worker1_row = _row(_bsale(LOCAL_A, at=T_A))  # worker1 leyó A antes de la reemisión
    worker2_row = _row(_bsale(SOURCE_B, at=T_B, total=2380))
    documents_repo.upsert_documents(docs_db, [worker2_row])  # worker2 COMMIT B
    stats: dict[str, Any] = {}
    documents_repo.upsert_documents(docs_db, [worker1_row], stats)  # worker1 intenta A

    stored = docs_db.rows[LOCAL_A]
    assert worker1_row["stale_revision_skipped"] is True
    assert stats["stale_revisions_skipped"] == 1
    assert stored["raw_data"]["id"] == SOURCE_B
    assert stored["source_document_id"] == SOURCE_B
    assert float(stored["total_amount"]) == 2380


def test_old_revision_arriving_later_in_same_batch_is_dropped(docs_db):
    row_a = _row(_bsale(LOCAL_A, at=T_A))
    row_b = _row(_bsale(SOURCE_B, at=T_B))
    documents_repo.upsert_documents(docs_db, [row_b, row_a])
    assert docs_db.rows[next(iter(docs_db.rows))]["raw_data"]["id"] == SOURCE_B
    assert row_a["stale_revision_skipped"] is True


def test_stale_header_does_not_refresh_children():
    row = {"document_id": LOCAL_A, "number": FOLIO, "document_type_id": 33, "_bsale_document": _bsale(LOCAL_A)}

    def fake_upsert(_cur, rows, _stats):
        rows[0]["stale_revision_skipped"] = True
        return 1, 1

    stats: dict[str, Any] = {"documents_processed": 0}
    with (
        patch("backend.services.distribuidora.sync_service.upsert_documents", side_effect=fake_upsert),
        patch("backend.services.distribuidora.sync_service._refresh_document_children") as refresh,
        patch("backend.services.distribuidora.sync_service.release_transaction"),
        patch("backend.services.distribuidora.sync_service.log_tx"),
    ):
        _process_one_pending_document_row(MagicMock(), MagicMock(), MagicMock(), row, stats)
    refresh.assert_not_called()
    assert stats["documents_stale_revision_skipped"] == 1


def test_folio_upsert_sql_has_freshness_guard_and_positive_folio(docs_db):
    documents_repo.upsert_documents(docs_db, [_row(_bsale(LOCAL_A))])
    cur = MagicMock()
    with patch.object(documents_repo, "execute_values", return_value=[]) as ev:
        documents_repo.upsert_documents(cur, [_row(_bsale(SOURCE_B, at=T_B))])
    sql = " ".join(ev.call_args.args[1].split())
    assert "WHERE document_type_id IS NOT NULL AND number > 0" in sql
    assert "(EXCLUDED.bsale_modified_at, EXCLUDED.document_id) >= (distribuidora.documents.bsale_modified_at," in sql
    assert "RETURNING raw_data->>'id'" in sql
    assert "source_document_id = EXCLUDED.source_document_id" in sql


# ---------------------------------------------------------------- V3 number=0


@pytest.mark.parametrize("number", [0, "0", -5])
def test_non_positive_number_is_not_a_commercial_document(number):
    stats: dict[str, Any] = {}
    assert _row(_bsale(SOURCE_B, number=number, state=8888), stats) is None
    assert stats["skipped_non_positive_folio"] == 1
    assert documents_repo._folio_number_from_bsale({"number": number}) is None


def test_first_number_zero_doc_does_not_create_oc_zero(docs_db):
    stats: dict[str, Any] = {}
    row = _row(_bsale(SOURCE_B, number=0, state=8888), stats)
    assert row is None
    assert documents_repo.upsert_documents(docs_db, [r for r in [row] if r]) == (0, 0)
    assert docs_db.rows == {}


def test_second_number_zero_doc_does_not_update_first(docs_db):
    manual_zero = {**_row(_bsale(SOURCE_B)), "number": 0}
    stats: dict[str, Any] = {}
    assert documents_repo.upsert_documents(docs_db, [manual_zero], stats) == (0, 0)
    assert stats["skipped_non_positive_folio"] == 1
    assert docs_db.rows == {}


def test_number_zero_revision_never_overwrites_active_oc(docs_db):
    documents_repo.upsert_documents(docs_db, [_row(_bsale(LOCAL_A, at=T_A))])
    husk = _bsale(LOCAL_A, number=0, state=8888, at=T_B, total=0)
    assert _row(husk) is None
    husk_row = {**_row(_bsale(LOCAL_A, at=T_B, total=0)), "number": 0}
    documents_repo.upsert_documents(docs_db, [husk_row])
    stored = docs_db.rows[LOCAL_A]
    assert stored["number"] == FOLIO
    assert stored["state"] == 0
    assert float(stored["total_amount"]) == 1190


def test_validate_missing_related_rejects_non_positive_folio():
    from backend.services.distribuidora.sync_missing_related_documents_service import (
        validate_bsale_against_candidate,
    )

    ok, reason = validate_bsale_against_candidate(
        related_document_id=SOURCE_B,
        expected_type=33,
        blob=_bsale(SOURCE_B, number=0, state=8888),
        company_id=3,
        office_id=1,
    )
    assert (ok, reason) == (False, "non_positive_folio")


# ------------------------------------------------------------------- 8888


def test_positive_folio_with_state_8888_is_not_discarded_by_state(docs_db):
    row = _row(_bsale(LOCAL_A, state=8888))
    assert row is not None
    assert row["state"] == 8888
    documents_repo.upsert_documents(docs_db, [row])
    assert docs_db.rows[LOCAL_A]["state"] == 8888
    ev = DocumentSourceEvidence(
        local_document_id=LOCAL_A,
        folio=FOLIO,
        source_document_id=LOCAL_A,
        raw_data_id=SOURCE_B,
        raw_number=FOLIO,
    )
    assert resolve_current_source_document_id(ev) == SOURCE_B


# ------------------------------------------------------------- V4 missing_related


def test_missing_related_uses_persisted_pk_and_bsale_source_after_remap():
    blob = _bsale(SOURCE_B, at=T_B)

    def fake_upsert(_cur, rows, _stats):
        rows[0]["document_id"] = LOCAL_A  # folio ya existía con PK A
        rows[0]["stale_revision_skipped"] = False
        return 1, 1

    with (
        patch(
            "backend.services.distribuidora.sync_missing_related_documents_service.upsert_documents",
            side_effect=fake_upsert,
        ),
        patch(
            "backend.services.distribuidora.sync_missing_related_documents_service._refresh_document_children"
        ) as refresh,
        patch("backend.services.distribuidora.sync_missing_related_documents_service.release_transaction"),
    ):
        local = _persist_document_from_bsale(
            MagicMock(), MagicMock(), MagicMock(), blob, company_id=3, office_id=1, stats={}
        )

    assert local == LOCAL_A
    args, kwargs = refresh.call_args
    assert args[3] == LOCAL_A
    assert kwargs["bsale_source_document_id"] == SOURCE_B
    assert kwargs["raw_document"]["id"] == SOURCE_B


def test_missing_related_stale_revision_skips_children():
    def fake_upsert(_cur, rows, _stats):
        rows[0]["document_id"] = LOCAL_A
        rows[0]["stale_revision_skipped"] = True
        return 1, 1

    with (
        patch(
            "backend.services.distribuidora.sync_missing_related_documents_service.upsert_documents",
            side_effect=fake_upsert,
        ),
        patch(
            "backend.services.distribuidora.sync_missing_related_documents_service._refresh_document_children"
        ) as refresh,
    ):
        local = _persist_document_from_bsale(
            MagicMock(), MagicMock(), MagicMock(), _bsale(LOCAL_A), company_id=3, office_id=1, stats={}
        )
    assert local == LOCAL_A
    refresh.assert_not_called()


# ------------------------------------------------------------ V13 related tx


class RelatedCursor:
    def __init__(self, bad_detail_ids: set[int]) -> None:
        self.bad = bad_detail_ids
        self.inserted: list[tuple[int, int, int]] = []
        self.statements: list[str] = []
        self.aborted = False
        self.rowcount = 0

    def execute(self, sql: str, params: Any = None) -> None:
        s = " ".join(sql.split())
        self.statements.append(s)
        if s.startswith("ROLLBACK TO SAVEPOINT"):
            self.aborted = False
            return
        if self.aborted:
            raise psycopg2.errors.InFailedSqlTransaction("current transaction is aborted")
        if s.startswith("SAVEPOINT"):
            return
        if s.startswith("INSERT INTO distribuidora.document_related"):
            if params[0] in self.bad:
                self.aborted = True
                raise psycopg2.DataError("invalid input")
            self.inserted.append(tuple(params))
            self.rowcount = 1


def test_one_bad_related_row_does_not_kill_batch():
    cur = RelatedCursor({DET_B})
    conn = MagicMock()
    stats: dict[str, Any] = {}
    inserted = srs._insert_related_triples(
        conn,
        cur,
        [(DET_A, INVOICE_ID, 6), (DET_B, INVOICE_ID, 6), (DET_C, INVOICE_ID, 6)],
        stats=stats,
        log_ctx="[test]",
    )
    assert inserted == 2
    assert [t[0] for t in cur.inserted] == [DET_A, DET_C]
    assert stats["related_insert_failures"] == 1
    assert "ROLLBACK TO SAVEPOINT document_related_row" in cur.statements
    assert not any("InFailedSqlTransaction" in s for s in cur.statements)


def test_related_loop_rolls_back_after_apply_error():
    conn = MagicMock()
    stats: dict[str, Any] = {"rows_inserted": 0}
    oc_res = {"status": "would_insert", "edges": [], "would_confirm": True}
    with (
        patch.object(srs, "discover_invoice_edges_for_oc", return_value=oc_res),
        patch.object(srs, "apply_discovered_invoice_edges", side_effect=psycopg2.DataError("bad")),
    ):
        srs._process_one_oc_related_sync(
            conn=conn,
            cur=MagicMock(),
            client=MagicMock(),
            doc_id=LOCAL_A,
            stats=stats,
            discovery_mode=srs.DISCOVERY_MODE_FULL,
            helper_throttle=0.0,
        )
    conn.rollback.assert_called_once()
    assert stats["document_errors"] == 1


def test_related_loop_rolls_back_after_discovery_sql_error():
    conn = MagicMock()
    stats: dict[str, Any] = {"rows_inserted": 0}
    with patch.object(
        srs, "discover_invoice_edges_for_oc", side_effect=psycopg2.ProgrammingError("bad sql")
    ):
        assert (
            srs._process_one_oc_related_sync(
                conn=conn,
                cur=MagicMock(),
                client=MagicMock(),
                doc_id=LOCAL_A,
                stats=stats,
                discovery_mode=srs.DISCOVERY_MODE_FULL,
                helper_throttle=0.0,
            )
            is None
        )
    conn.rollback.assert_called_once()


def test_self_heal_does_not_replace_with_incomplete_pages():
    conn = MagicMock()
    with (
        patch.object(srs, "_detail_ids_missing_for_document", return_value=[DET_D]),
        patch.object(srs, "_bsale_source_id_from_pg", return_value=(SOURCE_B, FOLIO)),
        patch.object(srs, "_fetch_all_detail_items_from_bsale", return_value=([_line(DET_A)], 1, False)),
        patch.object(srs, "replace_document_details") as replace,
    ):
        srs._self_heal_document_details_if_needed(
            MagicMock(), conn, MagicMock(), LOCAL_A, [DET_D], throttle=0.0, log_ctx="[t]", stats={}
        )
    replace.assert_not_called()


def test_self_heal_skips_when_source_no_longer_current():
    conn = MagicMock()
    with (
        patch.object(srs, "_detail_ids_missing_for_document", return_value=[DET_D]),
        patch.object(srs, "_bsale_source_id_from_pg", return_value=(LOCAL_A, FOLIO)),
        patch.object(srs, "_fetch_all_detail_items_from_bsale", return_value=([_line(DET_D)], 1, True)),
        patch.object(srs, "children_source_is_current", return_value=False),
        patch.object(srs, "replace_document_details") as replace,
    ):
        stats: dict[str, Any] = {}
        srs._self_heal_document_details_if_needed(
            MagicMock(), conn, MagicMock(), LOCAL_A, [DET_D], throttle=0.0, log_ctx="[t]", stats=stats
        )
    replace.assert_not_called()
    conn.rollback.assert_called_once()
    assert stats["children_skipped_stale_source"] == 1
