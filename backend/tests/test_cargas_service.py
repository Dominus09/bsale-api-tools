"""Tests servicio Cargas: add, exceso, certify atómico, cancel, reopen, hash."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from backend.services.cargas import service as svc
from backend.services.cargas.parse_excel import PickingParseError
from backend.services.cargas.sec import units_from_boxes_and_loose


def test_units_calc_jagermeister_half_box_style():
    assert units_from_boxes_and_loose(boxes=0.5, loose=0, sec=6) == 3


def test_item_status_helpers():
    assert svc._item_status(24, 0) == "pending"
    assert svc._item_status(24, 12) == "partial"
    assert svc._item_status(24, 24) == "complete"
    assert svc._item_status(24, 48) == "excess"


def test_search_tokens_match_cristal(monkeypatch):
    load = {
        "items": [
            {
                "id": 1,
                "product_name": "CRISTAL LATA 470 CC (SEC 24)",
                "barcode": "7802100505323",
                "product_type": "CERVEZA",
                "status": "pending",
                "normalized_product_name": "cristal lata 470 cc sec 24",
            },
            {
                "id": 2,
                "product_name": "COCA COLA LATA 350 CC (SEC 24)",
                "barcode": "7801610001196",
                "product_type": "BEBIDAS",
                "status": "pending",
                "normalized_product_name": "coca cola lata 350 cc sec 24",
            },
        ]
    }
    monkeypatch.setattr(svc, "get_load", lambda _id: load)
    rows = svc.search_items(1, q="cristal")
    assert len(rows) == 1
    assert "CRISTAL" in rows[0]["product_name"]
    rows2 = svc.search_items(1, q="470 cristal")
    assert len(rows2) == 1
    rows3 = svc.search_items(1, q="7801610001196")
    assert len(rows3) == 1


def test_compute_file_hash_stable():
    h1 = svc.compute_file_hash(b"abc")
    h2 = svc.compute_file_hash(b"abc")
    h3 = svc.compute_file_hash(b"abd")
    assert h1 == h2
    assert h1 != h3
    assert len(h1) == 64


def _mock_conn_cur():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_add_units_partial_and_multiple_accumulate():
    conn, cur = _mock_conn_cur()

    def run_one(certified_before: float, boxes: float, loose: float, expected_after: float):
        cur.reset_mock()
        conn.reset_mock()
        conn.cursor.return_value = cur
        cur.fetchone.side_effect = [
            ("in_progress",),
            (10, 500.0, certified_before, 24, "partial" if certified_before else "pending"),
        ]
        with patch.object(svc, "get_connection", return_value=conn), patch.object(
            svc, "get_load", return_value={"id": 1, "ok": True}
        ):
            result = svc.add_units(
                load_id=1,
                item_id=10,
                user_email="u@x.com",
                boxes=boxes,
                loose_units=loose,
            )
        assert result["ok"] is True
        update_calls = [
            c
            for c in cur.execute.call_args_list
            if c[0] and "SET certified_units" in str(c[0][0])
        ]
        assert update_calls
        args = update_calls[0][0][1]
        assert args[0] == expected_after
        insert_calls = [
            c
            for c in cur.execute.call_args_list
            if c[0] and "load_item_events" in str(c[0][0])
        ]
        assert insert_calls
        ev = insert_calls[0][0][1]
        assert ev[6] == expected_after - certified_before
        assert ev[7] == expected_after
        conn.commit.assert_called()

    run_one(0.0, 10, 0, 240.0)
    run_one(240.0, 4, 0, 336.0)
    run_one(336.0, 0, 6, 342.0)


def test_add_units_excess_blocked_no_write():
    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("in_progress",),
        (10, 24.0, 0.0, 24, "pending"),
    ]
    with patch.object(svc, "get_connection", return_value=conn):
        with pytest.raises(ValueError, match="EXCESO"):
            svc.add_units(
                load_id=1,
                item_id=10,
                user_email="u@x.com",
                boxes=2,  # 48 units > 24
                loose_units=0,
            )
    # No commit of successful add; rollback path
    assert not any(
        "certified_units = %s" in str(c) and c[0][1][0] == 48
        for c in cur.execute.call_args_list
        if c[0]
    )


def test_add_units_excess_register_issue():
    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("in_progress",),
        (10, 24.0, 0.0, 24, "pending"),
    ]
    with patch.object(svc, "get_connection", return_value=conn):
        with pytest.raises(ValueError, match="EXCESO"):
            svc.add_units(
                load_id=1,
                item_id=10,
                user_email="u@x.com",
                boxes=2,
                loose_units=0,
                register_excess_issue=True,
            )
    assert any("load_issues" in str(c) for c in cur.execute.call_args_list)
    conn.commit.assert_called()


def test_report_and_resolve_issue():
    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("in_progress",),  # load lock
        (24.0, 0.0),  # item
        None,  # RETURNING id unused via fetchone on insert? insert uses RETURNING but we don't fetch
    ]
    # report_issue doesn't fetch RETURNING - it just executes. Fix side_effect.
    cur.fetchone.side_effect = [
        ("in_progress",),
        (24.0, 12.0),
    ]
    with patch.object(svc, "get_connection", return_value=conn), patch.object(
        svc, "get_load", return_value={"id": 1, "issues": "ok"}
    ):
        out = svc.report_issue(
            load_id=1,
            item_id=10,
            user_email="u@x.com",
            issue_type="not_found",
            description="falta",
        )
    assert out["issues"] == "ok"
    assert any("load_issues" in str(c) for c in cur.execute.call_args_list)

    conn2, cur2 = _mock_conn_cur()
    cur2.fetchone.side_effect = [
        ("in_progress",),
        (24.0, 12.0),
    ]
    with patch.object(svc, "get_connection", return_value=conn2), patch.object(
        svc, "get_load", return_value={"id": 1, "resolved": True}
    ):
        out2 = svc.resolve_issue(
            load_id=1, item_id=10, user_email="u@x.com", issue_id=5
        )
    assert out2["resolved"] is True
    assert any("resolved_by" in str(c) for c in cur2.execute.call_args_list)


def test_certify_incomplete_raises():
    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("in_progress", None, None),
    ]
    cur.fetchall.return_value = [
        (1, 24.0, 12.0, "partial"),
    ]
    with patch.object(svc, "get_connection", return_value=conn):
        with pytest.raises(ValueError, match="incompletos"):
            svc.certify_load(load_id=1, user_email="u@x.com")
    conn.rollback.assert_called()


def test_certify_complete_atomic_for_update():
    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("in_progress", None, None),  # load lock
        (0,),  # open issues count
        (1,),  # update returning
    ]
    cur.fetchall.return_value = [
        (1, 24.0, 24.0, "complete"),
        (2, 10.0, 10.0, "complete"),
    ]
    with patch.object(svc, "get_connection", return_value=conn), patch.object(
        svc, "get_load", return_value={"id": 1, "status": "certified"}
    ):
        out = svc.certify_load(load_id=1, user_email="cert@x.com")
    assert out["status"] == "certified"
    sqls = " ".join(str(c) for c in cur.execute.call_args_list)
    assert "FOR UPDATE" in sqls
    assert "certified" in sqls.lower()
    assert "last_certified_by" in sqls
    conn.commit.assert_called()


def test_certify_blocked_when_cancelled():
    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [("cancelled", None, None)]
    with patch.object(svc, "get_connection", return_value=conn):
        with pytest.raises(ValueError, match="cancelada"):
            svc.certify_load(load_id=1, user_email="u@x.com")


def test_cancel_requires_reason_and_sets_fields():
    with pytest.raises(ValueError, match="Motivo"):
        svc.cancel_load(load_id=1, user_email="u@x.com", reason="ab")

    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("pending",),
        (1,),
    ]
    with patch.object(svc, "get_connection", return_value=conn), patch.object(
        svc, "get_load", return_value={"id": 1, "status": "cancelled"}
    ):
        out = svc.cancel_load(
            load_id=1, user_email="u@x.com", reason="duplicado de import"
        )
    assert out["status"] == "cancelled"
    sqls = " ".join(str(c) for c in cur.execute.call_args_list)
    assert "cancelled" in sqls
    assert "cancel_reason" in sqls


def test_cancel_rejects_certified():
    conn, cur = _mock_conn_cur()
    cur.fetchone.return_value = ("certified",)
    with patch.object(svc, "get_connection", return_value=conn):
        with pytest.raises(ValueError, match="certificada"):
            svc.cancel_load(load_id=1, user_email="u@x.com", reason="no se debe")


def test_reopen_requires_reason_preserves_last_certified():
    with pytest.raises(ValueError, match="Motivo"):
        svc.reopen_load(load_id=1, user_email="u@x.com", reason="x")

    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("certified", "orig@x.com", "2026-01-01", None, None),
        (1,),
    ]
    with patch.object(svc, "get_connection", return_value=conn), patch.object(
        svc,
        "get_load",
        return_value={
            "id": 1,
            "status": "in_progress",
            "last_certified_by": "orig@x.com",
        },
    ):
        out = svc.reopen_load(
            load_id=1, user_email="boss@x.com", reason="faltó un pallet"
        )
    assert out["status"] == "in_progress"
    sqls = " ".join(str(c) for c in cur.execute.call_args_list)
    assert "last_certified_by" in sqls
    assert "reopen_reason" in sqls
    assert "reopen" in sqls


def test_confirm_import_hash_mismatch():
    preview = MagicMock()
    preview.file_hash = "aaa"
    preview.picking_number = "2531"
    preview.to_dict.return_value = {"can_import": True, "errors": []}
    with patch.object(svc, "_parse_file", return_value=preview), patch.object(
        svc, "compute_file_hash", return_value="aaa"
    ):
        with pytest.raises(PickingParseError, match="no coincide"):
            svc.confirm_import(
                data=b"x",
                filename="a.xlsx",
                user_email="u@x.com",
                expected_file_hash="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )


def test_confirm_import_rejects_active_picking_allows_after_cancel_logic():
    """Simula UNIQUE app-level: activo bloquea; cancelled no aparece en SELECT."""
    preview = MagicMock()
    preview.file_hash = "abc"
    preview.picking_number = "2531"
    preview.picking_date = None
    preview.destination = "X"
    preview.truck = "T"
    preview.seal = "S"
    preview.source_type = "excel"
    preview.summed_units = 10
    preview.summed_value = 1
    preview.document_units_total = 10
    preview.document_value_total = 1
    preview.valid_lines = []
    preview.to_dict.return_value = {"can_import": True, "errors": []}

    conn, cur = _mock_conn_cur()
    cur.fetchone.return_value = (99, "pending")
    with patch.object(svc, "_parse_file", return_value=preview), patch.object(
        svc, "get_connection", return_value=conn
    ):
        with pytest.raises(PickingParseError, match="Ya existe"):
            svc.confirm_import(
                data=b"x",
                filename="a.xlsx",
                user_email="u@x.com",
                expected_file_hash="abc",
            )

    # After cancel: SELECT returns None → would proceed to insert
    conn2, cur2 = _mock_conn_cur()
    cur2.fetchone.side_effect = [None, (100,)]  # no active, then insert id
    with patch.object(svc, "_parse_file", return_value=preview), patch.object(
        svc, "get_connection", return_value=conn2
    ), patch.object(svc, "get_load", return_value={"id": 100, "picking_number": "2531"}):
        out = svc.confirm_import(
            data=b"x",
            filename="a.xlsx",
            user_email="u@x.com",
            expected_file_hash="abc",
        )
    assert out["id"] == 100


def test_certify_uses_for_update_before_update_order():
    """Concurrencia viable: FOR UPDATE aparece antes del UPDATE status=certified."""
    conn, cur = _mock_conn_cur()
    cur.fetchone.side_effect = [
        ("in_progress", None, None),
        (0,),
        (1,),
    ]
    cur.fetchall.return_value = [(1, 5.0, 5.0, "complete")]
    with patch.object(svc, "get_connection", return_value=conn), patch.object(
        svc, "get_load", return_value={"id": 1, "status": "certified"}
    ):
        svc.certify_load(load_id=1, user_email="u@x.com")

    texts = [str(c[0][0]) for c in cur.execute.call_args_list if c[0]]
    for_update_idx = next(i for i, t in enumerate(texts) if "FOR UPDATE" in t)
    certify_idx = next(
        i for i, t in enumerate(texts) if "status = 'certified'" in t.replace("\n", " ")
    )
    assert for_update_idx < certify_idx


def test_add_units_blocked_when_cancelled():
    conn, cur = _mock_conn_cur()
    cur.fetchone.return_value = ("cancelled",)
    with patch.object(svc, "get_connection", return_value=conn):
        with pytest.raises(ValueError, match="bloqueada"):
            svc.add_units(load_id=1, item_id=1, user_email="u@x.com", boxes=1, loose_units=0)


def test_preview_sets_file_hash():
    from backend.services.cargas.parse_common import ParsedLoadLine, ParsedLoadPreview

    fake = ParsedLoadPreview(
        source_type="excel",
        original_filename="a.xlsx",
        picking_number="1",
        document_units_total=1,
        lines=[
            ParsedLoadLine(
                product_name="A (SEC 24)",
                requested_units=1,
                barcode="1",
                normalized_product_name="a",
            )
        ],
    )
    with patch(
        "backend.services.cargas.service.parse_picking_excel", return_value=fake
    ):
        out = svc.preview_import(data=b"hello-bytes", filename="a.xlsx")
    assert out["file_hash"] == svc.compute_file_hash(b"hello-bytes")
    assert len(out["file_hash"]) == 64
