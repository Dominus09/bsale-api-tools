"""Tests API read-only Costos V2 (sin PostgreSQL / sin prod)."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from backend.repositories.cost_v2_read_repo import LIST_SELECT, CostV2ReadRepository
from backend.schemas.cost_v2_read import (
    ALLOWED_QUALITY_STATUSES,
    CALCULATION_VERSION_PIN,
    DATA_SOURCE,
    CostV2ReadValidationError,
    decode_cursor,
    encode_cursor,
    money_to_json,
    normalize_statuses,
    unit_difference,
    validate_limit,
)
from backend.services.analytics.money import D
from backend.services.analytics.validate_distribuidora_source import (
    assert_sql_is_read_only,
)

def _row(
    history_id: int,
    *,
    admission_date: date = date(2026, 6, 1),
    net: Decimal = D("650"),
    gross_stored: Decimal | None = D("650"),
    corrected: Decimal | None = D("773.50"),
    iva: Decimal | None = D("123.50"),
    status: str = "missing_taxes_in_gross",
    warnings: list[str] | None = None,
    barcode: str = "7803473005960",
    variant_id: int = 10,
    tax_ids: list[int] | None = None,
    additional: list[dict] | None = None,
    company_id: int = 3,
    office_id: int = 3,
    document_number: int | None = 100,
) -> dict[str, Any]:
    return {
        "history_id": history_id,
        "company_id": company_id,
        "office_id": office_id,
        "variant_id": variant_id,
        "admission_date": admission_date,
        "calculation_version": CALCULATION_VERSION_PIN,
        "calculation_batch_id": str(uuid4()),
        "calculated_at": datetime(2026, 7, 31, 12, 0, 0),
        "stored_cost_net": net,
        "stored_quantity": D("2"),
        "stored_iva_amount": D("0"),
        "stored_other_taxes": D("0"),
        "stored_gross_cost": gross_stored,
        "corrected_gross_cost": corrected,
        "calculated_iva_amount": iva,
        "additional_tax_amount_total": D("0") if not additional else D("79.68"),
        "additional_tax_rate_total": D("0"),
        "total_tax_rate": D("0.19"),
        "iva_tax_id": 1,
        "iva_rate": D("0.19"),
        "resolved_tax_ids_json": tax_ids if tax_ids is not None else [1],
        "additional_taxes_json": additional or [],
        "reception_tax_ids_json": [],
        "catalog_tax_ids_json": tax_ids if tax_ids is not None else [1],
        "tax_ids_source": "current_product_tax",
        "tax_rates_source": "bsale_taxes",
        "tax_resolution_quality": "current_catalog",
        "tax_context_source": "current_product_tax",
        "tax_context_is_historical": False,
        "tax_context_fingerprint": "taxfp",
        "source_history_fingerprint": "srcfp",
        "calculation_result_fingerprint": "resfp",
        "effective_quality_status": status,
        "warnings_json": warnings or [],
        "gross_difference_amount": (
            None
            if corrected is None or gross_stored is None
            else corrected - gross_stored
        ),
        "document_number": document_number,
        "document": str(document_number) if document_number else None,
        "reception_id": document_number,
        "barcode": barcode,
        "product_name": "PROD",
        "variant_name": "VAR",
        "history_created_at": datetime(2026, 6, 2),
    }


class ReadFake:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = list(rows)
        self.sqls: list[str] = []
        self.params: list[tuple] = []
        self.write_attempts = 0

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        upper = sql.upper()
        if any(x in upper for x in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ")):
            self.write_attempts += 1
            raise AssertionError(f"escritura no permitida: {sql[:80]}")
        assert_sql_is_read_only(sql)
        self.sqls.append(sql)
        self.params.append(params)

        # Filter helpers
        rows = list(self.rows)
        # company / office / version from params order in scope
        # Heuristic: apply filters present in SQL
        if "COUNT(*)" in upper and "BY_STATUS" not in upper and "WARNING" not in upper:
            if "EFFECTIVE_QUALITY_STATUS AS STATUS" in upper or (
                "GROUP BY c.effective_quality_status" in sql.lower()
            ):
                counts: dict[str, int] = {}
                for r in self._scoped(rows, sql, params):
                    counts[r["effective_quality_status"]] = (
                        counts.get(r["effective_quality_status"], 0) + 1
                    )
                return [{"status": k, "n": v} for k, v in counts.items()]
            if " AS n_pairs" in sql.lower() or "n_pairs" in sql.lower():
                scoped = self._scoped(rows, sql, params)
                pairs = {(r["history_id"], r["calculation_version"]) for r in scoped}
                return [{"n": len(scoped), "n_pairs": len(pairs)}]
            # summary agregados (evitar confundir unique_variants con n_pairs)
            if "total_rows" in sql.lower() or "with_corrected_gross" in sql.lower():
                scoped = self._scoped(rows, sql, params)
                return [
                    {
                        "total_rows": len(scoped),
                        "unique_variants": len({r["variant_id"] for r in scoped}),
                        "unique_documents": len(
                            {
                                r.get("document_number") or r.get("reception_id")
                                for r in scoped
                            }
                        ),
                        "with_corrected_gross": sum(
                            1
                            for r in scoped
                            if r.get("corrected_gross_cost") is not None
                        ),
                        "without_corrected_gross": sum(
                            1 for r in scoped if r.get("corrected_gross_cost") is None
                        ),
                        "min_admission_date": min(
                            (r["admission_date"] for r in scoped), default=None
                        ),
                        "max_admission_date": max(
                            (r["admission_date"] for r in scoped), default=None
                        ),
                    }
                ]
            scoped = self._scoped(rows, sql, params)
            return [
                {
                    "total_rows": len(scoped),
                    "unique_variants": len({r["variant_id"] for r in scoped}),
                    "unique_documents": 1,
                    "with_corrected_gross": 0,
                    "without_corrected_gross": 0,
                    "min_admission_date": None,
                    "max_admission_date": None,
                }
            ]

        # warnings unnest (COUNT con WARNING en SQL)
        if "jsonb_array_elements_text" in sql.lower() or (
            "COUNT(*)" in upper and "w.warning" in sql.lower()
        ):
            counts_w: dict[str, int] = {}
            for r in self._scoped(rows, sql, params):
                for w in r.get("warnings_json") or []:
                    counts_w[str(w)] = counts_w.get(str(w), 0) + 1
            return [{"warning": k, "n": v} for k, v in counts_w.items()]

        scoped = self._scoped(rows, sql, params)
        # keyset cursor
        if "admission_date, h.id) <" in sql.lower() or "(h.admission_date, h.id) <" in sql:
            # last two params before limit are cursor date/id roughly — find date+int pair near end
            cur_date = None
            cur_id = None
            for p in params:
                if isinstance(p, date) and not isinstance(p, datetime):
                    cur_date = p
                if isinstance(p, int) and cur_date is not None and p > 100:
                    cur_id = p
            # Better: params include cursor as last date/int before limit
            # Find pattern: ... cursor_date, cursor_id, limit
            if len(params) >= 2 and isinstance(params[-1], int):
                # limit is last
                maybe_id = params[-2]
                maybe_date = params[-3] if len(params) >= 3 else None
                if isinstance(maybe_date, date) and isinstance(maybe_id, int):
                    cur_date, cur_id = maybe_date, maybe_id
            if cur_date is not None and cur_id is not None:
                scoped = [
                    r
                    for r in scoped
                    if (r["admission_date"], r["history_id"]) < (cur_date, cur_id)
                ]

        scoped = sorted(
            scoped,
            key=lambda r: (r["admission_date"], r["history_id"]),
            reverse=True,
        )
        limit = None
        if params and isinstance(params[-1], int):
            limit = int(params[-1])
        if limit is not None:
            scoped = scoped[:limit]
        return [dict(r) for r in scoped]

    def _scoped(self, rows: list[dict], sql: str, params: tuple) -> list[dict]:
        out = list(rows)
        company_id = None
        office_id = None
        date_from = None
        date_to_excl = None
        history_id = None

        if "c.history_id = %s" in sql and "h.admission_date" not in sql.lower():
            # get_reception: version, company, office, history_id
            if len(params) >= 4 and params[0] == CALCULATION_VERSION_PIN:
                company_id = int(params[1])
                office_id = int(params[2])
                history_id = int(params[3])
        elif len(params) >= 5 and params[0] == CALCULATION_VERSION_PIN:
            company_id = int(params[1])
            office_id = int(params[2])
            if isinstance(params[3], date):
                date_from = params[3]
            if isinstance(params[4], date):
                date_to_excl = params[4]

        out = [
            r
            for r in out
            if r.get("calculation_version") == CALCULATION_VERSION_PIN
            and (company_id is None or int(r["company_id"]) == company_id)
            and (office_id is None or int(r["office_id"]) == office_id)
        ]
        if history_id is not None:
            out = [r for r in out if int(r["history_id"]) == history_id]
        if isinstance(date_from, date) and isinstance(date_to_excl, date):
            out = [
                r
                for r in out
                if r["admission_date"] >= date_from and r["admission_date"] < date_to_excl
            ]
        # status ANY / warnings ?|
        for p in params:
            if isinstance(p, list) and p and all(isinstance(x, str) for x in p):
                if set(p) <= ALLOWED_QUALITY_STATUSES:
                    out = [r for r in out if r["effective_quality_status"] in set(p)]
                elif all(isinstance(x, str) for x in p):
                    wanted = set(p)
                    out = [
                        r
                        for r in out
                        if wanted.intersection(set(r.get("warnings_json") or []))
                    ]
        if "TRIM(COALESCE(h.barcode, '')) = %s" in sql:
            # barcode es el primer string que no es calculation_version ni like
            for p in params:
                if (
                    isinstance(p, str)
                    and p != CALCULATION_VERSION_PIN
                    and not p.startswith("%")
                    and "\\" not in p
                ):
                    out = [r for r in out if (r.get("barcode") or "").strip() == p]
                    break
        if "c.variant_id = %s" in sql:
            # variant_id is typically after scope fixed params
            for i, p in enumerate(params):
                if (
                    isinstance(p, int)
                    and i >= 5
                    and any(int(r["variant_id"]) == p for r in rows)
                ):
                    out = [r for r in out if int(r["variant_id"]) == p]
                    break
        if "h.document_number = %s" in sql:
            for i, p in enumerate(params):
                if (
                    isinstance(p, int)
                    and i >= 5
                    and any(r.get("document_number") == p for r in rows)
                ):
                    out = [
                        r
                        for r in out
                        if r.get("document_number") == p or r.get("reception_id") == p
                    ]
                    break
        if "ILIKE" in sql.upper():
            for p in params:
                if isinstance(p, str) and p.startswith("%") and p.endswith("%"):
                    term = p.strip("%").replace("\\", "").lower()
                    out = [
                        r
                        for r in out
                        if term in (r.get("barcode") or "").lower()
                        or term in (r.get("product_name") or "").lower()
                        or term in (r.get("variant_name") or "").lower()
                        or term in str(r.get("document_number") or "").lower()
                    ]
                    break
        return out


def _repo(rows: list[dict]) -> tuple[CostV2ReadRepository, ReadFake]:
    fake = ReadFake(rows)
    return CostV2ReadRepository(fake), fake


def _call_list(repo, fake, **kwargs):
    # Bypass DB connection: call repo through service mapping helpers
    from backend.services.cost_v2_read_service import _map_list_item
    from backend.schemas.cost_v2_read import (
        parse_iso_date,
        validate_date_range,
        validate_limit,
        normalize_statuses,
        normalize_warnings,
        decode_cursor,
        encode_cursor,
        DEFAULT_LIMIT,
    )

    company_id = kwargs.get("company_id", 3)
    office_id = kwargs.get("office_id", 3)
    d_from = parse_iso_date(kwargs.get("date_from", date(2026, 3, 25)))
    d_to = parse_iso_date(kwargs.get("date_to", date(2026, 6, 22)))
    validate_date_range(date_from=d_from, date_to=d_to)
    lim = validate_limit(int(kwargs.get("limit", DEFAULT_LIMIT)))
    statuses = normalize_statuses(kwargs.get("status"))
    warnings = normalize_warnings(kwargs.get("warning"))
    cur_adm = cur_hid = None
    if kwargs.get("cursor"):
        cur_adm, cur_hid = decode_cursor(kwargs["cursor"])
    rows = repo.list_receptions(
        company_id=company_id,
        office_id=office_id,
        date_from=d_from,
        date_to=d_to,
        limit=lim,
        cursor_admission_date=cur_adm,
        cursor_history_id=cur_hid,
        statuses=statuses,
        warnings=warnings,
        barcode=kwargs.get("barcode"),
        variant_id=kwargs.get("variant_id"),
        document_number=kwargs.get("document_number"),
        history_id=kwargs.get("history_id"),
        search=kwargs.get("search"),
    )
    has_more = len(rows) > lim
    page = rows[:lim]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = encode_cursor(
            admission_date=last["admission_date"], history_id=int(last["history_id"])
        )
    return {
        "items": [_map_list_item(r) for r in page],
        "page": {"limit": lim, "has_more": has_more, "next_cursor": next_cursor},
        "meta": {
            "data_source": DATA_SOURCE,
            "calculation_version": CALCULATION_VERSION_PIN,
        },
        "_sqls": fake.sqls,
    }


# ---------------------------------------------------------------------------
# Unit / schema
# ---------------------------------------------------------------------------


def test_01_list_works():
    rows = [_row(i, admission_date=date(2026, 6, 1)) for i in range(1, 6)]
    repo, fake = _repo(rows)
    out = _call_list(repo, fake, limit=50)
    assert len(out["items"]) == 5
    assert out["meta"]["data_source"] == DATA_SOURCE
    assert out["meta"]["calculation_version"] == CALCULATION_VERSION_PIN


def test_02_03_uses_calculated_table_and_version():
    repo, fake = _repo([_row(1)])
    _call_list(repo, fake)
    sql = " ".join(fake.sqls).lower()
    assert "cost_reception_calculated" in sql
    assert "calculation_version" in sql
    assert "variant_cost" not in sql
    assert "products.taxes" not in sql and "p.taxes" not in sql
    assert "offset" not in sql
    assert "*" not in LIST_SELECT.split("FROM")[0] or "SELECT\n    c.history_id" in LIST_SELECT


def test_04_05_company_office_scope():
    rows = [
        _row(1, company_id=3, office_id=3),
        _row(2, company_id=9, office_id=3),
        _row(3, company_id=3, office_id=99),
    ]
    repo, fake = _repo(rows)
    out = _call_list(repo, fake)
    ids = {i["history_id"] for i in out["items"]}
    assert ids == {1}


def test_06_07_date_inclusive_exclusive_next_day():
    rows = [
        _row(1, admission_date=date(2026, 3, 25)),
        _row(2, admission_date=date(2026, 6, 22)),
        _row(3, admission_date=date(2026, 6, 23)),
    ]
    repo, fake = _repo(rows)
    out = _call_list(
        repo, fake, date_from=date(2026, 3, 25), date_to=date(2026, 6, 22)
    )
    ids = {i["history_id"] for i in out["items"]}
    assert ids == {1, 2}


def test_08_09_10_limits():
    assert validate_limit(50) == 50
    with pytest.raises(CostV2ReadValidationError):
        validate_limit(201)
    rows = [_row(i) for i in range(1, 60)]
    repo, fake = _repo(rows)
    out = _call_list(repo, fake)  # default 50
    assert out["page"]["limit"] == 50
    assert len(out["items"]) == 50
    assert out["page"]["has_more"] is True


def test_11_12_13_keyset_pagination():
    rows = [
        _row(i, admission_date=date(2026, 6, 10)) for i in range(1, 21)
    ]
    repo, fake = _repo(rows)
    p1 = _call_list(repo, fake, limit=7)
    assert len(p1["items"]) == 7
    assert p1["page"]["has_more"] is True
    cursor = p1["page"]["next_cursor"]
    p2 = _call_list(repo, fake, limit=7, cursor=cursor)
    ids1 = [i["history_id"] for i in p1["items"]]
    ids2 = [i["history_id"] for i in p2["items"]]
    assert not set(ids1).intersection(ids2)
    # no omissions within first 14 of DESC order
    expected = sorted(range(1, 21), reverse=True)
    assert ids1 + ids2 == expected[:14]
    assert all("OFFSET" not in s.upper() for s in fake.sqls)


def test_14_invalid_cursor():
    with pytest.raises(CostV2ReadValidationError):
        decode_cursor("%%%not-valid%%%")


def test_15_16_status_warning_filters():
    rows = [
        _row(1, status="missing_taxes_in_gross", warnings=[]),
        _row(2, status="incomplete_tax_context", warnings=["reception_tax_context_unavailable"]),
        _row(3, status="missing_taxes_in_gross", warnings=["suspicious_outlier"]),
    ]
    repo, fake = _repo(rows)
    out = _call_list(repo, fake, status=["incomplete_tax_context"])
    assert [i["history_id"] for i in out["items"]] == [2]
    out2 = _call_list(repo, fake, warning=["suspicious_outlier"])
    assert [i["history_id"] for i in out2["items"]] == [3]
    with pytest.raises(CostV2ReadValidationError):
        normalize_statuses(["not_a_status"])


def test_17_18_19_barcode_variant_document():
    rows = [
        _row(1, barcode="AAA", variant_id=10, document_number=1),
        _row(2, barcode="7803473005960", variant_id=42, document_number=555),
    ]
    repo, fake = _repo(rows)
    assert _call_list(repo, fake, barcode="7803473005960")["items"][0]["history_id"] == 2
    assert _call_list(repo, fake, variant_id=42)["items"][0]["history_id"] == 2
    assert _call_list(repo, fake, document_number=555)["items"][0]["history_id"] == 2


def test_20_search_parametrized():
    rows = [
        _row(1, barcode="111"),
        _row(2, barcode="222"),
    ]
    rows[0]["product_name"] = "HARINA"
    rows[1]["product_name"] = "CARNE"
    repo, fake = _repo(rows)
    out = _call_list(repo, fake, search="hari")
    assert [i["history_id"] for i in out["items"]] == [1]
    assert "ILIKE" in " ".join(fake.sqls).upper()
    assert "%s" in " ".join(fake.sqls)


def test_21_22_23_detail():
    rows = [
        _row(23190, warnings=["suspicious_outlier"], corrected=D("3014.27"), net=D("2533"), gross_stored=D("2533"), iva=D("481.27")),
        _row(99, company_id=9),
    ]
    repo, fake = _repo(rows)
    from backend.services.cost_v2_read_service import _map_detail

    row = repo.get_reception(company_id=3, office_id=3, history_id=23190)
    detail = _map_detail(row)
    assert detail["history_id"] == 23190
    assert detail["suspicious_outlier"] is True
    assert detail["calculation"]["formula"] == "net + iva + additional_taxes"
    assert detail["source_history_fingerprint"]
    assert detail["calculation_version"] == CALCULATION_VERSION_PIN
    assert isinstance(detail["corrected_gross_cost"], str)
    assert not isinstance(detail["corrected_gross_cost"], float)

    assert repo.get_reception(company_id=3, office_id=3, history_id=99) is None
    assert repo.get_reception(company_id=9, office_id=3, history_id=23190) is None


def test_24_25_26_27_summary():
    rows = [
        _row(1, status="missing_taxes_in_gross", warnings=["suspicious_outlier"]),
        _row(2, status="missing_cost", corrected=None, iva=None, gross_stored=None),
        _row(
            3,
            status="incomplete_tax_context",
            corrected=None,
            iva=None,
            warnings=["reception_tax_context_unavailable"],
        ),
    ]
    rows[1]["stored_cost_net"] = None
    rows[1]["corrected_gross_cost"] = None
    repo, fake = _repo(rows)
    summary = repo.summarize(
        company_id=3,
        office_id=3,
        date_from=date(2026, 3, 25),
        date_to=date(2026, 6, 22),
    )
    assert summary["total_rows"] == 3
    assert sum(summary["by_status"].values()) == 3
    assert summary["by_warning"].get("suspicious_outlier") == 1
    blob = json.dumps(summary, default=str)
    assert "corrected_gross_sum" not in blob
    assert "unit_difference_sum" not in blob
    assert "purchase_impact" not in blob


def test_28_29_30_31_quality_rules():
    from backend.services.cost_v2_read_service import _map_list_item

    m = _map_list_item(
        _row(
            1,
            status="missing_cost",
            corrected=None,
            iva=None,
            net=D("0"),
            gross_stored=None,
        )
    )
    m["stored_cost_net"] = None  # simulate missing
    # rebuild with explicit nulls
    row = _row(1, status="missing_cost", corrected=None, iva=None, gross_stored=None)
    row["stored_cost_net"] = None
    row["corrected_gross_cost"] = None
    row["calculated_iva_amount"] = None
    m = _map_list_item(row)
    assert m["corrected_gross_cost"] is None
    inc = _map_list_item(
        _row(2, status="incomplete_tax_context", corrected=None, iva=None)
    )
    assert inc["corrected_gross_cost"] is None
    miss_tax = _map_list_item(
        _row(
            3,
            status="missing_taxes_in_gross",
            corrected=D("773.50"),
            gross_stored=D("650"),
        )
    )
    assert miss_tax["corrected_gross_cost"] == "773.50"
    assert miss_tax["unit_difference"] == money_to_json(D("123.50"))
    out = _map_list_item(_row(4, warnings=["suspicious_outlier"]))
    assert out["suspicious_outlier"] is True
    assert out["effective_quality_status"] == "missing_taxes_in_gross"


def test_32_to_36_known_history_ids():
    rows = [
        _row(23190, barcode="7803473005960", warnings=["suspicious_outlier"], net=D("2533"), gross_stored=D("2533"), corrected=D("3014.27"), iva=D("481.27")),
        _row(15978, tax_ids=[1, 6], additional=[{"tax_id": 6, "kind": "iva_advance", "rate": "0.12", "amount": "79.68"}], net=D("664"), corrected=D("869.84")),
        _row(19076, tax_ids=[1, 7], additional=[{"tax_id": 7, "kind": "iva_advance", "rate": "0.05"}], net=D("7770"), corrected=D("9634.80")),
        _row(16822, status="incomplete_tax_context", corrected=None, iva=None, warnings=["reception_tax_context_unavailable"]),
        _row(15941, status="missing_cost", net=D("0"), gross_stored=None, corrected=None, iva=None),
    ]
    rows[-1]["stored_cost_net"] = None
    rows[-1]["corrected_gross_cost"] = None
    rows[-1]["calculated_iva_amount"] = None
    rows[1]  # flour
    # incomplete
    rows[3]["corrected_gross_cost"] = None
    rows[3]["calculated_iva_amount"] = None
    repo, fake = _repo(rows)
    from backend.services.cost_v2_read_service import _map_detail

    mankeke = _map_detail(repo.get_reception(company_id=3, office_id=3, history_id=23190))
    assert mankeke["suspicious_outlier"] is True
    flour = _map_detail(repo.get_reception(company_id=3, office_id=3, history_id=15978))
    assert flour["calculation"]["iva"]["tax_id"] == 1
    assert any(a.get("tax_id") == 6 for a in flour["additional_taxes"])
    meat = _map_detail(repo.get_reception(company_id=3, office_id=3, history_id=19076))
    assert meat["calculation"]["iva"]["tax_id"] == 1
    assert any(a.get("tax_id") == 7 for a in meat["additional_taxes"])
    ctx = _map_detail(repo.get_reception(company_id=3, office_id=3, history_id=16822))
    assert ctx["corrected_gross_cost"] is None
    zero = _map_detail(repo.get_reception(company_id=3, office_id=3, history_id=15941))
    assert zero["corrected_gross_cost"] is None
    assert zero["effective_quality_status"] == "missing_cost"


def test_37_advances_do_not_replace_iva():
    from backend.services.cost_v2_read_service import _map_detail

    row = _row(
        15978,
        tax_ids=[1, 6],
        additional=[{"tax_id": 6, "kind": "iva_advance"}],
        iva=D("126.16"),
    )
    d = _map_detail(row)
    assert d["calculation"]["iva"]["tax_id"] == 1
    assert d["calculation"]["iva"]["rate"] == "0.19"
    assert d["calculation"]["iva"]["tax_id"] != 6


def test_38_decimal_not_float():
    assert isinstance(unit_difference(D("10"), D("1")), Decimal)
    assert money_to_json(D("3014.27")) == "3014.27"
    assert not isinstance(money_to_json(D("3014.27")), float)


def test_39_40_41_42_43_no_forbidden_ops():
    repo, fake = _repo([_row(1)])
    _call_list(repo, fake)
    repo.summarize(
        company_id=3, office_id=3, date_from=date(2026, 3, 25), date_to=date(2026, 6, 22)
    )
    assert fake.write_attempts == 0
    joined = " ".join(fake.sqls).lower()
    assert "variant_cost" not in joined
    assert "insert " not in joined
    assert "update " not in joined
    assert "delete " not in joined


def test_44_legacy_routes_intact():
    # Evitar import circular auth: leer el fuente del router
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "routers" / "cost_analytics.py"
    text = src.read_text(encoding="utf-8")
    assert '@router.get("/receptions")' in text
    assert '@router.get("/v2/receptions")' in text
    assert '@router.get("/v2/summary")' in text
    assert '@router.get("/v2/receptions/{history_id}")' in text
    assert "svc.list_receptions" in text
    assert "v2svc.list_v2_receptions" in text
    # legacy sync endpoint permanece
    assert '@router.post("/sync")' in text


def test_45_no_credentials_in_response():
    repo, fake = _repo([_row(1)])
    out = _call_list(repo, fake)
    blob = json.dumps(out).lower()
    assert "password" not in blob
    assert "database_url" not in blob
    assert "postgres://" not in blob


def test_46_47_48_49_detail_meta_and_explicit_columns():
    assert "c.history_id" in LIST_SELECT
    assert "SELECT *" not in LIST_SELECT
    repo, fake = _repo([_row(23190)])
    from backend.services.cost_v2_read_service import _map_detail

    d = _map_detail(repo.get_reception(company_id=3, office_id=3, history_id=23190))
    assert d["calculation_result_fingerprint"]
    assert d["tax_context_fingerprint"]
    assert d["source_history_fingerprint"]
    assert d["calculation_version"] == CALCULATION_VERSION_PIN
    out = _call_list(repo, fake)
    assert out["meta"]["data_source"]
    assert out["meta"]["calculation_version"]


def test_history_version_unique_invariant():
    """history_id + calculation_version = una sola fila (UNIQUE / conteo)."""
    rows = [_row(1), _row(2)]
    repo, fake = _repo(rows)
    inv = repo.count_history_version_pairs(
        company_id=3,
        office_id=3,
        date_from=date(2026, 3, 25),
        date_to=date(2026, 6, 22),
    )
    assert inv["n"] == inv["n_pairs"]


def test_unit_difference_requires_both():
    assert unit_difference(None, D("1")) is None
    assert unit_difference(D("1"), None) is None
    assert unit_difference(D("10"), D("7")) == D("3")


def test_encode_decode_cursor_roundtrip():
    tok = encode_cursor(admission_date=date(2026, 6, 1), history_id=23190)
    adm, hid = decode_cursor(tok)
    assert adm == date(2026, 6, 1)
    assert hid == 23190
