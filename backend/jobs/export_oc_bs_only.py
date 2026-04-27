from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

if __package__ is None or __package__ == "":
    # Permite ejecutar como archivo directo sin depender del módulo.
    sys.path.append(str(Path(__file__).resolve().parents[2]))

BASE_URL = "https://api.bsale.io/v1"
OC_LIMIT = 50
SLEEP_BETWEEN_CALLS = 0.1
MAX_ATTEMPTS = 40

BSALE_TOKEN = os.getenv("BSALE_TOKEN")
if not BSALE_TOKEN:
    raise Exception("Falta BSALE_TOKEN en variables de entorno")


def _safe_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _document_type_id(item: dict[str, Any]) -> int | None:
    dt = item.get("document_type") or item.get("documentType")
    if isinstance(dt, dict):
        return _safe_int(dt.get("id"))
    return _safe_int(dt)


def _get(session: requests.Session, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = path if path.startswith("http") else f"{BASE_URL}{path}"
    params = params or {}
    attempts = 0

    while True:
        attempts += 1
        try:
            r = session.get(
                url,
                headers={"access_token": BSALE_TOKEN},
                params=params,
                timeout=45,
            )
        except requests.RequestException as e:
            if attempts >= MAX_ATTEMPTS:
                raise RuntimeError(f"Error de red Bsale: {e}") from e
            time.sleep(5)
            continue

        if r.status_code == 429:
            if attempts >= MAX_ATTEMPTS:
                raise RuntimeError("Bsale 429 persistente")
            time.sleep(5)
            continue

        if r.status_code in (500, 502, 503, 504):
            if attempts >= MAX_ATTEMPTS:
                raise RuntimeError(f"Bsale HTTP {r.status_code} persistente")
            time.sleep(5)
            continue

        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"Bsale HTTP {r.status_code}: {(r.text or '')[:400]}")

        time.sleep(SLEEP_BETWEEN_CALLS)
        return r.json()


def _count_details(session: requests.Session, document_id: int) -> tuple[int, list[int]]:
    data = _get(session, f"/documents/{document_id}/details.json")
    items = data.get("items") or []
    if not isinstance(items, list):
        return 0, []

    detail_ids: list[int] = []
    for it in items[:3]:
        if isinstance(it, dict):
            did = _safe_int(it.get("id"))
            if did is not None:
                detail_ids.append(did)
    return len(items), detail_ids


def _related_stats(session: requests.Session, detail_ids: list[int]) -> tuple[int, str]:
    related_items_count = 0
    related_types: set[int] = set()

    for detail_id in detail_ids:
        data = _get(session, "/documents.json", {"relateddetailid": detail_id})
        items = data.get("items") or []
        if not isinstance(items, list):
            continue
        related_items_count += len(items)
        for it in items:
            if isinstance(it, dict):
                tid = _document_type_id(it)
                if tid is not None:
                    related_types.add(tid)

    related_types_str = ",".join(str(t) for t in sorted(related_types)) if related_types else ""
    return related_items_count, related_types_str


def _references_count(session: requests.Session, document_id: int) -> int:
    data = _get(session, f"/documents/{document_id}/references.json")
    items = data.get("items")
    if items is None:
        items = data.get("references") or []
    return len(items) if isinstance(items, list) else 0


def run() -> int:
    session = requests.Session()

    docs_data = _get(
        session,
        "/documents.json",
        {"documenttypeid": 33, "limit": OC_LIMIT},
    )
    docs = docs_data.get("items") or []
    if not isinstance(docs, list):
        docs = []

    rows: list[dict[str, Any]] = []

    for oc in docs:
        if not isinstance(oc, dict):
            continue
        document_id = _safe_int(oc.get("id"))
        number = oc.get("number")
        total = oc.get("totalAmount")
        client = oc.get("client")
        client_id = _safe_int(client.get("id")) if isinstance(client, dict) else None

        if document_id is None:
            continue

        try:
            details_count, detail_ids = _count_details(session, document_id)
            related_items_count, related_types = _related_stats(session, detail_ids)
            references_count = _references_count(session, document_id)
        except Exception as error:
            print(f"❌ Error en OC {number}: {error}")
            continue

        rows.append(
            {
                "OC_number": number,
                "document_id": document_id,
                "client_id": client_id,
                "total": total,
                "details_count": details_count,
                "related_types": related_types,
                "related_items_count": related_items_count,
                "references_count": references_count,
            }
        )

    df = pd.DataFrame(rows)
    df.to_excel("oc_analysis.xlsx", index=False)
    print("✅ Excel generado: oc_analysis.xlsx")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
