"""Consulta puntual Bsale API para validar stock (oficina 1, company token).

Uso (local, con token):
  set BSALE_TOKEN=...
  python -m backend.scripts.catalog_v2_bsale_point_check 28216 29094 31199

Sin token: sale con código 2 y no llama red. No escribe BD. No es cron.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API = "https://api.bsale.io/v1"
DEFAULT_IDS = [28216, 29094, 31199, 28258, 30814, 30293, 23312, 31199]


def _get(token: str, path: str, params: dict) -> dict:
    url = f"{API}{path}?{urlencode(params)}"
    req = Request(url, headers={"access_token": token, "Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main(argv: list[str]) -> int:
    token = (os.getenv("BSALE_TOKEN") or os.getenv("BSALE_TOKEN_SPA") or "").strip()
    ids = [int(x) for x in argv[1:]] or DEFAULT_IDS
    now = datetime.now(timezone.utc).isoformat()
    if not token:
        print(
            json.dumps(
                {
                    "ok": False,
                    "reason": "BSALE_TOKEN absent",
                    "queried_at": now,
                    "variant_ids": ids,
                    "note_es": (
                        "Sin token local no se puede reconsultar Bsale en vivo. "
                        "Usar payload de bsale_raw.stocks como snapshot API."
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2

    rows = []
    for vid in ids:
        try:
            body = _get(
                token,
                "/stocks.json",
                {"variantid": vid, "officeid": 1, "limit": 5, "offset": 0},
            )
            items = body.get("items") or []
            hit = items[0] if items else None
            rows.append(
                {
                    "variant_id": vid,
                    "office_id": 1,
                    "ok": hit is not None,
                    "quantity": None if not hit else hit.get("quantity"),
                    "quantityAvailable": None if not hit else hit.get("quantityAvailable"),
                    "quantityReserved": None if not hit else hit.get("quantityReserved"),
                    "count": body.get("count"),
                }
            )
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            rows.append({"variant_id": vid, "ok": False, "error": str(e)[:160]})

    print(
        json.dumps(
            {
                "ok": True,
                "queried_at": now,
                "company_token_env": "BSALE_TOKEN",
                "office_id": 1,
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
