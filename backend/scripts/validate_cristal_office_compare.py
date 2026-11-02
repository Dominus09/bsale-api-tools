#!/usr/bin/env python3
"""Validación READ-ONLY Cristal 11053 para Comparativo por oficina.

Requiere DATABASE_URL del backend (usuario app con SELECT en bsale_raw).
No escribe. No usa analytics.cost_reception_history.

Uso:
  python -m backend.scripts.validate_cristal_office_compare --company-id 3
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import make_psycopg_executor
from backend.services.analytics_v2.access import CompanyScope
from backend.services.analytics_v2.cost_control_repo import CostControlV2Repository
from backend.services.analytics_v2.cost_control_service import CostControlV2Service


VARIANT_ID = 11053


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--company-id", type=int, default=3)
    args = parser.parse_args(argv)

    conn = get_connection()
    try:
        cur = conn.cursor()
        repo = CostControlV2Repository(make_psycopg_executor(cur))
        svc = CostControlV2Service(
            repo,
            CompanyScope(company_id=args.company_id, company_name="La Quillotana SPA"),
            today=date.today(),
            use_cache=False,
        )
        detail = svc.get_office_compare_product(VARIANT_ID)
        timeline = svc.office_compare_timeline(
            VARIANT_ID,
            date_from=date(2026, 8, 1),
            date_to=date(2026, 10, 31),
            page_size=100,
        )
        by_class: dict[str, int] = {}
        for line in timeline.get("items") or []:
            label = line.get("movement_label") or line.get("movement_class") or "?"
            by_class[str(label)] = by_class.get(str(label), 0) + 1
        payload = {
            "ok": True,
            "variant_id": VARIANT_ID,
            "product_name": detail.get("product_name"),
            "variant_name": detail.get("variant_name"),
            "bsale_average_cost": detail.get("bsale_average_cost"),
            "offices": [
                {
                    "office_id": c["office_id"],
                    "office_name": c["office_name"],
                    "stock": c.get("stock"),
                    "info_tag": c.get("info_tag"),
                    "last_reception_unit_cost": c.get("last_reception_unit_cost"),
                    "last_supplier_purchase": c.get("last_supplier_purchase"),
                    "last_guide": c.get("last_guide"),
                }
                for c in detail.get("offices") or []
            ],
            "aug_oct_2026": {
                "lines": timeline.get("total"),
                "by_movement_label": by_class,
                "sample": (timeline.get("items") or [])[:10],
            },
            "note": (
                "Si aug_oct_2026.lines == 0, RAW no tiene movimientos del SKU en ese "
                "rango o el token/DB no alcanzan stock_receptions."
            ),
        }
        print(json.dumps(payload, default=str, ensure_ascii=False, indent=2))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
