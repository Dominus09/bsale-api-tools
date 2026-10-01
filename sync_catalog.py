"""
Sync de catálogo Bsale (taxes, product_types, price_lists, offices, products, variants).

Uso: python sync_catalog.py
Lógica en backend/services/bsale/catalog_company_sync.py. Exit 0 sólo si todas las empresas OK.
"""

from backend.services.bsale.catalog_job import run_single_phase_script

if __name__ == "__main__":
    raise SystemExit(run_single_phase_script("catalog"))
