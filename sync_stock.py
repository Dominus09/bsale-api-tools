"""
Sync de stock Bsale (bsale.stocks) con reconciliación segura.

Uso: python sync_stock.py
Lógica en backend/services/bsale/stock_sync.py. Exit 0 sólo si todas las empresas OK.
"""

from backend.services.bsale.catalog_job import run_single_phase_script

if __name__ == "__main__":
    raise SystemExit(run_single_phase_script("stock"))
