"""
Sync de costos (variant_cost) y precios (variant_prices) Bsale con reconciliación segura.

Uso: python sync_prices_costs.py
Lógica en backend/services/bsale/prices_costs_sync.py. Exit 0 sólo si todas las empresas OK.
"""

from backend.services.bsale.catalog_job import run_single_phase_script

if __name__ == "__main__":
    raise SystemExit(run_single_phase_script("prices_costs"))
