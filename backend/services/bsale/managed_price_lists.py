"""
Listas de precios Bsale administradas por la ERP, por empresa.

Única fuente de verdad del alcance del sync de ``bsale.variant_prices``: sólo estas listas se
descargan, reconcilian y pueden quedar degradadas. Cualquier otra lista de Bsale (históricas,
inactivas o retiradas, p. ej. COMODITI absorbida por Ruta/Web) se ignora por completo.

Para agregar o retirar una lista, editar ``MANAGED_PRICE_LISTS``.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from backend.services.bsale.sync_common import CompanySyncError

MANAGED_PRICE_LISTS: Mapping[int, tuple[int, ...]] = MappingProxyType(
    {
        1: (2, 3),  # Minimarket, Ruta/Web
        2: (3, 13),  # QUILLOTANA V, RUTA/WEB (BOLETA O FACTURA)
        3: (12, 13, 16),  # SUPERMERCADO LA QUILLOTANA, RUTA/WEB (BOLETA O FACTURA), MELINKA
    }
)


def managed_price_lists(
    company_id: int, config: Mapping[int, tuple[int, ...]] | None = None
) -> tuple[int, ...]:
    """Listas administradas de la empresa; falla si la empresa no tiene configuración."""
    lists = (MANAGED_PRICE_LISTS if config is None else config).get(company_id)
    if not lists:
        raise CompanySyncError(
            f"company_id={company_id} sin listas de precios administradas configuradas "
            "(backend/services/bsale/managed_price_lists.py)"
        )
    if len(set(lists)) != len(lists):
        raise CompanySyncError(f"company_id={company_id}: listas administradas duplicadas {lists}")
    return tuple(int(x) for x in lists)
