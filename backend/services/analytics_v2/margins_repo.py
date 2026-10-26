"""SQL de Márgenes V2: precios y listas desde BSALE RAW; reglas desde bsale.margin_rules.

Solo lectura, siempre filtrado por empresa. Nunca lee ``payload``. bsale.margin_rules es la única
tabla fuera de RAW y se usa exclusivamente como fuente de reglas comerciales.
"""

from __future__ import annotations

from typing import Any, Callable, Sequence

from backend.services.analytics_v2.commercial_scope import PRICE_LISTS_SQL
from backend.services.analytics_v2.stock_scope import STOCK_TOTALS_SQL

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]

VARIANT_PRICES_SQL = """
SELECT vp.price_list_id, vp.variant_id, vp.variant_value, vp.variant_value_with_taxes,
       vp.missing_since, vp.api_fetched_at
FROM bsale_raw.variant_prices vp
WHERE vp.company_id = %s
  AND vp.price_list_id = ANY(%s)
"""

MARGIN_RULES_SQL = """
SELECT mr.id, mr.price_list_id, mr.price_list_name, mr.product_type_id, mr.product_type_name,
       mr.min_margin, mr.max_margin, mr.active, mr.rule_key
FROM bsale.margin_rules mr
WHERE mr.company_id = %s
ORDER BY mr.price_list_id, mr.product_type_id NULLS FIRST, mr.id
"""


class MarginsV2Repository:
    def __init__(self, execute: QueryExecutor) -> None:
        self._execute = execute

    def price_lists(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._execute(PRICE_LISTS_SQL, (company_id,))

    def variant_prices(self, *, company_id: int, price_list_ids: Sequence[int]) -> list[dict[str, Any]]:
        if not price_list_ids:
            return []
        return self._execute(VARIANT_PRICES_SQL, (company_id, list(price_list_ids)))

    def margin_rules(self, *, company_id: int) -> list[dict[str, Any]]:
        return self._execute(MARGIN_RULES_SQL, (company_id,))

    def stock_totals(self, *, company_id: int) -> list[dict[str, Any]]:
        """Stock físico total por variante (sum(quantity), todas las sucursales).

        Una fila por variante (GROUP BY sobre la PK de bsale_raw.stocks):
        el join posterior es por variante y nunca multiplica existencias
        cuando la variante aparece en varias listas de precios.
        """
        return self._execute(STOCK_TOTALS_SQL, (company_id,))
