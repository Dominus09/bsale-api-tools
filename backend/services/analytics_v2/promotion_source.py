"""Fuente de excepciones de Promociones V2 para Márgenes V2 (solo lectura).

Entrega como candidatos los ítems activos de campañas aprobadas o pendientes que cubren la fecha
evaluada, más las promociones legacy activas y mapeables (que nunca otorgan excepción). Si las
tablas V2 aún no existen la fuente se declara no integrada y Márgenes V2 no concede nada.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable

from backend.services.analytics_v2.commercial_scope import CommercialScopeNotConfigured, resolve_price_list_scope
from backend.services.analytics_v2.promotion_campaigns import (
    LEGACY_MAPPEABLE,
    campaign_item_exception,
    legacy_exception,
    legacy_mapping_status,
)
from backend.services.analytics_v2.promotion_exceptions import PromotionException
from backend.services.analytics_v2.promotions_repo import PromotionsV2Repository


class CampaignExceptionSource:
    def __init__(self, repo: PromotionsV2Repository, *, today: date) -> None:
        self._repo = repo
        self._today = today
        self._available: bool | None = None

    @property
    def available(self) -> bool:
        if self._available is None:
            self._available = self._repo.tables_ready()
        return self._available

    def exceptions_for(self, company_id: int) -> Iterable[PromotionException]:
        if not self.available:
            return []
        out: list[PromotionException] = []
        for row in self._repo.covering_items(company_id=company_id, date_from=self._today, date_to=self._today):
            exc = campaign_item_exception(row)
            if exc is not None:
                out.append(exc)
        out.extend(self._legacy(company_id))
        return out

    def _legacy(self, company_id: int) -> list[PromotionException]:
        try:
            scope = resolve_price_list_scope(company_id, self._repo.price_lists(company_id=company_id))
        except CommercialScopeNotConfigured:
            return []
        out: list[PromotionException] = []
        for row in self._repo.legacy_snapshots(company_id=company_id):
            status, variant_id, list_id = legacy_mapping_status(row, scope)
            if status != LEGACY_MAPPEABLE or variant_id is None or list_id is None:
                continue
            exc = legacy_exception(row, company_id, variant_id, list_id)
            if exc is not None and exc.is_active_on(self._today):
                out.append(exc)
        return out
