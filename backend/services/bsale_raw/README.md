# backend/services/bsale_raw

Capa espejo fiel de la API Bsale: **Bsale API → `bsale_raw` → `bsale` → products_master / ERP**.

Documentación:

- Arquitectura: [`docs/BSALE_RAW_ARCHITECTURE.md`](../../../docs/BSALE_RAW_ARCHITECTURE.md)
- Matriz de endpoints (oficial): [`docs/BSALE_RAW_ENDPOINT_MATRIX.md`](../../../docs/BSALE_RAW_ENDPOINT_MATRIX.md)
- Inventario de syncs existentes: [`docs/BSALE_SYNC_INDEX.md`](../../../docs/BSALE_SYNC_INDEX.md)

## Estado: fase 1 (scaffold)

No hay escritura a BD, ni jobs, ni endpoint HTTP de webhooks. Sólo piezas puras y testeadas:

| Módulo | Contenido |
|---|---|
| `core/rate_limit.py` | `TokenBucket` por empresa, `CompanyRateLimiters`, `RateLimitedSession` (cuenta también los reintentos). |
| `core/client.py` | `build_company_client`: reutiliza `backend.services.bsale.http_client.BsaleHttpClient` (timeouts, retry 408/425/429/5xx, Retry-After, validación de host) con la sesión limitada. |
| `core/models.py` | `RawRecord`, `StockRecord`, `payload_hash`, enums de modo, estado de corrida y estado de webhook. |
| `core/registry.py` | `ResourceSpec` + `REGISTRY` (un motor, N recursos declarativos). |
| `core/freshness.py` | `FreshnessState` y evaluación contra SLA. |
| `resources/*.py` | Declaración de recursos según la matriz. |
| `webhooks/__init__.py` | `parse_webhook` (cpnId → company_id) y `route` (refrescos puntuales). |

## Reglas

- Identidad siempre `(company_id, bsale_id)`; ningún id Bsale es global.
- Sin reglas de negocio: si Bsale dice variante X → producto Y, se guarda así.
- SKU y código de barras se guardan como columnas de búsqueda, nunca como PK.
- Tokens sólo en variables de entorno; nunca en PostgreSQL ni en logs.
- No modifica `backend/services/bsale/*` ni los syncs legacy.

Tests: `python -m pytest backend/tests/bsale_raw -q`.
