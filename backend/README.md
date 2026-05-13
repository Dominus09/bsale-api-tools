# Backend ERP

API **FastAPI** definida en `backend/main.py` (montaje de routers, CORS, middlewares y jobs en `lifespan`).

## Tooling y scripts (FASE 6)

| Carpeta | Uso |
|---------|-----|
| `backend/jobs/` | Jobs de producción: `sync_*.py` (Coolify / cron). |
| `backend/audits/` | Auditorías solo lectura (`audit_document_related`). |
| `backend/maintenance/` | Limpiezas puntuales y utilidades operador (`cleanup_*`, `gen_*_password_hash`). |
| `backend/debug/` | Diagnósticos y exports analíticos. |
| `backend/rebuilds/` | Reservado (repoblados masivos futuros). |
| `backend/migrations/` | Reservado (runners DDL Python; DDL en `backend/sql/`). |
| `backend/scripts/` | **Shims** hacia los módulos anteriores (compatibilidad con rutas antiguas). |

- [Registro de scripts](SCRIPTS_REGISTRY.md) · [Plan consolidación](SCRIPTS_CONSOLIDATION_PLAN.md) · [Hardening](HARDENING_REPORT.md) · [Migración estructura](STRUCTURE_MIGRATION_REPORT.md)

## Documentación técnica

- [Análisis técnico backend](../docs/backend/ANALISIS_BACKEND_ERP.md)
- [Inventario de endpoints backend](../docs/backend/ENDPOINTS_BACKEND.md)
- [Limpieza propuesta backend](../docs/backend/LIMPIEZA_PROPUESTA_BACKEND.md)

## Ejecución local (referencia)

```bash
uvicorn backend.main:app --reload
```

Variables y puertos: ver sección de configuración en el análisis técnico enlazado arriba.
