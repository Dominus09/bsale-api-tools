# Backend ERP

API **FastAPI** definida en `backend/main.py` (montaje de routers, CORS, middlewares y jobs en `lifespan`).

## Documentación técnica

- [Análisis técnico backend](../docs/backend/ANALISIS_BACKEND_ERP.md)
- [Inventario de endpoints backend](../docs/backend/ENDPOINTS_BACKEND.md)
- [Limpieza propuesta backend](../docs/backend/LIMPIEZA_PROPUESTA_BACKEND.md)

## Ejecución local (referencia)

```bash
uvicorn backend.main:app --reload
```

Variables y puertos: ver sección de configuración en el análisis técnico enlazado arriba.
