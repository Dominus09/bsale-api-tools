import logging
import os

from fastapi import FastAPI

logger = logging.getLogger(__name__)

from backend.cors_middleware import QuillotanaCorsMiddleware
from backend.middleware.distribuidora_request_log import DistribuidoraRequestLogMiddleware
from backend.diagnostics.middleware import DiagnosticsRequestLogMiddleware
from backend.routers import auth
from backend.routers import orders
from backend.routers.catalog import router as catalog_router

# Panel / analytics (rutas legacy usadas por el frontend admin)
from backend.routers import alerts
from backend.routers import companies
from backend.routers import dashboard
from backend.routers import distribuidora
from backend.routers import distribuidora_orders
from backend.routers import distribuidora_planificacion
from backend.routers import distribuidora_clients
from backend.routers import distribuidora_planning
from backend.routers import distribuidora_route_picking
from backend.routers import distribuidora_route_planning
from backend.routers import distribuidora_sync
from backend.routers.distribuidora_sync import sync_router
from backend.routers import distribuidora_trucks
from backend.routers.app_distribuidora import router as app_distribuidora_router
from backend.routers import margin_export
from backend.routers import margin_problems
from backend.routers import margins
from backend.routers import offers
from backend.routers import price_lists
from backend.routers import promotions
from backend.routers import products
from backend.routers import products_master
from backend.routers import purchases
from backend.routers import suppliers
from backend.routers import summary
from backend.routers import uploads

# ERP (prefijo /erp: dashboard, alertas, márgenes internos)
from backend.routers.erp import router as erp_router
from backend.routers import diagnostics as diagnostics_router

def _cors_allow_origins() -> list[str]:
    """Orígenes permitidos para el front (subdominios quillotana + local)."""
    base = [
        "http://localhost:3000",
        "https://cat.quillotana.cl",
        "https://work.quillotana.cl",
        "https://test.quillotana.cl",
        "https://erp.quillotana.cl",
    ]
    extra = os.getenv("CORS_EXTRA_ORIGINS", "").strip()
    if extra:
        base.extend(o.strip() for o in extra.split(",") if o.strip())
    return base


def _cors_allow_origin_regex() -> str | None:
    """Cualquier host `*.quillotana.cl` en HTTPS (p. ej. test/work) sin redeploy por cada subdominio."""
    raw = os.getenv("CORS_ALLOW_ORIGIN_REGEX", "").strip()
    if raw:
        return raw
    # Coincide con test.quillotana.cl, work.quillotana.cl, etc. No incluye apex sin subdominio.
    return r"https://[a-z0-9-]+\.quillotana\.cl$"


app = FastAPI(
    title="Quillotana Analytics API",
    version="1.0",
)
# CORS: middleware ASGI propio (preflight + ACAO en cada respuesta) además de ser tolerante
# con proxies; el panel usa Bearer, sin cookies → no hace falta Access-Control-Allow-Credentials.
app.add_middleware(
    QuillotanaCorsMiddleware,
    allow_origins=_cors_allow_origins(),
    allow_origin_regex=_cors_allow_origin_regex(),
)
# Log de duración en ``/distribuidora`` (añadido después de CORS → capa exterior).
app.add_middleware(DistribuidoraRequestLogMiddleware)
# Diagnóstico: capa más externa para registrar status final y tiempos (sin payloads sensibles).
app.add_middleware(DiagnosticsRequestLogMiddleware)

# --- Auth (login staff + login-client) ---
app.include_router(auth.router)

# --- Pedidos (app.orders) ---
app.include_router(orders.router)

# --- Catálogo público ---
app.include_router(catalog_router, prefix="/api")

# --- Analytics / empresas (no mezclar con /erp: URLs distintas) ---
app.include_router(companies.router)
app.include_router(dashboard.router)
app.include_router(price_lists.router)
app.include_router(margins.router)
app.include_router(offers.router)
app.include_router(promotions.router)
app.include_router(alerts.router)
app.include_router(summary.router)
app.include_router(products.router)
app.include_router(products_master.router)
app.include_router(suppliers.router)
app.include_router(purchases.router)
app.include_router(uploads.router)
app.include_router(margin_problems.router)
app.include_router(margin_export.router)

# --- Distribuidora ---
app.include_router(distribuidora.router)
app.include_router(distribuidora_sync.router)
app.include_router(sync_router)
app.include_router(distribuidora_orders.router)
app.include_router(distribuidora_planificacion.router)
app.include_router(distribuidora_planning.router)
app.include_router(distribuidora_clients.router)
app.include_router(distribuidora_route_planning.router)
app.include_router(distribuidora_route_picking.router)
app.include_router(distribuidora_trucks.router)

# --- App móvil / rutas del día (bsale.rutas_dia + visitas, sync offline) ---
app.include_router(app_distribuidora_router, prefix="/app_distribuidora")

# --- ERP ---
app.include_router(erp_router)

# --- Diagnóstico interno (JWT admin + ENABLE_DIAGNOSTICS en prod/staging) ---
app.include_router(diagnostics_router.router)


@app.on_event("startup")
def _startup_attach_diagnostics_log_handler() -> None:
    from backend.diagnostics.logging_handler import attach_memory_log_handler

    attach_memory_log_handler()


@app.get("/")
def root():
    return {"status": "API funcionando"}
