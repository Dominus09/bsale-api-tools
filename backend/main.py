import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

logger = logging.getLogger(__name__)

from backend.cors_middleware import QuillotanaCorsMiddleware
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


async def _distribuidora_bsale_sync_background_loop() -> None:
    """Ejecuta sync_bsale_distribuidora cada DISTRIBUIDORA_BSALE_SYNC_INTERVAL_SEC (default 30 min)."""
    from backend.jobs.sync_bsale_distribuidora import sync_bsale_distribuidora

    delay_first = int(os.getenv("DISTRIBUIDORA_BSALE_SYNC_START_DELAY_SEC", "45"))
    interval = int(os.getenv("DISTRIBUIDORA_BSALE_SYNC_INTERVAL_SEC", str(30 * 60)))
    if interval < 120:
        interval = 120
    await asyncio.sleep(max(0, delay_first))
    while True:
        try:
            sync_bsale_distribuidora()
        except Exception:
            logger.exception("sync_bsale_distribuidora (job programado) falló")
        await asyncio.sleep(interval)


async def _rutero_sync_background_loop() -> None:
    """Ejecuta sync_rutero cada RUTERO_SYNC_INTERVAL_SEC (default 6 h)."""
    from backend.jobs.sync_rutero import sync_rutero

    delay_first = int(os.getenv("RUTERO_SYNC_START_DELAY_SEC", "15"))
    interval = int(os.getenv("RUTERO_SYNC_INTERVAL_SEC", str(6 * 3600)))
    if interval < 60:
        interval = 60
    await asyncio.sleep(max(0, delay_first))
    while True:
        try:
            sync_rutero()
        except Exception:
            logger.exception("sync_rutero (job programado) falló")
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks: list[asyncio.Task] = []
    rutero_disabled = os.getenv("RUTERO_SYNC_DISABLED", "").strip().lower() in ("1", "true", "yes")
    if not rutero_disabled:
        tasks.append(asyncio.create_task(_rutero_sync_background_loop()))
    dist_disabled = os.getenv("DISTRIBUIDORA_BSALE_SYNC_DISABLED", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if not dist_disabled:
        tasks.append(asyncio.create_task(_distribuidora_bsale_sync_background_loop()))
    yield
    for task in tasks:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(
    title="Quillotana Analytics API",
    version="1.0",
    lifespan=lifespan,
)
# CORS: middleware ASGI propio (preflight + ACAO en cada respuesta) además de ser tolerante
# con proxies; el panel usa Bearer, sin cookies → no hace falta Access-Control-Allow-Credentials.
app.add_middleware(
    QuillotanaCorsMiddleware,
    allow_origins=_cors_allow_origins(),
    allow_origin_regex=_cors_allow_origin_regex(),
)

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


@app.get("/")
def root():
    return {"status": "API funcionando"}
