from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.routers import auth
from backend.routers import orders
from backend.routers.catalog import router as catalog_router

# Panel / analytics (rutas legacy usadas por el frontend admin)
from backend.routers import alerts
from backend.routers import companies
from backend.routers import dashboard
from backend.routers import margin_export
from backend.routers import margin_problems
from backend.routers import margins
from backend.routers import products
from backend.routers import summary

# ERP (prefijo /erp: dashboard, alertas, márgenes internos)
from backend.routers.erp import router as erp_router

app = FastAPI(
    title="Quillotana Analytics API",
    version="1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://cat.quillotana.cl",
        "https://work.quillotana.cl",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
app.include_router(margins.router)
app.include_router(alerts.router)
app.include_router(summary.router)
app.include_router(products.router)
app.include_router(margin_problems.router)
app.include_router(margin_export.router)

# --- ERP ---
app.include_router(erp_router)


@app.get("/")
def root():
    return {"status": "API funcionando"}
