from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.routers import companies
from backend.routers import dashboard
from backend.routers import margins
from backend.routers import alerts
from backend.routers import summary
from backend.routers import products
from backend.routers import margin_problems
from backend.routers import margin_export
from backend.routers import auth
from backend.routers.catalog import router as catalog_router
from backend.routers.erp import router as erp_router


app = FastAPI(
    title="Quillotana Analytics API",
    version="1.0"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # permite llamadas desde cualquier frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    
)

app.include_router(companies.router)
app.include_router(dashboard.router)
app.include_router(margins.router)
app.include_router(alerts.router)
app.include_router(summary.router)
app.include_router(products.router)
app.include_router(margin_problems.router)
app.include_router(margin_export.router)
app.include_router(auth.router)
app.include_router(catalog_router, prefix="/api")
app.include_router(erp_router)

@app.get("/")
def root():
    return {"status": "API running"}
