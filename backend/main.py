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
from backend.db import get_connection


app = FastAPI(
    title="Quillotana Analytics API",
    version="1.0"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://cat.quillotana.cl",
        "http://localhost:3000",
    ],
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
    return {"status": "API funcionando"}
    

@app.get("/test-db")
def test_db():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        conn.close()

        return {
            "status": "ok",
            "db_response": result
        }

    except Exception as e:
        return {"error": str(e)}