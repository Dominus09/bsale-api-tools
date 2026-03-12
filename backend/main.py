from fastapi import FastAPI

from backend.routers import companies
from backend.routers import dashboard
from backend.routers import margin
from backend.routers import margin_problems

app = FastAPI(
    title="Quillotana Analytics API",
    version="1.0"
)

app.include_router(companies.router)
app.include_router(dashboard.router)
app.include_router(margin.router)
app.include_router(margin_problems.router)

@app.get("/")
def root():
    return {"status": "API running"}
