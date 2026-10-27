"""Aplicación FastAPI y puntos de entrada generales del servicio de catálogo."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import products

app = FastAPI(
    title="Product Catalog API",
    description="API REST para consumo y consulta del catálogo normalizado de productos en el Data Warehouse.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Integración del router de productos
app.include_router(products.router)


@app.get("/health", tags=["Salud del Servicio"])
def health_check():
    return {"status": "healthy", "service": "catalog-api"}