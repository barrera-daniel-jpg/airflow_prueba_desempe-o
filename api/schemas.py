"""Contratos Pydantic para las respuestas públicas del catálogo."""

from typing import List, Optional
from pydantic import BaseModel, Field


class ProductResponse(BaseModel):
    """Modelo canónico de producto expuesto por la API."""
    code: str = Field(..., description="Identificador único del producto")
    name: str = Field(..., description="Nombre normalizado del producto")
    category: str = Field(..., description="Categoría (Electrónica, Hogar, Ropa, Deportes, Papelería)")
    country_origin: str = Field(..., description="País de origen del proveedor")
    status: str = Field(..., description="Estado del producto (activo o descontinuado)")

    class Config:
        from_attributes = True


class PaginatedProductsResponse(BaseModel):
    """Envoltura estándar para respuestas con paginación."""
    total: int = Field(..., description="Total de productos registrados en el catálogo")
    page: int = Field(..., description="Página actual consultada")
    limit: int = Field(..., description="Cantidad de productos por página")
    total_pages: int = Field(..., description="Total de páginas disponibles")
    data: List[ProductResponse] = Field(..., description="Listado de productos")