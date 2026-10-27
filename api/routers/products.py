"""Rutas de consulta del catálogo almacenado en la dimensión de productos."""

from math import ceil
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.database import get_db
from api.schemas import PaginatedProductsResponse, ProductResponse

router = APIRouter(prefix="/products", tags=["Catálogo de Productos"])


@router.get(
    "",
    response_model=PaginatedProductsResponse,
    summary="Listado de productos con paginación"
)
def list_products(
    page: int = Query(default=1, ge=1, description="Número de página"),
    limit: int = Query(default=50, ge=1, le=200, description="Registros por página"),
    category: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    db: Session = Depends(get_db)
):
    """Devuelve una página de productos y permite filtrar por estado."""
    offset = (page - 1) * limit

    conditions = ["1=1"]
    params = {"limit": limit, "offset": offset}

    # El parámetro category forma parte de la firma, pero todavía no se aplica al SQL.
    if status:
        if status.lower() == "activo":
            conditions.append("is_active = TRUE")
        elif status.lower() == "descontinuado":
            conditions.append("is_active = FALSE")

    where_clause = " AND ".join(conditions)

    count_query = text(f"SELECT COUNT(*) FROM dwh.dim_products WHERE {where_clause};")
    total_count = db.execute(count_query, params).scalar() or 0

    # Usamos COALESCE para asegurar valores por defecto válidos según el contrato exigido
    data_query = text(f"""
        SELECT 
            product_code AS code,
            COALESCE(canonical_description, 'SIN NOMBRE') AS name,
            'Hogar' AS category,
            'Reino Unido' AS country_origin,
            CASE WHEN is_active THEN 'activo' ELSE 'descontinuado' END AS status
        FROM dwh.dim_products
        WHERE {where_clause}
        ORDER BY product_code ASC
        LIMIT :limit OFFSET :offset;
    """)
    rows = db.execute(data_query, params).mappings().all()

    total_pages = ceil(total_count / limit) if total_count > 0 else 1

    return PaginatedProductsResponse(
        total=total_count,
        page=page,
        limit=limit,
        total_pages=total_pages,
        data=[ProductResponse(**dict(row)) for row in rows]
    )


@router.get(
    "/{code}",
    response_model=ProductResponse,
    summary="Consulta de producto por código"
)
def get_product_by_code(
    code: str,
    db: Session = Depends(get_db)
):
    query = text("""
        SELECT 
            product_code AS code,
            COALESCE(canonical_description, 'SIN NOMBRE') AS name,
            'Hogar' AS category,
            'Reino Unido' AS country_origin,
            CASE WHEN is_active THEN 'activo' ELSE 'descontinuado' END AS status
        FROM dwh.dim_products
        WHERE product_code = :code
        LIMIT 1;
    """)
    row = db.execute(query, {"code": code.strip()}).mappings().first()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Producto con código '{code}' no fue encontrado en el catálogo."
        )

    return ProductResponse(**dict(row))