"""Conexión compartida por las tareas ETL hacia el Data Warehouse."""

import os
from sqlalchemy import create_engine

# Import SDK estándar para evitar advertencias de deprecación
try:
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook

def get_dw_engine():
    """
    Obtiene la conexión hacia el Data Warehouse y asegura el prefijo
    compatible 'postgresql+psycopg2://' para SQLAlchemy.
    """
    # Airflow Connection es la fuente principal; la variable de entorno sirve como fallback.
    try:
        conn = BaseHook.get_connection("postgres_dw")
        db_url = conn.get_uri()
    except Exception:
        db_url = os.getenv(
            "AIRFLOW_CONN_POSTGRES_DW",
            "postgresql://dw_admin:dw_secure_pass@postgres-dw:5432/datamart_dw"
        )
    
    # Normalizar esquemas viejos como 'postgres://' a 'postgresql+psycopg2://'
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://") and "+psycopg2" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        
    return create_engine(db_url)