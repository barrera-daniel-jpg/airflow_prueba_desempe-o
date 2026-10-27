"""Motor y sesiones SQLAlchemy de la API para consultar PostgreSQL del DWH."""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

DW_USER = os.getenv("DW_USER", "dw_admin")
DW_PASSWORD = os.getenv("DW_PASSWORD", "dw1234")
DW_HOST = os.getenv("DW_HOST", "postgres-dw")
DW_PORT = os.getenv("DW_PORT", "5432")
DW_NAME = os.getenv("DW_NAME", "datamart_dw")

SQLALCHEMY_DATABASE_URL = (
    f"postgresql+psycopg2://{DW_USER}:{DW_PASSWORD}@{DW_HOST}:{DW_PORT}/{DW_NAME}"
)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Proporciona una sesión por petición y la cierra incluso si ocurre un error."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()