"""Carga transaccional por lotes del modelo dimensional en PostgreSQL."""

import pandas as pd
from sqlalchemy import text
from src.utils.db import get_dw_engine

def chunked_iterable(iterable, size=5000):
    """Generador para procesar listas en bloques."""
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def load_dimensions(data_dict: dict):
    """Carga dimensiones (dim_customers, dim_products, dim_date) de forma idempotente y por lotes."""
    engine = get_dw_engine()
    
    # Una sola transacción evita dejar dimensiones parcialmente cargadas si falla un lote.
    with engine.begin() as conn:
        # 1. Dimensión Clientes
        df_cust = data_dict['dim_customers'].drop_duplicates(subset=['customer_id'])
        cust_records = df_cust[['customer_id', 'customer_type']].to_dict(orient='records')
        
        insert_cust_query = text("""
            INSERT INTO dwh.dim_customers (customer_id, customer_type)
            VALUES (:customer_id, :customer_type)
            ON CONFLICT (customer_id) DO NOTHING;
        """)
        for chunk in chunked_iterable(cust_records, size=5000):
            conn.execute(insert_cust_query, chunk)

        # 2. Dimensión Productos
        df_prod = data_dict['dim_products'].drop_duplicates(subset=['product_code'])
        prod_records = df_prod[[
            'product_code', 'canonical_description', 'category', 'country_origin', 'is_active'
        ]].to_dict(orient='records')
        
        insert_prod_query = text("""
            INSERT INTO dwh.dim_products (product_code, canonical_description, category, country_origin, is_active)
            VALUES (:product_code, :canonical_description, :category, :country_origin, :is_active)
            ON CONFLICT (product_code) DO UPDATE
            SET canonical_description = EXCLUDED.canonical_description;
        """)
        for chunk in chunked_iterable(prod_records, size=5000):
            conn.execute(insert_prod_query, chunk)

        # 3. Dimensión Fecha
        df_date = data_dict['dim_date'].drop_duplicates(subset=['date_key'])
        date_records = df_date[['date_key', 'full_date', 'year', 'month', 'month_name', 'day']].to_dict(orient='records')
        
        insert_date_query = text("""
            INSERT INTO dwh.dim_date (date_key, full_date, year, month, month_name, day)
            VALUES (:date_key, :full_date, :year, :month, :month_name, :day)
            ON CONFLICT (date_key) DO NOTHING;
        """)
        for chunk in chunked_iterable(date_records, size=5000):
            conn.execute(insert_date_query, chunk)

def load_facts_and_rejects(data_dict: dict):
    """Mapea claves foráneas e inserta los hechos y rechazos en bloques masivos."""
    engine = get_dw_engine()
    
    with engine.begin() as conn:
        # Traducir las claves naturales del Parquet a las claves sustitutas del DWH.
        prod_map = dict(conn.execute(text("SELECT product_code, product_key FROM dwh.dim_products;")).fetchall())
        cust_map = dict(conn.execute(text("SELECT customer_id, customer_key FROM dwh.dim_customers;")).fetchall())

        # 1. Fact Sales (Carga por lotes)
        df_sales = data_dict['fact_sales'].copy()
        df_sales['product_key'] = df_sales['product_code'].map(prod_map)
        df_sales['customer_key'] = df_sales['customer_id'].map(cust_map)
        
        df_sales = df_sales.dropna(subset=['product_key', 'customer_key'])
        
        sales_records = df_sales[[
            'invoice_no', 'line_index', 'product_key', 'customer_key', 'date_key',
            'country', 'quantity', 'unit_price', 'gross_revenue',
            'transaction_timestamp_utc', 'source_file'
        ]].to_dict(orient='records')

        insert_sales_query = text("""
            INSERT INTO dwh.fact_sales (
                invoice_no, line_index, product_key, customer_key, date_key,
                country, quantity, unit_price, gross_revenue,
                transaction_timestamp_utc, source_file
            ) VALUES (
                :invoice_no, :line_index, :product_key, :customer_key, :date_key,
                :country, :quantity, :unit_price, :gross_revenue,
                :transaction_timestamp_utc, :source_file
            )
            ON CONFLICT (invoice_no, product_key, transaction_timestamp_utc, line_index) DO NOTHING;
        """)

        for chunk in chunked_iterable(sales_records, size=5000):
            conn.execute(insert_sales_query, chunk)

        # 2. Fact Returns (Carga por lotes)
        df_returns = data_dict['fact_returns'].copy()
        df_returns['product_key'] = df_returns['product_code'].map(prod_map)
        df_returns['customer_key'] = df_returns['customer_id'].map(cust_map)
        df_returns = df_returns.dropna(subset=['product_key', 'customer_key'])

        returns_records = df_returns[[
            'invoice_no', 'line_index', 'product_key', 'customer_key', 'date_key',
            'country', 'quantity_returned', 'unit_price', 'refund_amount',
            'transaction_timestamp_utc', 'source_file'
        ]].to_dict(orient='records')

        insert_returns_query = text("""
            INSERT INTO dwh.fact_returns (
                invoice_no, line_index, product_key, customer_key, date_key,
                country, quantity_returned, unit_price, refund_amount,
                transaction_timestamp_utc, source_file
            ) VALUES (
                :invoice_no, :line_index, :product_key, :customer_key, :date_key,
                :country, :quantity_returned, :unit_price, :refund_amount,
                :transaction_timestamp_utc, :source_file
            )
            ON CONFLICT (invoice_no, product_key, transaction_timestamp_utc, line_index) DO NOTHING;
        """)

        for chunk in chunked_iterable(returns_records, size=5000):
            conn.execute(insert_returns_query, chunk)

    # Los rechazos se registran para auditoría; esta tabla no deduplica reejecuciones.
        df_rejects = data_dict['data_quality_rejects']
        if not df_rejects.empty:
            rejects_records = df_rejects[['source_file', 'raw_payload', 'rejection_reason']].to_dict(orient='records')
            
            # Usar CAST() evita que SQLAlchemy interprete ::jsonb como parámetro con dos puntos
            insert_rejects_query = text("""
                INSERT INTO dwh.data_quality_rejects (source_file, raw_payload, rejection_reason)
                VALUES (:source_file, CAST(:raw_payload AS jsonb), :rejection_reason);
            """)
            
            for chunk in chunked_iterable(rejects_records, size=5000):
                conn.execute(insert_rejects_query, chunk)