"""Limpieza, controles de calidad y modelado dimensional de las fuentes."""

import pandas as pd
import numpy as np
import json

def transform_raw_data(df_sales_raw: pd.DataFrame, df_history_raw: pd.DataFrame = None) -> dict:
    """
    Aplica limpieza, normalización y reglas de negocio sobre los datasets crudos.
    Retorna un diccionario con DataFrames listos para cargar:
    - dim_products
    - dim_customers
    - dim_date
    - fact_sales
    - fact_returns
    - data_quality_rejects
    """
    
    # 1. Asignar nombres exactos de los archivos fuentes
    df_sales_raw = df_sales_raw.copy()
    df_sales_raw['source_file'] = 'Transacciones de ventas.csv'
    
    if df_history_raw is not None and not df_history_raw.empty:
        df_history_raw = df_history_raw.copy()
        df_history_raw['source_file'] = 'Historial extendido de transacciones.csv'
        
        # Homogeneizar posibles diferencias de nombres entre fuentes
        rename_map = {
            'Invoice': 'InvoiceNo',
            'Stock Code': 'StockCode',
            'Price': 'UnitPrice',
            'Customer ID': 'CustomerID'
        }
        df_history_raw.rename(columns=rename_map, inplace=True)
        
        # El orden conserva ventas antes que histórico cuando se eliminan duplicados.
        df_combined = pd.concat([df_sales_raw, df_history_raw], ignore_index=True)
    else:
        df_combined = df_sales_raw

    # Normalizar cabeceras eliminando espacios en blanco
    df_combined.columns = [c.strip() for c in df_combined.columns]

    # Preservar el índice de línea para evitar duplicados en la constraint única
    df_combined['line_index'] = df_combined.index

    # 2. Normalización de campos clave
    df_combined['StockCode'] = df_combined['StockCode'].astype(str).str.strip().str.upper()
    df_combined['InvoiceNo'] = df_combined['InvoiceNo'].astype(str).str.strip()
    df_combined['Country'] = df_combined['Country'].astype(str).str.strip()

    # Normalización de CustomerID (17850.0 -> 17850 y nulos a UNKNOWN_CUSTOMER)
    def clean_customer_id(val):
        if pd.isna(val) or str(val).strip().lower() in ['nan', 'none', '', 'null']:
            return 'UNKNOWN_CUSTOMER'
        try:
            return str(int(float(val)))
        except (ValueError, TypeError):
            return str(val).strip()

    df_combined['CustomerID_clean'] = df_combined['CustomerID'].apply(clean_customer_id)

    # Las fechas sin zona horaria del CSV se interpretan como UTC.
    df_combined['transaction_timestamp_utc'] = pd.to_datetime(
        df_combined['InvoiceDate'], 
        errors='coerce'
    ).dt.tz_localize('UTC')

    # Las filas inválidas se conservan para auditoría y no pasan a las tablas de hechos.
    invalid_date = df_combined['transaction_timestamp_utc'].isna()
    invalid_price = (df_combined['UnitPrice'] <= 0) | (df_combined['UnitPrice'].isna())

    rejects_mask = invalid_date | invalid_price
    df_rejects = df_combined[rejects_mask].copy()
    df_valid = df_combined[~rejects_mask].copy()

    # Log de rechazos
    reject_records = []
    for _, row in df_rejects.iterrows():
        reasons = []
        if pd.isna(row['transaction_timestamp_utc']):
            reasons.append("FECHA_INVALIDA_O_NO_PARSEABLE")
        if pd.isna(row['UnitPrice']) or row['UnitPrice'] <= 0:
            reasons.append("PRECIO_INVALIDO_MENOR_O_IGUAL_A_CERO")
            
        raw_dict = {
            'InvoiceNo': str(row.get('InvoiceNo', '')),
            'StockCode': str(row.get('StockCode', '')),
            'Quantity': str(row.get('Quantity', '')),
            'InvoiceDate': str(row.get('InvoiceDate', '')),
            'UnitPrice': str(row.get('UnitPrice', '')),
            'CustomerID': str(row.get('CustomerID', ''))
        }
        reject_records.append({
            'source_file': row['source_file'],
            'raw_payload': json.dumps(raw_dict),
            'rejection_reason': " | ".join(reasons)
        })

    df_quality_rejects = pd.DataFrame(reject_records)

    # Deduplicar por atributos de negocio, no por el índice de origen.
    dedup_cols = ['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice', 'transaction_timestamp_utc', 'CustomerID_clean']
    df_valid.drop_duplicates(subset=dedup_cols, keep='first', inplace=True)

    # 6. Generación de Dimensiones
    # A. Dimensión Cliente
    unique_customers = df_valid[['CustomerID_clean']].drop_duplicates().copy()
    unique_customers['customer_type'] = np.where(
        unique_customers['CustomerID_clean'] == 'UNKNOWN_CUSTOMER',
        'GUEST/ANONYMOUS',
        'IDENTIFIED'
    )
    dim_customers = unique_customers.rename(columns={'CustomerID_clean': 'customer_id'})

    # B. Dimensión Producto: usa la descripción válida más frecuente por código.
    def get_canonical_description(series):
        valid_desc = series.dropna().astype(str).str.strip()
        valid_desc = valid_desc[valid_desc != '']
        if not valid_desc.empty:
            return valid_desc.mode()[0].title()
        return "Sin Descripción"

    desc_map = df_valid.groupby('StockCode')['Description'].apply(get_canonical_description).reset_index()
    desc_map.rename(columns={'StockCode': 'product_code', 'Description': 'canonical_description'}, inplace=True)
    desc_map['category'] = 'General'
    desc_map['country_origin'] = 'Unknown'
    desc_map['is_active'] = True
    dim_products = desc_map

    # C. Dimensión Fecha
    dates = pd.to_datetime(df_valid['transaction_timestamp_utc'].dt.date.unique())
    dim_date = pd.DataFrame({
        'full_date': dates,
        'date_key': dates.strftime('%Y%m%d').astype(int),
        'year': dates.year,
        'month': dates.month,
        'month_name': dates.strftime('%B'),
        'day': dates.day
    })

    df_valid['date_key'] = df_valid['transaction_timestamp_utc'].dt.strftime('%Y%m%d').astype(int)

    # Las unidades positivas son ventas; cero y negativas quedan clasificadas como devoluciones.
    sales_mask = df_valid['Quantity'] > 0
    
    # Fact Sales
    df_sales = df_valid[sales_mask].copy()
    df_sales['gross_revenue'] = (df_sales['Quantity'] * df_sales['UnitPrice']).round(4)
    fact_sales = df_sales[[
        'InvoiceNo', 'line_index', 'StockCode', 'CustomerID_clean', 'date_key',
        'Country', 'Quantity', 'UnitPrice', 'gross_revenue',
        'transaction_timestamp_utc', 'source_file'
    ]].rename(columns={
        'InvoiceNo': 'invoice_no',
        'StockCode': 'product_code',
        'CustomerID_clean': 'customer_id',
        'Country': 'country',
        'Quantity': 'quantity',
        'UnitPrice': 'unit_price'
    })

    # Fact Returns
    df_returns = df_valid[~sales_mask].copy()
    df_returns['quantity_returned'] = df_returns['Quantity'].abs()
    df_returns['refund_amount'] = (df_returns['quantity_returned'] * df_returns['UnitPrice']).round(4)
    fact_returns = df_returns[[
        'InvoiceNo', 'line_index', 'StockCode', 'CustomerID_clean', 'date_key',
        'Country', 'quantity_returned', 'UnitPrice', 'refund_amount',
        'transaction_timestamp_utc', 'source_file'
    ]].rename(columns={
        'InvoiceNo': 'invoice_no',
        'StockCode': 'product_code',
        'CustomerID_clean': 'customer_id',
        'Country': 'country',
        'UnitPrice': 'unit_price'
    })

    return {
        'dim_customers': dim_customers,
        'dim_products': dim_products,
        'dim_date': dim_date,
        'fact_sales': fact_sales,
        'fact_returns': fact_returns,
        'data_quality_rejects': df_quality_rejects
    }