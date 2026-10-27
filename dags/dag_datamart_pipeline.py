"""Orquestación diaria del pipeline ETL del Data Warehouse de ventas."""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Asegurar que el directorio /opt/airflow esté en el PYTHONPATH del contenedor
sys.path.append("/opt/airflow")

from src.extract import extract_sales_data, extract_history_data
from src.transform import transform_raw_data
from src.load import load_dimensions, load_facts_and_rejects

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def task_extract_and_transform(**context):
    """Extrae las dos fuentes de ventas e historia y aplica las transformaciones de negocio."""
    df_sales = extract_sales_data()
    
    # Manejo tolerante por si el archivo histórico está ausente o vacío
    try:
        df_history = extract_history_data()
    except Exception:
        df_history = None

    transformed_dict = transform_raw_data(df_sales, df_history)
    
    # El volumen compartido permite que las tareas independientes intercambien resultados.
    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    for table_name, df in transformed_dict.items():
        output_path = f"/opt/airflow/data/processed/{table_name}.parquet"
        df.to_parquet(output_path, index=False)

def task_load_dimensions(**context):
    """Carga dim_customers, dim_products y dim_date desde los datos intermedios."""
    import pandas as pd
    data_dict = {
        'dim_customers': pd.read_parquet("/opt/airflow/data/processed/dim_customers.parquet"),
        'dim_products': pd.read_parquet("/opt/airflow/data/processed/dim_products.parquet"),
        'dim_date': pd.read_parquet("/opt/airflow/data/processed/dim_date.parquet")
    }
    load_dimensions(data_dict)

def task_load_facts_and_rejects(**context):
    """Carga fact_sales, fact_returns y data_quality_rejects garantizando foreign keys e idempotencia."""
    import pandas as pd
    data_dict = {
        'fact_sales': pd.read_parquet("/opt/airflow/data/processed/fact_sales.parquet"),
        'fact_returns': pd.read_parquet("/opt/airflow/data/processed/fact_returns.parquet"),
        'data_quality_rejects': pd.read_parquet("/opt/airflow/data/processed/data_quality_rejects.parquet")
    }
    load_facts_and_rejects(data_dict)

with DAG(
    dag_id='datamart_etl_daily_pipeline',
    default_args=default_args,
    description='Pipeline ETL de ingesta diaria y analitica para DataMart S.A.S.',
    schedule='@daily',       
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['datamart', 'etl', 'sales']
) as dag:

    t_extract_transform = PythonOperator(
        task_id='extract_and_transform_sources',
        python_callable=task_extract_and_transform
    )

    t_load_dims = PythonOperator(
        task_id='load_dimensions_dw',
        python_callable=task_load_dimensions
    )

    t_load_facts = PythonOperator(
        task_id='load_facts_and_rejects_dw',
        python_callable=task_load_facts_and_rejects
    )

    # Orden explícito de dependencias
    t_extract_transform >> t_load_dims >> t_load_facts