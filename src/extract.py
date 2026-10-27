"""Lectura de los archivos fuente de ventas desde el almacenamiento de Airflow."""

import os
import pandas as pd
from airflow.models import Variable

def get_raw_data_path() -> str:
    """Resuelve la ruta de entrada desde una Variable de Airflow o su valor por defecto."""
    return Variable.get("raw_data_path", default_var="/opt/airflow/data/raw")

def extract_sales_data() -> pd.DataFrame:
    """Lee el archivo diario obligatorio de transacciones de ventas."""
    base_path = get_raw_data_path()
    file_path = os.path.join(base_path, "Transacciones de ventas.csv")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
        
    return pd.read_csv(file_path, encoding="latin1", low_memory=False)

def extract_history_data() -> pd.DataFrame:
    """Lee el histórico extendido, que el DAG trata como una fuente opcional."""
    base_path = get_raw_data_path()
    file_path = os.path.join(base_path, "Historial extendido de transacciones.csv")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
        
    return pd.read_csv(file_path, encoding="latin1", low_memory=False)