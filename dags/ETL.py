from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from src.extract import extract  
from src.load import load  
from src.transform import run_queries 
from src import config

# Configuración de parámetros iniciales
CSV_FOLDER = "dataset"  
CSV_TABLE_MAPPING = {}  
PUBLIC_HOLIDAYS_URL = config.PUBLIC_HOLIDAYS_URL
OUTPUT_FOLDER = "dags/output"

# Funciones para las etapas del ETL
def extract_task():
    """Ejecuta la extracción de datos usando la función extract de extract.py."""
    try:
        dataframes = extract(
            csv_folder=CSV_FOLDER,
            csv_table_mapping=CSV_TABLE_MAPPING,
            public_holidays_url=PUBLIC_HOLIDAYS_URL
        )
        for table_name, df in dataframes.items():
            print(f"Tabla extraída {table_name}: {df.shape}")
        return dataframes
    except Exception as e:
        print(f"Error en la extracción: {e}")
        raise

def load_task(ti):
    """Carga los DataFrames extraídos en la base de datos usando load de load.py."""
    dataframes = ti.xcom_pull(task_ids='extract_data')
    engine = create_engine(rf"sqlite:///{config.SQLITE_BD_ABSOLUTE_PATH}", echo=False)
    
    try:
        load(dataframes, engine)
        print("Carga completada con éxito.")
    except Exception as e:
        print(f"Error en la carga: {e}")
        raise

def transform_task(ti):
    """Transforma los datos cargados usando run_queries de transform.py."""
    engine = create_engine(rf"sqlite:///{config.SQLITE_BD_ABSOLUTE_PATH}", echo=False)
    
    try:
        transformed_dataframes = run_queries(engine)
        for query_name, df in transformed_dataframes.items():
            print(f"Resultado de la consulta {query_name}: {df.shape}")
        return transformed_dataframes
    except Exception as e:
        print(f"Error en la transformación: {e}")
        raise

def save_transformed_task(ti):
    """Guarda los DataFrames transformados como archivos CSV."""
    transformed_dataframes = ti.xcom_pull(task_ids='transform_data')
    
    try:
        for query_name, df in transformed_dataframes.items():
            output_path = f"{OUTPUT_FOLDER}/{query_name}.csv"
            df.to_csv(output_path, index=False)
            print(f"Guardado {query_name} en {output_path}")
    except Exception as e:
        print(f"Error al guardar los resultados: {e}")
        raise

# Configuración del DAG
default_args = {
    'owner': 'jeisson_araque',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'etl_proyecto_integrado_iv',
    default_args=default_args,
    description='DAG para automatizar el flujo ETL de proyecto-integrado-IV',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 3),
    catchup=False,
) as dag:

    # Tareas del ETL
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_task,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task,
    )

    save_transformed_data = PythonOperator(
        task_id='save_transformed_data',
        python_callable=save_transformed_task,
    )

    # Definir el flujo: extract -> load -> transform -> save
    extract_data >> load_data >> transform_data >> save_transformed_data