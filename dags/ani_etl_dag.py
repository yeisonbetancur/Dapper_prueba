import sys
sys.path.insert(0, '/opt/airflow')

from airflow import DAG
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.decorators import task, dag
from datetime import datetime, timedelta
import pandas as pd
from src.extraccion import run_extraction, ENTITY_VALUE
from src.escritura import run_write
from src.validacion import run_validation

#configuracion por defecto del dag
default_args = {
    'owner': 'airflow',
    'retries': 0,

}

@dag(
    'ani_etl_dag',
    default_args=default_args,
    description='Pipeline ETL con 3 pasos para ani: Extracción -> Transformación -> Carga',
    start_date=days_ago(1),
    schedule_interval=None, 
    catchup=False,
    params={
        'num_pages_to_scrape': Param(3, type='integer', minimum=1, maximum=25),
        'verbose': Param(False, type='boolean'),
    },
) 
def ani_pipeline():


    @task
    def extraer_normas(**context):
        params=context["params"]
        num_pages_to_scrape=params["num_pages_to_scrape"]
        verbose=params["verbose"]
        print(" Extraccion ")
        print(f"Parámetros: num_pages_to_scrape={num_pages_to_scrape}, verbose={verbose}")
        num_pages=int(num_pages_to_scrape)
        df=run_extraction(num_pages,verbose)
        print(f"Total de registros obtenidos: {len(df)}") 
        return  df.to_dict(orient="records")

    @task
    def validar_datos(records):
        print(f"\nVALIDACIÓN ")
        df = pd.DataFrame(records)
        df_valid, reporte = run_validation(df, ENTITY_VALUE)
    
        # Mostrar el reporte de validación
        print(f"✓ Filas válidas: {reporte['total_valid_rows']}")
        print(f"✗ Filas descartadas: {reporte['total_dropped_rows']}")
        if reporte['invalid_by_field']:
            print(f"Campos inválidos: {reporte['invalid_by_field']}")
        return df_valid


    @task 
    def escribir_datos(records):
        print("\n ESCRITURA ")
        df = pd.DataFrame(records)
        if df.empty:
            inserted, message = (0, 'No hay normas para escribir')
        else:
            inserted, message = run_write(df,ENTITY_VALUE)
        print(f"normas insertadas: {inserted}")
        print(f"Mensaje: {message}")
        print("=== FIN  ===\n")
        return {
            'inserted': inserted,
            'message': message,
        }


    datos = extraer_normas()
    validados = validar_datos(datos)
    escribir_datos(validados)

dag_instance = ani_pipeline()
