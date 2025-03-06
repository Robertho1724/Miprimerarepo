from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

# Definir los argumentos predeterminados del DAG
default_args = {
    'owner': 'Docente',
    'start_date': datetime(2024, 10, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Definir el DAG
with DAG(
    dag_id='sensor_archivo',
    schedule_interval='@daily',
    tags=["MODULO_3"],
    default_args=default_args,
    catchup=False
    
    ) as dag:
    
    # Tarea inicio
    start_task = DummyOperator(task_id='start_task')

    # Sensor de archivo
    file_sensor_tarea = FileSensor(
        task_id='file_sensor_tarea',
        fs_conn_id='my_filesystem',
        filepath='/opt/airflow/dags/MODULO_3/archivo_sensor/Modelo_base_consolidado.xlsx',
        poke_interval=20,  # Tiempo en segundos para la verificación
        timeout=300  # Tiempo máximo de espera en segundos
    )

    # Tarea fin
    end_task = DummyOperator(task_id='end_task')

    # Definir el orden de las tareas
    start_task >> file_sensor_tarea >> end_task
