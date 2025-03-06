import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import logging

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'Docente',
    'depends_on_past': True,
    'start_date': datetime(2023,7,31),
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


# Función para extracción de datos de MySQL (con DISTINCT)
def extraer_datos_mysql(ti):
    engine = create_engine("mysql+pymysql://root:dwh@mysql_dwh:3306/dwh")
    connection = engine.connect()
    
    with engine.begin() as connection:
        # Agregar DISTINCT para evitar duplicados en la consulta SQL
        base = pd.read_sql_query(f"SELECT DISTINCT * FROM jugadores_futbol_ejemplo1", con=connection)
        ti.xcom_push(key='dataframe_base', value=base)


# Función para selección de columnas (modificada)
def seleccionar_columnas(ti):
    base = ti.xcom_pull(key='dataframe_base', task_ids='EXTRAER')
    # Columnas disponibles en la tabla
    columnas_necesarias = ['id_jugador', 'nombre', 'posicion', 'edad']
    
    # Verificar si todas las columnas están presentes en el DataFrame
    if all(col in base.columns for col in columnas_necesarias):
        seleccion = base[columnas_necesarias]
        ti.xcom_push(key='dataframe_seleccion', value=seleccion)
    else:
        # Registrar un mensaje de error si faltan columnas
        logging.error(f"Las columnas {columnas_necesarias} no se encuentran en el DataFrame.")
        raise KeyError(f"Las columnas {columnas_necesarias} no se encuentran en el DataFrame.")


# Función para limpiar duplicados y depurar
def limpiar_duplicados(ti):
    seleccion = ti.xcom_pull(key='dataframe_seleccion', task_ids='TRANSFORMACION.seleccionar_columnas')
    # Verificar qué filas están duplicadas
    duplicados = seleccion[seleccion.duplicated(keep=False)]  # Mostrar todas las filas duplicadas
    logging.info(f"Filas duplicadas encontradas: {duplicados}")
    
    # Eliminar duplicados
    limpiado = seleccion.drop_duplicates(keep='first')
    ti.xcom_push(key='dataframe_limpiado', value=limpiado)




# Función para ordenar y filtrar
def ordenar_filtrar(ti):
    limpiado = ti.xcom_pull(key='dataframe_limpiado', task_ids='TRANSFORMACION.limpiar_duplicados')
    filtrado = limpiado[(limpiado['edad'] > 25) & (limpiado['posicion'] == 'Attacker')].sort_values(by='edad', ascending=False)
    ti.xcom_push(key='dataframe_filtrado', value=filtrado)

# Función para crear columnas 'Joven' y 'Veterano'
def crear_columnas_extra(ti):
    filtrado = ti.xcom_pull(key='dataframe_filtrado', task_ids='TRANSFORMACION.ordenar_filtrar')
    filtrado['Joven'] = filtrado['edad'].apply(lambda x: 1 if 25 < x <= 30 else 0)
    filtrado['Veterano'] = filtrado['edad'].apply(lambda x: 1 if x > 30 else 0)
    ti.xcom_push(key='dataframe_final', value=filtrado)

# Función para cargar datos en MySQL
def cargar_datos_mysql(ti):
    final = ti.xcom_pull(key='dataframe_final', task_ids='TRANSFORMACION.crear_columnas_extra')
    
    engine = create_engine("mysql+pymysql://root:dwh@mysql_dwh:3306/dwh")
    with engine.begin() as connection:
        final.to_sql(name='LABORATORIO2', con=connection, if_exists='append', index=False)

# Definir el DAG principal
with DAG(dag_id='activador',
         schedule='@daily',
         catchup=False,
         tags=["MODULO_3"],
         default_args=default_args
         ) as dag:

    # Sensor para detectar el archivo activador
    INICIO = FileSensor(
        task_id='INICIO',
        fs_conn_id='my_filesystem',
        filepath='/opt/airflow/dags/MODULO_3/activador.py',
        poke_interval=20,
        timeout=300
    )

    # Extracción de datos de MySQL
    EXTRAER = PythonOperator(
        task_id='EXTRAER',
        python_callable=extraer_datos_mysql
    )

    # Grupo de transformaciones
    with TaskGroup(group_id='TRANSFORMACION') as TRANSFORMACION:

        seleccionar_columnas = PythonOperator(
            task_id='seleccionar_columnas',
            python_callable=seleccionar_columnas
        )

        limpiar_duplicados = PythonOperator(
            task_id='limpiar_duplicados',
            python_callable=limpiar_duplicados
        )

        ordenar_filtrar = PythonOperator(
            task_id='ordenar_filtrar',
            python_callable=ordenar_filtrar
        )

        crear_columnas_extra = PythonOperator(
            task_id='crear_columnas_extra',
            python_callable=crear_columnas_extra
        )

        seleccionar_columnas >> limpiar_duplicados >> ordenar_filtrar >> crear_columnas_extra

    # Cargar datos en MySQL
    CARGAR = PythonOperator(
        task_id='CARGAR',
        python_callable=cargar_datos_mysql
    )

    # Finalización con BashOperator
    FINAL = BashOperator(
        task_id='FINAL',
        bash_command="echo 'Transformaciones y carga completadas con éxito'",
        trigger_rule="all_success"
    )

    # Definir el flujo de tareas
    INICIO >> EXTRAER >> TRANSFORMACION >> CARGAR >> FINAL
