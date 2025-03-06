import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging

# Importando funciones generales
from MODULO_4.Proyecto_final.general.general_funciones import get_config, get_md
# Importando funciones específicas
from MODULO_4.Proyecto_final.funciones.cargar_datos_mysql import cargar_datos_mysql
from MODULO_4.Proyecto_final.funciones.filtrar_columnas import filtrar_columnas
from MODULO_4.Proyecto_final.funciones.seleccion_columnas import seleccion_columnas
from MODULO_4.Proyecto_final.funciones.leer_archivos import leer_archivo_principal
# Importando funciones de transformación
from MODULO_4.Proyecto_final.funciones.transformaciones import (
    limpiar_duplicados, ordenar_por_id, crear_columna_edad,
    crear_columna_gasto_total, crear_columna_gasto_total_productos,
    extraer_anio_registro, extraer_mes_registro, renombrar_columnas,
    filtrar_ingresos
)

# Configuraciones
config_cargar = get_config('config_cargar.yaml')
config_extraer = get_config('config_extraer.yaml')
config_filtrar = get_config('config_filtrar.yaml')
config_final = get_config('config_final.yaml')
config_inicio = get_config('config_inicio.yaml')
config_seleccion = get_config('config_seleccion.yaml')
general_config = get_config('general_config.yaml')

# Características de ejecución del DAG
default_args = general_config['default_args']['dev']
dag_caracteristicas = general_config['dag_caracteristicas']['dev']
doc_md = get_md('README.md')

# Definición del DAG
with DAG(
    dag_id='pf_dagcompleto',
    default_args=default_args,
    tags=["MODULO_4"],
    schedule_interval=dag_caracteristicas['schedule_interval'],
    start_date=datetime.strptime(dag_caracteristicas['start_date'], '%Y-%m-%d'),
    max_active_runs=dag_caracteristicas['max_active_runs'],
    catchup=dag_caracteristicas['catchup'],
    max_active_tasks=dag_caracteristicas['max_active_tasks'],
    description=dag_caracteristicas['description'],
    doc_md=doc_md
) as dag:

    # Paso 1: Sensor de archivo para activar el DAG
    INICIO = FileSensor(
        task_id='INICIO',
        fs_conn_id='my_filesystem',
        filepath=config_inicio['filepath'],
        poke_interval=config_inicio['poke_interval'],
        timeout=config_inicio['timeout']
    )

    # Paso 2: Extraer datos del archivo CSV principal
    EXTRAER = PythonOperator(
        task_id='EXTRAER',
        python_callable=leer_archivo_principal,
        provide_context=True,
        op_kwargs={'ruta_directorio': config_extraer['op_kwargs']['ruta_directorio']}
    )

    # Paso 3: Selección de columnas
    SELECCION = PythonOperator(
        task_id='seleccion',
        python_callable=seleccion_columnas,
        provide_context=True,
        op_kwargs=config_seleccion['op_kwargs']
    )

    # Paso 4: Transformación de datos (TaskGroup para múltiples transformaciones)
    with TaskGroup(group_id='TRANSFORMACION') as TRANSFORMACION:

        # Limpieza de duplicados
        limpiar = PythonOperator(
            task_id='limpiar_duplicados',
            python_callable=limpiar_duplicados,
            provide_context=True
        )

        # Ordenar por ID
        ordenar = PythonOperator(
            task_id='ordenar_por_id',
            python_callable=ordenar_por_id,
            provide_context=True
        )

        # Crear columna Edad
        edad = PythonOperator(
            task_id='crear_columna_edad',
            python_callable=crear_columna_edad,
            provide_context=True
        )

        # Crear columna GastoTotal
        gasto_total = PythonOperator(
            task_id='crear_columna_gasto_total',
            python_callable=crear_columna_gasto_total,
            provide_context=True
        )

        # Crear columna GastoTotalProductos
        gasto_total_productos = PythonOperator(
            task_id='crear_columna_gasto_total_productos',
            python_callable=crear_columna_gasto_total_productos,
            provide_context=True
        )

        # Extraer año de registro
        anio_registro = PythonOperator(
            task_id='extraer_anio_registro',
            python_callable=extraer_anio_registro,
            provide_context=True
        )

        # Extraer mes de registro
        mes_registro = PythonOperator(
            task_id='extraer_mes_registro',
            python_callable=extraer_mes_registro,
            provide_context=True
        )

        # Renombrar columnas y eliminar columnas innecesarias
        renombrar = PythonOperator(
            task_id='renombrar_columnas',
            python_callable=renombrar_columnas,
            provide_context=True
        )

        # Filtrar ingresos
        filtrar_ing = PythonOperator(
            task_id='filtrar_ingresos',
            python_callable=filtrar_ingresos,
            provide_context=True
        )

        # Definir el flujo de dependencias dentro del TaskGroup
        limpiar >> ordenar >> edad >> gasto_total >> gasto_total_productos >> anio_registro >> mes_registro >> renombrar >> filtrar_ing

    # Paso 5: Cargar datos transformados en MySQL
    CARGAR = PythonOperator(
        task_id='CARGAR',
        python_callable=cargar_datos_mysql,
        provide_context=True,
        op_kwargs=config_cargar['op_kwargs']
    )

    # Paso 6: Confirmación final con BashOperator
    FINAL = BashOperator(
        task_id='FINAL',
        bash_command=config_final['bash_command'],
        trigger_rule=config_final['trigger_rule']
    )

    # Definir el flujo de tareas
    INICIO >> EXTRAER >> SELECCION >> TRANSFORMACION >> CARGAR >> FINAL
