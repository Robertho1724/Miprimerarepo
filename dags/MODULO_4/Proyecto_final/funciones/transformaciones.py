import pandas as pd
import logging
from datetime import datetime

def limpiar_duplicados(ti):
    df = ti.xcom_pull(key='dataframe_seleccion', task_ids='seleccion')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'seleccion'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df = df.drop_duplicates(subset='ID')
    ti.xcom_push(key='dataframe_limpio', value=df)
    logging.info("Limpieza de duplicados completa.")

def ordenar_por_id(ti):
    df = ti.xcom_pull(key='dataframe_limpio', task_ids='TRANSFORMACION.limpiar_duplicados')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'limpiar_duplicados'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df = df.sort_values(by='ID')
    ti.xcom_push(key='dataframe_ordenado', value=df)
    logging.info("Ordenación por ID completa.")

def crear_columna_edad(ti):
    df = ti.xcom_pull(key='dataframe_ordenado', task_ids='TRANSFORMACION.ordenar_por_id')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'ordenar_por_id'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df['Edad'] = datetime.now().year - df['AñoNac']
    ti.xcom_push(key='dataframe_con_edad', value=df)
    logging.info("Creación de columna Edad completa.")

def crear_columna_gasto_total(ti):
    df = ti.xcom_pull(key='dataframe_con_edad', task_ids='TRANSFORMACION.crear_columna_edad')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'crear_columna_edad'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df['GastoTotal'] = df['GastoVinos'] + df['GastoFrutas'] + df['GastoCarnes'] + df['GastoPescados'] + df['GastoDulces'] + df['GastoOro']
    ti.xcom_push(key='dataframe_con_gasto_total', value=df)
    logging.info("Creación de columna GastoTotal completa.")

def crear_columna_gasto_total_productos(ti):
    df = ti.xcom_pull(key='dataframe_con_gasto_total', task_ids='TRANSFORMACION.crear_columna_gasto_total')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'crear_columna_gasto_total'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df['GastoTotalProductos'] = df['GastoVinos'] + df['GastoFrutas'] + df['GastoCarnes'] + df['GastoPescados'] + df['GastoDulces'] + df['GastoOro']
    ti.xcom_push(key='dataframe_con_gasto_total_productos', value=df)
    logging.info("Creación de columna GastoTotalProductos completa.")

def extraer_anio_registro(ti):
    df = ti.xcom_pull(key='dataframe_con_gasto_total_productos', task_ids='TRANSFORMACION.crear_columna_gasto_total_productos')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'crear_columna_gasto_total_productos'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df['FechaRegistro'] = pd.to_datetime(df['FechaRegistro'])
    df['año_registro'] = df['FechaRegistro'].dt.year
    ti.xcom_push(key='dataframe_con_anio_registro', value=df)
    logging.info("Extracción de año de registro completa.")

def extraer_mes_registro(ti):
    df = ti.xcom_pull(key='dataframe_con_anio_registro', task_ids='TRANSFORMACION.extraer_anio_registro')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'extraer_anio_registro'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df['mes_registro'] = df['FechaRegistro'].dt.month
    ti.xcom_push(key='dataframe_con_mes_registro', value=df)
    logging.info("Extracción de mes de registro completa.")

def renombrar_columnas(ti):
    df = ti.xcom_pull(key='dataframe_con_mes_registro', task_ids='TRANSFORMACION.extraer_mes_registro')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'extraer_mes_registro'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df = df.rename(columns={'AceptóCmp1': 'AceptoCampana1', 'AceptóCmp2': 'AceptoCampana2'})
    df = df.drop(columns=['AceptóCmp3', 'AceptóCmp4', 'AceptóCmp5'])
    ti.xcom_push(key='dataframe_renombrado', value=df)
    logging.info("Renombrado de columnas y eliminación completa.")

def filtrar_ingresos(ti):
    df = ti.xcom_pull(key='dataframe_renombrado', task_ids='TRANSFORMACION.renombrar_columnas')
    if df is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'renombrar_columnas'.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    df = df[(df['Ingresos'] > 1500) & (df['Ingresos'] < 5000)]
    ti.xcom_push(key='dataframe_final', value=df)
    logging.info("Filtrado de ingresos completo.")
