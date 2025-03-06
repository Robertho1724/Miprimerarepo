from sqlalchemy import create_engine
import logging

def cargar_datos_mysql(ti, **kwargs):
    archivo_subido = ti.xcom_pull(key='final', task_ids='TRANSFORMACION.filtrar')
    if archivo_subido is None:
        logging.error("Error: No se pudo obtener el DataFrame filtrado.")
        raise ValueError("El DataFrame no se pudo obtener. Verifica la tarea anterior.")
    
    mysql_conn_string = kwargs.get('mysql_conn_string', "mysql+pymysql://root:dwh@mysql_dwh:3306/dwh")
    tabla_destino = kwargs.get('tabla_destino', 'PF_base_marketing')
    
    engine = create_engine(mysql_conn_string)
    
    # Obtener las columnas de la tabla MySQL
    with engine.connect() as connection:
        resultado = connection.execute(f"SHOW COLUMNS FROM {tabla_destino}")
        columnas_mysql = [fila['Field'] for fila in resultado]
    
    # Filtrar el DataFrame para incluir solo las columnas que existen en la tabla MySQL
    columnas_comunes = [col for col in archivo_subido.columns if col in columnas_mysql]
    archivo_subido = archivo_subido[columnas_comunes]
    
    try:
        with engine.begin() as connection:
            archivo_subido.to_sql(name=tabla_destino, con=connection, if_exists='append', index=False)
        logging.info(f"Datos cargados en MySQL en la tabla {tabla_destino}")
    except Exception as e:
        logging.error(f"Error al cargar datos en MySQL: {e}")
        raise
