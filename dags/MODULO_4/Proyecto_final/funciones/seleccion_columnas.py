import logging

def seleccion_columnas(ti, columnas):
    dataframe = ti.xcom_pull(key='dataframe_principal', task_ids='EXTRAER')
    if dataframe is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'EXTRAER'.")
        raise ValueError("El DataFrame no se pudo obtener desde 'EXTRAER'. Verifica la tarea anterior.")
    columnas_faltantes = [col for col in columnas if col not in dataframe.columns]
    if columnas_faltantes:
        logging.error(f"Las siguientes columnas no se encontraron en el DataFrame: {columnas_faltantes}")
        raise ValueError(f"Error: Columnas faltantes: {columnas_faltantes}")
    dataframe_seleccionado = dataframe[columnas]
    ti.xcom_push(key='dataframe_seleccion', value=dataframe_seleccionado)  # Asegúrate de esta clave
    logging.info(f"Selección de columnas realizada exitosamente. Columnas seleccionadas: {columnas}")

