import logging

def filtrar_columnas(ti, valor):
    dataframe_principal = ti.xcom_pull(key='dataframe', task_ids='TRANSFORMACION.seleccion')
    if dataframe_principal is None:
        logging.error("Error: No se pudo obtener el DataFrame de la tarea 'EXTRAER'.")
        raise ValueError("El DataFrame no se pudo obtener desde 'EXTRAER'. Verifica la tarea anterior.")
    
    columnas_disponibles = dataframe_principal.columns
    logging.info(f"Columnas disponibles en el DataFrame: {columnas_disponibles}")

    # Ajuste para no usar 'Resolución de problemas_Valor'
    if 'Resolución de problemas_Valor' not in columnas_disponibles:
        logging.warning("'Resolución de problemas_Valor' no está en el DataFrame. Evitando este paso.")

    # Implementar la lógica de filtrado correcta aquí sin usar 'Resolución de problemas_Valor'
    final = dataframe_principal[dataframe_principal['Alguna_Columna_Existente'] > valor]
    
    ti.xcom_push(key='final', value=final)
    logging.info(f"Filtrado de columnas realizado con valor {valor}")
