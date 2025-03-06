import pandas as pd
import logging

def leer_archivo_principal(ti, ruta_directorio):
    dataframe_principal = pd.read_csv(ruta_directorio, delimiter=';')
    ti.xcom_push(key='dataframe_principal', value=dataframe_principal)
    logging.info("Archivo principal leído con éxito")

# Si hay otras funciones aquí, asegúrate de que no se importen dentro del mismo archivo.












