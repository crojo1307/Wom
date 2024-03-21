import pandas as pd
import numpy as np

from datetime import datetime

import logging
import warnings

import cx_Oracle    
import sqlalchemy as sa

warnings.filterwarnings("ignore")

dia = int(str(datetime.now())[:10].replace('-', ''))
mes=int(str(dia)[:6])

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    filename=f'./logging_{dia}.txt',
    level=logging.DEBUG,
    format=LOG_FORMAT,
    filemode='a'
)
logger = logging.getLogger()

logger.info('')
logger.info('Inicia la ejecucion del archivo: cargue_a_DWH')
logger.info('')


# Conexión BBDD
try:
    logger.info('Iniciando la conexión con la BBDD.')
    ruta = 'C:/Users/HP EliteBook 840 G3/Documents/wom/instantclient-basic-windows.x64-19.11.0.0.0dbru/instantclient_19_11'
    cx_Oracle.init_oracle_client(lib_dir = ruta)
    dsn_tns = cx_Oracle.makedsn('10.41.87.18', '1521', service_name='DWHWOM')
    conn = cx_Oracle.connect(user=r'ANALITICA_WOM', password='ANALI2023*5', dsn=dsn_tns)
    cur = conn.cursor()

    oracle_db = sa.create_engine('oracle+cx_oracle://ANALITICA_WOM:ANALITIC2021*@10.41.87.18:1521/?service_name=DWHWOM')
    connection = oracle_db.connect()
    logger.info('Conexion realizada con exito.')
except Exception as e: 
    logger.error(f'Ocurrio un problema: \n {e}.')


# Bases
try:
    logger.info('Preparando archivos para DWH.')
    df = pd.read_csv(f'./datos/{dia}/Resultados_{dia}.csv', index_col=0)
    df['PERIODO_PROCESO_CODIGO'] = mes
    df['FECHA_PRED'] = dia
    df['MODELO_VERSION'] = 'V2.1'
    df.rename(columns={'VALOR_ULTIMA_RECARGA':'VALOR_CARGA'}, inplace=True)
    df.rename(columns={'DIAS_VENCIDO':'DIAS_PAQ_VENCIDO'}, inplace=True)
    df = df[['SUBSCRIBER_ID', 'PERIODO_PROCESO_CODIGO','FECHA_PRED', 'PREDICCION', 'PROBABILIDAD','VALOR_CARGA','MODELO_VERSION','CICLOS','DIAS_PAQ_VENCIDO']]

    df.to_sql('agg_dl_pj_topups', connection, if_exists='append', index=False, chunksize=100000)    
    logger.info(f'Se cargan {len(df)} registros en dwh.')

except Exception as e: 
    logger.error(f'Ocurrio un problema: \n {e}.')
