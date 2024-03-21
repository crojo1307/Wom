import pandas as pd
import numpy as np

import consultas as cst

import pickle

from datetime import datetime

import logging
import warnings

warnings.filterwarnings("ignore")

dia = int(str(datetime.now())[:10].replace('-', ''))

LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    filename=f'./logging_{dia}.txt',
    level=logging.DEBUG,
    format=LOG_FORMAT,
    filemode='a'
)
logger = logging.getLogger()

logger.info('')
logger.info('Inicia la ejecucion del archivo: consolidar_base')
logger.info('')
#lectura

try:
    base = pd.read_csv(f'./datos/{dia}/base_{dia}.csv', index_col=0)
    trafico_datos = pd.read_csv(f'./datos/{dia}/trafico_datos_{dia}.csv', index_col=0)
    trafico_voz = pd.read_csv(f'./datos/{dia}/trafico_voz_{dia}.csv', index_col=0)
    paquetes_dur = pd.read_csv(f'./datos/{dia}/paquetes_dur_{dia}.csv', index_col=0)   
except Exception as e: 
    logger.error(f'Ocurrio un problema leyendo archivos: \n {e}.')
#consolidacion



try:
    trafico_datos['SUBSCRIBER_ID'] = trafico_datos['SUBSCRIBER_ID'].astype('int64')
    trafico_voz['SUBSCRIBER_ID'] = trafico_voz['SUBSCRIBER_ID'].astype('int64')
    df_insumos=base.merge(trafico_voz,left_on='SUBSCRIBER_ID',right_on='SUBSCRIBER_ID',how='left')
    df_insumos=df_insumos.merge(trafico_datos,left_on='SUBSCRIBER_ID',right_on='SUBSCRIBER_ID',how='left')
    df_insumos=df_insumos.merge(paquetes_dur,left_on='SUBSCRIBER_ID',right_on='SUBSCRIBER_ID',how='left')
    df_insumos.drop_duplicates(subset='SUBSCRIBER_ID', inplace=True)
    df_insumos.fillna(0, inplace=True)
    df_insumos.to_csv(f'./datos/{dia}/df_insumos_{dia}.csv')
    logger.info(f'Se han guardado {len(df_insumos)} registros del dataframe final.')

except Exception as e: 
    logger.error(f'Ocurrio un problema juntando variables: \n {e}.')

#construccion variables
# Modelo
try:
    logger.info('Iniciando con el modelo....')
    
    model = pickle.load(open("model_topups_v2.1.pkl", "rb"))
    X=df_insumos[['VALOR_ULTIMA_RECARGA','GROSS_PERMA','CICLOS', 'CANTIDAD_LLAMADAS', 'DURACION_LLAMADAS',
       'DIAS_LLAMADAS', 'MB_CONSUMIDAS', 'DIAS_NAV','DIAS_VENCIDO']]

    prediction = pd.DataFrame(model.predict(X))
    prediction.columns = ['PREDICCION']

    prediction_prob = pd.DataFrame(model.predict_proba(X))
    prediction_prob.columns = ['PROBABILIDAD_0', 'PROBABILIDAD_1']

    df_final = pd.merge(df_insumos.reset_index(), prediction.reset_index(), how='left', left_on='index', right_on='index')
    df_final.drop('index', axis=1, inplace=True)
    df_final = pd.merge(df_final.reset_index(), prediction_prob[['PROBABILIDAD_1']].reset_index(), how='left', left_on='index', right_on='index')
    df_final.drop('index', axis=1, inplace=True)
    df_final.rename(columns={'PROBABILIDAD_1':'PROBABILIDAD'}, inplace=True)
    df_final['PROBABILIDAD'] = df_final['PROBABILIDAD'].apply(lambda x: round(x*100))
    df_final.to_csv(f'./datos/{dia}/Resultados_{dia}.csv')

    logger.info(f'Se han guardado {len(df_final)} registros con los resultados del modelo.')

except Exception as e: 
    logger.error(f'Ocurrio un problema: \n {e}.')

logger.info('Termina la ejecucion del archivo: consolidar_base')

