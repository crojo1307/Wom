import pandas as pd
import numpy as np

import consultas as cst

from datetime import datetime

import logging
import warnings

warnings.filterwarnings("ignore")

dia = int(str(datetime.now())[:10].replace('-', ''))
mes=int(str(dia)[:6])
ciclos=6
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(
    filename=f'./logging_{dia}.txt',
    level=logging.DEBUG,
    format=LOG_FORMAT,
    filemode='a'
)

logger = logging.getLogger()

logger.info('')
logger.info('Inicia la ejecucion del archivo: construir_variables')
logger.info('')
logger.info('Descargando Base...')

query = '''
WITH BASE AS 
(SELECT /*PARALLEL(8)*/
        A.SUBSCRIBER_ID,
        A.MOVIMIENTO_NOMBRE 
FROM DWH_BODEGA_WOM.FCT_SUBSCRIBERS_ENDING A 
WHERE A.PERIODO_PROCESO_CODIGO={mes}
AND A.SERVICIO='Prepaid'
AND A.MOVIMIENTO_NOMBRE IN ('ACTIVACION','PERMANECE')
AND A.ESTADO='Active'
AND A.SUBSCRIBER_ID IN (
            SELECT SUBSCRIBER_ID FROM ( 
                    SELECT B.*,
                    ROW_NUMBER() OVER (PARTITION BY SUBSCRIBER_ID ORDER BY FECHA_PRED DESC) AS RN 
                    FROM AGG_DL_PJ_QUALITY B )
            WHERE RN=1
            AND   PREDICCION=0
)),
RECARGAS AS (
SELECT /*PARALLEL(8)*/
       B.SUBSCRIBER_ID,
       B.TIEMPO_RECARGA_DK AS TIEMPO_ULTIMA_RECARGA,
       B.VALOR_CARGA AS VALOR_ULTIMA_RECARGA,
       B.FECHA_RECARGA AS ULTIMA_RECARGA
FROM (
SELECT /*PARALLEL(8)*/
       B.*,
       ROW_NUMBER() OVER (PARTITION BY SUBSCRIBER_ID ORDER BY FECHA_RECARGA DESC) AS RN  
FROM DWH_BODEGA_WOM.FCT_RECARGAS B
WHERE   B.TIEMPO_RECARGA_DK<={dia}
        AND B.SUBSCRIBER_ID IN (
        SELECT A.SUBSCRIBER_ID 
        FROM DWH_BODEGA_WOM.FCT_SUBSCRIBERS_ENDING A 
        WHERE A.PERIODO_PROCESO_CODIGO={mes}
        AND A.SERVICIO='Prepaid'
        AND A.MOVIMIENTO_NOMBRE IN ('ACTIVACION','PERMANECE')
        AND A.ESTADO='Active'
        AND A.SUBSCRIBER_ID IN (
                SELECT SUBSCRIBER_ID FROM ( 
                        SELECT B.*,
                        ROW_NUMBER() OVER (PARTITION BY SUBSCRIBER_ID ORDER BY FECHA_PRED DESC) AS RN 
                        FROM AGG_DL_PJ_QUALITY B )
                WHERE RN=1
                AND   PREDICCION=0
        )
) )B
WHERE B.RN=1
)
SELECT * FROM  (
SELECT  /*PARALLEL(8)*/
        A.MOVIMIENTO_NOMBRE,
        B.*,
        CASE
            WHEN A.MOVIMIENTO_NOMBRE='ACTIVACION' THEN 1 
            ELSE 0 
        END AS GROSS_PERMA,
        CASE 
            WHEN B.TIEMPO_ULTIMA_RECARGA >= {dia}-5 THEN B.TIEMPO_ULTIMA_RECARGA
            ELSE TO_NUMBER(TO_CHAR(B.ULTIMA_RECARGA+ROUND((TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-B.ULTIMA_RECARGA)/5,0)*5 -5,'YYYYMMDD')) 
        END AS INICIO_CICLO,
        {dia} AS FECHA_PRED,
        ROUND((TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-B.ULTIMA_RECARGA)/5,0)-1 AS CICLOS 
FROM BASE A 
JOIN RECARGAS B ON A.SUBSCRIBER_ID=B.SUBSCRIBER_ID 
)G WHERE INICIO_CICLO=TO_NUMBER(TO_CHAR(TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-5,'YYYYMMDD'))
   AND CICLOS<={ciclos} 
'''

try:
    base = cst.consulta(query)
    base.to_csv(f'./datos/{dia}/base_{dia}.csv')
    logger.info(f'Se han descargado {len(base)} registros del dataframe base.')
except Exception as e: 
    logger.error(f'Ocurrio un problema: \n {e}.')

logger.info('Descargando trafico_voz...')

query = '''
SELECT  SUBSCRIBER_ID,
        SUM(CANT_CALLS) AS CANTIDAD_LLAMADAS,
        SUM(DURACION_CALLS) AS DURACION_LLAMADAS,
        COUNT(DISTINCT(PERIODO_PROCESO_CODIGO)) AS DIAS_LLAMADAS
FROM AGG_DL_VOICE
WHERE PERIODO_PROCESO_CODIGO BETWEEN TO_NUMBER(TO_CHAR(TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-5,'YYYYMMDD')) AND TO_NUMBER(TO_CHAR(TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-1,'YYYYMMDD'))
      AND SERVICIO='Prepaid'
      AND SENTIDO='SALIENTE'
      AND SUBSCRIBER_ID IN (SELECT SUBSCRIBER_ID FROM ( 
                    SELECT *
                     
                    FROM AGG_DL_PJ_QUALITY B )
            WHERE PREDICCION=0)
GROUP BY SUBSCRIBER_ID
'''

try:
    voz = cst.consulta(query)
    voz.to_csv(f'./datos/{dia}/trafico_voz_{dia}.csv')
    logger.info(f'Se han descargado {len(voz)} registros del dataframe trafico_voz.')
except Exception as e: 
    logger.error(f'Ocurrio un problema: \n {e}.')

logger.info('Descargando trafico_datos...')

query = '''
SELECT SUBSCRIBER_ID,
       ROUND(SUM(BYTES_TOTALES)/1024/1024,0) AS MB_CONSUMIDAS ,
       COUNT(DISTINCT(PERIODO_PROCESO_CODIGO)) AS DIAS_NAV 
FROM AGG_DL_DATA
WHERE PERIODO_PROCESO_CODIGO BETWEEN TO_NUMBER(TO_CHAR(TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-5,'YYYYMMDD')) AND TO_NUMBER(TO_CHAR(TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-1,'YYYYMMDD'))
      AND SERVICIO='Prepaid'
      AND SUBSCRIBER_ID IN (SELECT SUBSCRIBER_ID FROM ( 
                    SELECT *
                     
                    FROM AGG_DL_PJ_QUALITY B )
            WHERE PREDICCION=0)
GROUP BY SUBSCRIBER_ID
'''

try:
    datos = cst.consulta(query)
    datos.to_csv(f'./datos/{dia}/trafico_datos_{dia}.csv')
    logger.info(f'Se han descargado {len(datos)} registros del dataframe trafico_datos.')
except Exception as e: 
    logger.error(f'Ocurrio un problema: \n {e}.')

logger.info('Descargando paquetes_dur...')

query = '''
SELECT C.*,
       ROUND(TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-C.FECHA_VENCE,0) AS DIAS_VENCIDO
FROM (
SELECT A.SUBSCRIBER_ID,
       A.FECHA_RECARGA,
       A.PAQUETE_NOMBRE,
       COALESCE(B.DURACION,3),
       A.FECHA_RECARGA+COALESCE(B.DURACION,3) AS FECHA_VENCE, 
       ROW_NUMBER() OVER(PARTITION BY A.SUBSCRIBER_ID ORDER BY A.FECHA_RECARGA DESC ) AS RN
FROM DWH_BODEGA_WOM.FCT_PAQUETES A
JOIN PAQUETES B ON A.PAQUETE_NOMBRE=B.PAQUETE_NOMBRE
WHERE TIEMPO_PAQUETE_DK<=TO_NUMBER(TO_CHAR(TO_DATE(TO_CHAR({dia}), 'YYYY/MM/DD')-5,'YYYYMMDD'))
      AND SERVICIO='Prepaid'
      AND SUBSCRIBER_ID IN (SELECT SUBSCRIBER_ID FROM ( 
                    SELECT *
                     
                    FROM AGG_DL_PJ_QUALITY B )
            WHERE PREDICCION=0)
    ) C WHERE RN=1
'''

try:
    paquetes_dur = cst.consulta(query)
    paquetes_dur.to_csv(f'./datos/{dia}/paquetes_dur_{dia}.csv')
    logger.info(f'Se han descargado {len(paquetes_dur)} registros del dataframe paquetes_dur.')
except Exception as e: 
    logger.error(f'Ocurrio un problema: \n {e}.')
logger.info('Termina la ejecucion del archivo: construir_variables')