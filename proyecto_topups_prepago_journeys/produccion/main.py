import os
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
    filemode='w'
)
logger = logging.getLogger()

logger.info('Inicia el proceso')

try:
    os.mkdir(f'./datos/{dia}')
    logger.info(f'La carpeta para el dia {dia} ha sido creada con exito.')

except Exception as e: 
    logger.error(f'Problema creando la carpeta: \n {e}.')


try:
    os.system('python construir_variables.py')
    os.system('python consolidar_base.py')
    os.system('python cargue_a_DWH.py')

except Exception as e: 
    logger.error(f'Problema ejecutando el archivo: \n {e}.')
