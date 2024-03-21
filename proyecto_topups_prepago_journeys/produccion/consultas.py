import pandas as pd
import numpy as np

import cx_Oracle

import consultas as cst

ruta = 'C:/Users/HP EliteBook 840 G3/Documents/wom/instantclient-basic-windows.x64-19.11.0.0.0dbru/instantclient_19_11'
cx_Oracle.init_oracle_client(lib_dir = ruta)
dsn_tns = cx_Oracle.makedsn('10.41.87.18', '1521', service_name='DWHWOM')
conn = cx_Oracle.connect(user=r'ANALITICA_WOM', password='ANALI2023*5', dsn=dsn_tns)

def tablas():
    df = pd.read_csv('https://gist.githubusercontent.com/JuanFeA98/7fac4c04b7256fd976061ea2dad75b17/raw/b26dd68d16ff4f0ca78fdb8a034d07a25286ffa3/DWH_Tablas', sep=';', index_col=0)
    return(df)

def columnas(tabla):
    query = f'SELECT * FROM {tabla} WHERE rownum <=10'
    
    df = pd.read_sql(query, con = conn)
    print(list(df))

def consulta(query):
    df = pd.read_sql(query, con=conn)
    return(df)

def consulta_cabecera(tabla):
    query = f'''
    SELECT *
    FROM {tabla}
    WHERE rownum <= 10
    '''

    df = pd.read_sql(query, con = conn)
    return(df)

def consulta_total(tabla):
    query = f'''
    SELECT *
    FROM {tabla}
    '''
    
    df = pd.read_sql(query, con = conn)
    return(df)

def guardar(tabla, carpeta, nombre):
    tabla.to_csv(f'{carpeta}/{nombre}.csv')