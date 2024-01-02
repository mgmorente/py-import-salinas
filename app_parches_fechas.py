import pandas as pd
import psycopg2
from config import config
import json
import re

from datetime import datetime, timedelta

def cargar_file():

    # Importar archivo XLSX con Pandas
    df = pd.read_excel('fechas.xlsx')

    # Almacenar el DataFrame en formato JSON
    df.to_json('fechas.json', orient='records')

    # Recorrer el archivo JSON
    with open('fechas.json') as file:
        data = json.load(file)

        with open('querys_fechas.txt', 'w') as file_output:
        
            for i,item in enumerate(data):
                update_fechas(file_output,item)
            
def valida_fecha(fecha):
    
    if type(fecha) is str and not fecha.strip():
        return None

    if fecha is None or fecha == 'None':
        return None
    
    timestamp_ms = int(fecha)
    if (timestamp_ms < 0):
        dias_a_restar = abs(timestamp_ms) / 86400000  # Calcular días a restar (milisegundos en un día)
        fecha_base = datetime(1970, 1, 1)  # Fecha base del estándar Unix
        fecha = fecha_base - timedelta(days=dias_a_restar)
    else:
        timestamp_s = timestamp_ms / 1000  # Convertir a segundos (dividir por 1000)
        fecha = datetime.utcfromtimestamp(timestamp_s)
    
    return fecha

def update_fechas(file_output, r):
    
    nif = re.sub(r'[^a-zA-Z0-9]', '', r['NIF'])
    nacimiento = valida_fecha(r['NACIMIENTO'])
    carnet = valida_fecha(r['CARNET'])
    matriculacion = valida_fecha(r['MATRICULACION'])
    matricula = get_matricula(r)

    if nacimiento is not None:
        query = f'''UPDATE clientes SET fecha_nacimiento = '{nacimiento}' WHERE created_by = '{created_by}' AND nif = '{nif}';'''
        file_output.write(str(query) + '\n')

    if carnet is not None:
        query = f'''UPDATE clientes SET fecha_carnet = '{carnet}' WHERE created_by = '{created_by}' AND nif = '{nif}';'''
        file_output.write(str(query) + '\n')
    
    if matriculacion is not None:
        query = f'''UPDATE polizas_autos a SET fecha_matriculacion = '{matriculacion}' FROM polizas p WHERE p.poliza = a.poliza AND a.created_by = '{created_by}' AND matricula = '{matricula}';'''
        file_output.write(str(query) + '\n')

    return

def obtener_valor(cadena, patron):
    resultado = re.search(patron, cadena)
    return resultado.group(1) if resultado else None

def get_matricula(r):
    # Define patrones regex para cada parte que deseas extraer
    patron_matricula = r"Matrícula:\s*([^\s]+)"

    # Busca los valores usando los patrones regex
    return obtener_valor(r['RIESGO'], patron_matricula) or ''

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# INIT
    
created_by = 'imp-salinas'

# read database configuration
params = config()
# connect to the PostgreSQL database
conn = psycopg2.connect(**params)

cargar_file()

# Cerrar conexion
if conn is not None:
    conn.close()

print('-- Success --')
