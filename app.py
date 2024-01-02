import pandas as pd
import psycopg2
from config import config
import json
import re

from datetime import datetime, timedelta

def get_nuevo_contrato():
    contrato = ''
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM incrementa_contador('SOLICITUDPOL')")
        contrato = sucursal + str(cur.fetchone()[0]).zfill(8)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return contrato

def formateo_poliza(r):
    cia_poliza = ''
    try:
        cur = conn.cursor()
        cur.execute("SELECT formateo_cia_poliza(%s, %s, %s)", (r['POLIZA'],r['COMPANIA_PACC'],r['RAMO_PACC']))
        cia_poliza = cur.fetchone()[0]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return cia_poliza

def cargar_file():

    # Importar archivo XLSX con Pandas
    df = pd.read_excel('datos.xlsx')

    # Almacenar el DataFrame en formato JSON
    df.to_json('datos.json', orient='records')

    # Recorrer el archivo JSON
    with open('datos.json') as file:
        data = json.load(file)
        
        for i,item in enumerate(data):
            insertar_poliza_bd(item)
            
            # if i >= 100:
            #     break

def only_numerics(s):
    return ''.join(filter(str.isdigit, s))

def valida_cadena(texto, longitud = 150):
    if texto is None or texto == 'None':
        return ''
    else:     
        return texto[0:longitud-1]

def valida_telefono(numero, campo_movil = False):
    if numero is None:
        return ''
    
    numero = only_numerics(numero)
    if not numero:
        return ''
    
    es_numero_movil = True if int(numero[0:1]) in [6,7] else False
    if es_numero_movil and campo_movil:
        return int(numero[0:9])
    elif not es_numero_movil and not campo_movil:
        return int(numero[0:9])
    else:
        return '' 

def valida_fecha(fecha):
    if fecha is None or fecha == '' or fecha == 'None':
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

def insertar_cliente_bd(values):
    
    sql_cliente = """INSERT INTO clientes(nif,nombre,domicilio,cpostal,poblacion,provincia,tel_privado,movil,email,fecha_nacimiento,fecha_carnet,persona,ecivil,sexo,nombre2,domicilio2,poblacion2,cpostal2,provincia2,fecha_alta,sucursal,colaborador,created_at,created_by,passweb)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT ON CONSTRAINT clientes_pk 
        DO NOTHING"""
    
    try:
        cur = conn.cursor()
        # Insertar en clientes
        cur.execute("select exists (select 1 from clientes where nif = %s)", (values[0],))
        if not cur.fetchone()[0]:
            cur.execute(sql_cliente, values)
            conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error al grabar cliente: ', values[0], error)

    return

def valida_email(valor):

    correos_separados = valor.split(';')

    # Tomar el primer correo electrónico (si existe)
    primer_correo = correos_separados[0].strip() if correos_separados else None
    return valida_cadena(primer_correo)

def dataCliente(r):

    _nombre = r['NOMBRE']
    _nif = r['NIF']
    _domicilio = r['DOMICILIO']
    _cpostal = r['CPOSTAL']
    _poblacion = r['POBLACION']
    _telefono = r['TELEFONO']
    _movil = r['MOVIL']
    _email = valida_email(r['EMAIL'])
    _nacimiento = None
    _carnet = None
    
    return (
        _nif,
        valida_cadena(_nombre, 45),
        valida_cadena(_domicilio, 45),
        valida_cadena(_cpostal, 6),
        valida_cadena(_poblacion, 30),
        _cpostal[0:2],
        valida_telefono(_telefono),  # telefono
        valida_telefono(_movil, True),  # movil
        _email,
        valida_fecha(_nacimiento),
        valida_fecha(_carnet),
        '',
        '',
        '',
        valida_cadena(_nombre, 45),
        valida_cadena(_domicilio, 45),
        valida_cadena(_poblacion, 30),
        valida_cadena(_cpostal, 6),
        _cpostal[0:2],
        'now()',  # fecha alta
        sucursal,  # sucursal
        r['COLABORADOR_PACC'],  # colaborador
        'now()',  # create_at
        created_by,  # created_by
        '',  # passweb
    )

def insertar_poliza_bd(r):
    
    contrato = get_nuevo_contrato()

    sql_poliza = """INSERT INTO polizas 
            (poliza,cia_poliza,cia_poliza_original,compania,producto,fecha_efecto,fecha_vencimiento,situacion,nif,nif_asegurado,ase_es_asegurado,matricula,forma_pago,
            tipo_poliza,objeto,comentario,fecha_alta,canal,iban,sucursal,colaborador,created_by) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING poliza"""

    sql_poliza_autos = """INSERT INTO polizas_autos 
            (poliza,marca,modelo,created_by) 
            VALUES (%s,%s,%s,%s)"""
    
    # Corregir nif
    r["NIF"] = re.sub(r'[^a-zA-Z0-9]', '', r['NIF'])

    for item in r:
        r[item] = str(r[item]).rstrip()
    
    # Define patrones regex para cada parte que deseas extraer
    patron_matricula = r"Matrícula:\s*([^\s]+)"
    patron_marca = r"Marca:\s*([^\s]+)"
    patron_modelo = r"Modelo:\s*([^\s]+)"

    # Busca los valores usando los patrones regex
    r["MATRICULA"] = obtener_valor(r['RIESGO'], patron_matricula) or ''
    r["MARCA"] = obtener_valor(r['RIESGO'], patron_marca) or ''
    r["MODELO"] = obtener_valor(r['RIESGO'], patron_modelo) or ''

    r["COMPANIA_PACC"] = get_compania(r['COMPANIA'])
    r["RAMO_PACC"] = get_ramo(r)
    r["COLABORADOR_PACC"] = get_colaborador(r["COMPANIA_PACC"])

    try:
        cur = conn.cursor()
        # insertar cliente
        # print('\ncliente...',r["NIF"])
        insertar_cliente_bd(dataCliente(r))
        # insertar poliza
        # print('poliza...',r["POLIZA"])
        cur.execute(sql_poliza, values_poliza(contrato, r))
        # insertar poliza autos
        if int(str(get_ramo(r))[0]) == 6: 
            cur.execute(sql_poliza_autos, values_poliza_auto(contrato, r))
   
        conn.commit()
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return contrato

# Función para extraer valores o devolver None si no se encuentra el patrón
def obtener_valor(cadena, patron):
    resultado = re.search(patron, cadena)
    return resultado.group(1) if resultado else None

def values_poliza_auto(contrato, r):
    return (
        contrato, 
        r["MARCA"], 
        r["MODELO"], 
        created_by,
    )

def get_ramo(r):
    
    nombre_ramo = r['RAMO']
    poliza = r['POLIZA']

    ramos = [
        ("1501-Turismos y Furgonetas", 614),
        ("1502-Camiones y Cabezas Tractoras", 601),
        ("1503-Motocicletas", 617),
        ("1504-Vehiculos industriales", 618),
        ("1508-Ciclomotores", 617),
        ("1509-Remolques/Semirremolque", 614),
        ("1510-Vehículos eléctricos mov. personal", 103),
        ("2806-Vida Temporal Renovable Colectivo", 702),
        ("2825-Vida Temporal Renovable Individual", 400),
        # ("2936-Jubilacion Indiv.", None),
        ("3012-Embarcaciones", 509),
        ("3030-Retirada de carnet", 226),
        ("3042-Agroseguro, ganaderia", 1415),
        ("3060-Asistencia en Viaje Vehiculos", 220),
        ("3061-P. J. Familiar", 222),
        ("3066-P.J. Comercios", 222),
        ("3068-Asist. Viaje Personas", 212),
        ("3080-Salud individual/familiar", 305),
        ("3083-Salud colectivo", 704),
        ("3084-Salud Dental", 307),
        ("3200-Transportes", 500),
        ("3366-Multirriesgo Comunidades", 203),
        ("3377-Multirriesgo Hogar", 201),
        ("3380-Multirriesgo Comercio", 209),
        ("3381-Multirriesgo Industrial", 206),
        ("3401-R.C. General Empresas", 103),
        ("3402-R.C. Profesional", 115),
        ("3404-R.C. Cazador", 108),
        ("3405-R.C. General Particulares/Privada", 103),
        ("3407-R.C. Consejeros y Directivos DO", 102),
        ("3409-R.C. Asociaciones", 103),
        ("3416-R.C. Pescador", 108),
        ("3671-Equipos Electronicos", 110),
        ("3716-Indemnizacion Diaria (I.T.)", 101),
        ("3724-Accidentes Convenio Colectivo", 115),
        ("3725-Accidentes Individual", 101),
        ("3726-Accidentes Colectivo", 115),
        ("3730-Decesos", 421),
        ("8938-Plan Pensiones Indiv.", 403)
    ]
    
    for ramo, numero in ramos:
        if ramo == nombre_ramo:
            return numero
    
    # excepciones
    excepciones = [
        ("78217307571","417"),
        ("3V-G-140000697","406"),
        ("VH-5-966143769","417"),
        ("78217307506","417"),
        ("018430815","417"),
        ("08-0086802","417"),
        ("3V-G-140000401","406"),
        ("3V-G-140000512","406"),
        ("78217306110","417"),
        ("750032891","417"),
        ("62112859","415"),
        ("41-0088383","417"),
        ("3V-G-140000219","406"),
        ("3V-G-140000220","406"),
        ("3V-G-140000259","406"),
        ("3V-G-140000457","406"),
        ("3V-G-140000468","406"),
        ("3V-G-140000467","406"),
        ("1V-G-140000037","403"),
        ("3V-G-140000550","407"),
        ("3V-G-140000549","406"),
        ("3V-G-140000356","406"),
        ("BIVS009306","407"),
        ("BIVS009304","406"),
        ("18-0121909","415"),
        ("08-0079018","417"),
        ("41-0088290","417"),
    ]

    for _poliza, numero in excepciones:
        if _poliza == poliza:
            return numero
    
    # Si no se encuentra el ramo, buscar el número entero antes del guion
    return int(nombre_ramo.split('-')[0])

def get_compania(nombre_compania):
    aseguradoras = [
        ["ALLIANZ, COMPAÑIA DE SEGUROS Y REASEGUROS, S.A.", 13],
        ["ARAG S.E. SUCURSAL EN ESPAÑA", 33],
        ["ASISA, ASISTENCIA SANIT. INTERPROV. SEG.Y REAS.,SA", 22],
        ["AXA AURORA VIDA, S.A.", 21],
        ["AXA PENSIONES EGFP, S.A.", 198],
        ["AXA SEGUROS GENERALES, S.A.", 2],
        ["BILBAO COMPAÑIA DE SEGUROS Y REASEGUROS, S.A.", 30],
        ["CHUBB EUROPEAN GROUP SUCURSAL EN ESPAÑA", 24],
        ["DAS INTERNACIONAL", 11],
        ["DKV SEGUROS SEGUROS Y REASEGUROS, S.A.", 4],
        ["FIATC MUTUA DE SEGUROS Y REASEGUROS", 49],
        ["GENERALI ESPAÑA, S.A. DE SEGUROS Y REASEGUROS", 8],
        ["HELVETIA CIA. SUIZA SA DE SEGUROS Y REASEGUROS, SA", 37],
        ["LIBERTY COMPAÑIA DE SEGUROS Y REASEGUROS, S.A.", 34],
        ["MAPFRE ESPAÑA SEGUROS Y REASEGUROS, S.A.", 27],
        ["MUTUA DE PROPIETARIOS", 18],
        ["PELAYO MUTUA DE SEGUROS Y REASEGUROS APF", 19],
        ["PLUS ULTRA, SEGUROS GENERALES Y VIDA, S.A.", 1],
        ["REALE SEGUROS GENERALES, S.A.", 38],
        ["SANITAS, S.A. DE SEGUROS", 26],
        ["SANTA LUCIA, S.A.", 42],
        ["SEGURCAIXA ADESLAS, S.A.", 35],
        ["W.R. BERKLEY EUROPE AG SUCURSAL ESPAÑA", 20],
        ["ZURICH INSURANCE, PLC SUCURSAL EN ESPAÑA", 9],
        ["ZURICH VIDA SEGUROS, S.A.", 111]
    ]

    for compania, numero in aseguradoras:
        if compania == nombre_compania:
            return numero

    # Si no se encuentra la compañía, se puede devolver un valor predeterminado o None
    return None

def get_formapago(valor):
    if valor == 'Temporal':
        return 'UNI'
    
    return valor[:3].upper()

def get_canalpago(valor):
    if valor == 'COMPAÑÍA':
        return 6  
    elif valor == 'REMESA':
        return 4
    elif valor == 'TRANSFERENCIA':
        return 0
    else:
        return 9

def get_colaborador(valor):
    if valor in [8,9,111,26]:
        return '14010195'
    else:
        return '14010196'

def values_poliza(contrato, r):

    comentario = r['RIESGO'] + "\n" + r['EMAIL']

    _cia_poliza_original = r['POLIZA']
    _cia_poliza_formateada = formateo_poliza(r)

    return (
        contrato,
        _cia_poliza_formateada,
        _cia_poliza_original,
        r['COMPANIA_PACC'],
        r['RAMO_PACC'],
        valida_fecha(r['EFECTO']),
        valida_fecha(r['VENCIMIENTO']),
        situacion,
        r['NIF'],
        r['NIF'],                                           # nif asegurado
        True,    # es asegurado
        r["MATRICULA"],                                                 # matricula
        get_formapago(r['FORMAPAGO']),
        1,
        r['RIESGO'],        # objeto
        comentario,
        'now()',
        get_canalpago(r['MEDIOPAGO']),
        valida_cadena(r['CCC']),
        sucursal,
        r['COLABORADOR_PACC'],
        created_by,
    )
            

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# INIT
    
tipo_poliza = 1
situacion = 1
compania = 0
ramo = 0
canal = 0
sucursal = '1401'
colaborador = ''
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
