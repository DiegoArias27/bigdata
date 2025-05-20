#Uso de librerias para dicho proyecto
from ElasticSearchProvider import ElasticSearchProvider
import json
import pandas as pd
import re
import matplotlib.pyplot as plt

#Variables globales que seran utilizadas durante el proyecto
file_json_path = "Datos_Accidentes.json"
file_csv_path = "Traffic_Crashes_-_Crashes.csv"
mapping_json_path = "mapping.json"
name_index = "datos-accidentes-chicago"


#Cargar los datos de un csv a un dataframe
df = pd.read_csv(file_csv_path)

#Borrar las columnas que no nos hagan falta
df = df.drop(columns=["CRASH_DATE_EST_I","POSTED_SPEED_LIMIT","LANE_CNT","INTERSECTION_RELATED_I","NOT_RIGHT_OF_WAY_I","HIT_AND_RUN_I", "DATE_POLICE_NOTIFIED", "PHOTOS_TAKEN_I", "STATEMENTS_TAKEN_I", "DOORING_I", "WORK_ZONE_I", "WORK_ZONE_TYPE", "WORKERS_PRESENT_I", "INJURIES_UNKNOWN"])

#Darle formato a la fecha del accidente
df["CRASH_DATE"] = pd.to_datetime(df["CRASH_DATE"], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

# Columnas numéricas que deben ser float
columnas_float = [
    "INJURIES_TOTAL",
    "INJURIES_FATAL",
    "INJURIES_INCAPACITATING",
    "INJURIES_NON_INCAPACITATING",
    "INJURIES_REPORTED_NOT_EVIDENT",
    "INJURIES_NO_INDICATION",
    "BEAT_OF_OCCURRENCE"
]

# Convertir columnas float: reemplazar valores no numéricos por NaN y luego rellenar con 0
for col in columnas_float:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

# Para el resto de las columnas, reemplazar nulos o vacíos con "UNKNOWN"
for col in df.columns:
    if col not in columnas_float:
        df[col] = df[col].replace("", "UNKNOWN").fillna("UNKNOWN")

# Función para extraer el valor máximo numérico de la columna DAMAGE
def extraer_max_valor(damage):
    if pd.isna(damage):
        return None
    valores = re.findall(r"\d+(?:,\d+)?", damage)
    valores = [int(v.replace(",", "")) for v in valores]
    if "OVER" in damage or "LESS" in damage:
        return valores[0]
    else:
        return max(valores)

# Reemplazar la columna DAMAGE con su valor numérico máximo
df["DAMAGE"] = df["DAMAGE"].apply(extraer_max_valor)

# Convertir el DataFrame a una lista de diccionarios (una lista de registros)
records = df.to_dict(orient="records")

# Guardar el JSON con formato de lista completa (con comas entre objetos)
with open(file_json_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

# Verificar conexión de ElasticSearch
try:
    with ElasticSearchProvider(index=name_index) as es_handler:
        if es_handler.check_connection():
            print("Conexión exitosa de ElasticSearch")
        else:
            print("Error en la conexión de ElasticSearch")
        
        response = es_handler.show_indices()
        print(json.dumps(response, indent=2))
except Exception as e:
    print(f"Error {e}")

#Crear el indice en ElasticSearch e ingresar el mapping para dicho indice
try:
    with ElasticSearchProvider(index=name_index) as es:
        with open(mapping_json_path, "r") as j:
            mapping_data = json.load(j)
        response = es.create_index(mapping=mapping_data)
        print(response)
        response = es.get_mapping()
        print(response)
except Exception as e:
    print(f"Ocurrio un error: {e}") 

#Cargar el archivo json con la data a elasticsearch
try:
    with ElasticSearchProvider(index=name_index) as es:
        with open(file_json_path, "r") as f:
            datos = json.load(f)
        response = es.insert_batch(datos=datos)
        print(response)
except Exception as e:
    print(f"Ocurrio un error {e}")
        
