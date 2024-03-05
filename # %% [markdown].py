# %% [markdown]
# ### CONEXION API

# %%
#importa librerias
import pandas as pd
import requests
import json
import psycopg2
from sqlalchemy import create_engine


# %%
# Leer la API key desde el archivo de texto
with open("/Users/julietasaez/Documents/data_engineer/entrega_1/key.txt",'r') as f:
    pwd= f.read()

# %%
# URL de la API
url = 'https://opendata.aemet.es/opendata/api/observacion/convencional/todas'

# Parametros de la solicitud GET
params = {"api_key": pwd}

# Realiza la solicitud GET a la API
response = requests.get(url, params=params)

# Verifica si la solicitud fue exitosa
if response.status_code == 200:
    try:
        # Obtiene la URL de los datos reales del diccionario de respuesta
        data_url = response.json().get('datos')
        metadatos_url = response.json().get('metadatos')
        
        # Realiza una solicitud GET a la URL de los datos reales
        response_data = requests.get(data_url)
        response_metadatos  = requests.get(metadatos_url)
        
        # Carga los datos en un DataFrame si la solicitud fue exitosa
        if response_data.status_code == 200:
            # Obtiene los datos en formato JSON
            data_json = response_data.json()
            
            # Convierte los datos a un DataFrame de Pandas
            df = pd.DataFrame(data_json)
            
            # Muestra el DataFrame
            print(df.head())
        else:
            print("Error al obtener los datos reales:", response_data.status_code)
    except Exception as e:
        print("Error al procesar la respuesta:", e)
else:
    print("Error al obtener la URL de los datos:", response.status_code)


# %% [markdown]
# ### EXPLORACION DATA

# %%
# Obtiene la URL de los datos reales del diccionario de respuesta
metadatos_json = response_metadatos.json()
# Convierte los datos a un DataFrame de Pandas
df_aux = pd.DataFrame(metadatos_json)
# Imprime resultados del df
df_aux.head()

# %%
# Normaliza la columna "campos" del diccionario JSON para entender la metadata
df_normalized = pd.json_normalize(df_aux['campos'])

# Imprime resultados
df_normalized

# %%
# Obtiene cantidad de filas y columnas del dataframe de candidate
print("El dataset de users tiene {} filas y {} columnas".format(df.shape[0],df.shape[1]))

# %%
# Selecciona columnas deseadas para el dataframe final
columnas_seleccionadas = ['ubi','lon', 'lat','alt','fint', 'prec', 'hr', 'ta', 'tamin', 'tamax']
df_final = df[columnas_seleccionadas]
df_final

# %% [markdown]
# ### CONEXION REDSHIFT

# %%
# Crea la conexión a Redshift
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="julietagsaez_coderhouse"
with open("/Users/julietasaez/Documents/data_engineer/entrega_1/redshift_key.txt",'r') as f:
    pwd_rd= f.read()
try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=pwd_rd,
        port='5439'
    )
    print("Conectado a Redshift con éxito")
    
except Exception as e:
    print("Error al intentar conectar a Redshift")
    print(e)

# %%
# Crea la tabla en Redshift con el ID + columnas 
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS julietagsaez_coderhouse.estaciones_metereologicas (
            id INT IDENTITY(1,1) PRIMARY KEY,
            ubicacion VARCHAR (50),
            longitud FLOAT,
            latitud FLOAT,
            altitud FLOAT, 
            fecha_obs VARCHAR(50), 
            precipitaciones FLOAT,
            humedad FLOAT,
            temperatura FLOAT, 
            temperatura_min FLOAT,
            temperatura_max FLOAT         
        )
    """)
    conn.commit()

# %%
#Vacia la tabla para evitar duplicados o inconsistencias
with conn.cursor() as cur:
  cur.execute("Truncate table estaciones_metereologicas")
  count = cur.rowcount

# %%
# Consulta la tabla
cur = conn.cursor()
cur.execute("SELECT * FROM estaciones_metereologicas")
resultados = cur.fetchall()
resultados 

# %%
# Verifica columnas con valores nulos
print(df_final.isnull().sum())



