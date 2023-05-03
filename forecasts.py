import os
import pyarrow
from minio import Minio
import requests
import pandas as pd
import json
import openpyxl
from dotenv.main import load_dotenv
from pyarrow import json as j
import pyarrow.parquet as pq
import pyarrow as pa

# loading environment variables
load_dotenv()

# Reading the excel file using pandas to extract coordinates of dragino and deltaohm
filename = 'InventarStatischeSensoren.xlsx'
dragino = pd.read_excel(filename, sheet_name='Dragino LWS')
deltaohm = pd.read_excel(filename, sheet_name='DeltaOhm LWS')

#metadata for dragino
coordinates = dragino['Coordinates'].tolist()
sensor_id = dragino['ID_TTS'].tolist()
eui = dragino['EUI'].tolist()
laubwand = dragino['Laubwand'].tolist()
ausbringungsort = dragino['Ausbringungsort'].tolist()

#metadata for deltaohm
coordinates_del = deltaohm['Coordinates'].tolist()
sensor_id_del = deltaohm['ID_TTS'].tolist()
laubwand_del = deltaohm['Laubwand'].tolist()
ausbringungsort_del = deltaohm['Ausbringungsort'].tolist()

# Extracting coordinates of dragino from InventarStatischeSensoren.xlsx and reset index
dragino[['Latitude', 'Longitude']] = dragino['Coordinates'].apply(lambda x: pd.Series(str(x).split(",")))

# Extracting coordinates of deltaohm from InventarStatischeSensoren.xlsx and reset index
deltaohm[['Latitude', 'Longitude']] = deltaohm['Coordinates'].apply(lambda x: pd.Series(str(x).split(",")))

#cleaning and adding metadata to dragino
df_dragino = dragino[['Latitude', 'Longitude']]
df_dragino_meta = df_dragino.assign(ID_TTS=sensor_id, EUI=eui, Laubwand=laubwand, Ausbringungsort=ausbringungsort)
df_dragino_clean = df_dragino_meta.dropna()
# df_dragino = dragino_clean.drop_duplicates()
df_dragino_clean.reset_index(drop=True, inplace=True)


#cleaning and adding metadata to deltaohm
df_deltaohm = deltaohm[['Latitude', 'Longitude']]
df_deltaohm_meta = df_deltaohm.assign(ID_TTS=sensor_id_del, Laubwand=laubwand_del, Ausbringungsort=ausbringungsort_del)
df_deltaohm_clean = df_deltaohm_meta.dropna()
# df_dragino = dragino_clean.drop_duplicates()
df_deltaohm_clean.reset_index(drop=True, inplace=True)


def forecasts():
    # connecting to minio
    client = Minio(
        "data.smarter-weinberg.de:9000",
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False
    )

    # checking if project bucket exists
    project_bucket = client.bucket_exists("projekt-daten")
    if not project_bucket:
        print("Bucket 'projekt-daten' does not exist")
    else:
        print("Bucket 'projekt-daten' already exists")

    # Looping over coordinates of dragino and making api calls
    for i in range(len(df_dragino_clean)):
        latitude = df_dragino_clean.loc[i, 'Latitude']
        longitude = df_dragino_clean.loc[i, 'Longitude']
        sensor_id = df_dragino_clean.loc[i, 'ID_TTS']
        eui = df_dragino_clean.loc[i, 'EUI']
        laubwand = df_dragino_clean.loc[i, 'Laubwand']
        ausbringungsort = df_dragino_clean.loc[i, 'Ausbringungsort']
        api_url = "https://my.meteoblue.com/packages/agro-1h_agromodelleafwetness-1h?apikey=946f8e5caba1&lat=" + latitude + "&lon=" + longitude + "&asl=75&format=json&tz=Europe%2FBerlin"
        response = requests.get(api_url).json()
        response['metadata']['name'] = sensor_id
        response['metadata']['EUI'] = eui
        response['metadata']['Laubwand'] = laubwand
        response['metadata']['Ausbringungsort'] = ausbringungsort
        jsonresponse = json.dumps(response)
        with open(sensor_id + ".json", 'w') as f:
            f.write(jsonresponse)
        table = pa.json.read_json(sensor_id + ".json")
        pq.write_table(table, sensor_id + ".parquet")

        if project_bucket:
            client.fput_object("projekt-daten", "UniKo/Egov/WetterDaten/Meteoblue/json-formatted" + os.path.basename(f.name), f.name)
            #client.fput_object("projekt-daten", "UniKo/Egov/WetterDaten/Meteoblue/parquet-formatted" + w, w)
    print("Dragino coordinate responses uploaded to Minio")



    #Looping over coordinates of deltaohm and making api calls
    for i in range(len(df_deltaohm_clean)):
        latitude = df_deltaohm_clean.loc[i, 'Latitude']
        longitude = df_deltaohm_clean.loc[i, 'Longitude']
        sensor_id_del = df_deltaohm_clean.loc[i, 'ID_TTS']
        laubwand_del = df_deltaohm_clean.loc[i, 'Laubwand']
        ausbringungsort_del = df_deltaohm_clean.loc[i, 'Ausbringungsort']
        api_url = "https://my.meteoblue.com/packages/agro-1h_agromodelleafwetness-1h?apikey=946f8e5caba1&lat=" + latitude + "&lon=" + longitude + "&asl=75&format=json&tz=Europe%2FBerlin"
        response = requests.get(api_url).json()
        response['metadata']['name'] = sensor_id_del
        response['metadata']['Laubwand'] = laubwand_del
        response['metadata']['Ausbringungsort'] = ausbringungsort_del
        jsonresponse = json.dumps(response)
        with open(sensor_id_del + ".json", 'w') as f:
            f.write(jsonresponse)
        table = pa.json.read_json(sensor_id_del + ".json")
        pq.write_table(table, sensor_id_del + ".parquet")

        if project_bucket:
            client.fput_object("projekt-daten", "UniKo/Egov/WetterDaten/Meteoblue/" + os.path.basename(f.name), f.name)
            #client.fput_object("projekt-daten", "UniKo/Egov/WetterDaten/Meteoblue/parquet-formatted" + s, s)
    print("Deltaohm coordinate responses uploaded to Minio")


forecasts()
