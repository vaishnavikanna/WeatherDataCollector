import glob
import os
from minio import Minio
import requests
import pandas as pd
import json
import openpyxl
from dotenv.main import load_dotenv
import pyarrow.parquet as pq
from fastparquet import write, ParquetFile, update_file_custom_metadata
import pyarrow as pa
import pyarrow.json as pj
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time

# loading environment variables
load_dotenv()

# Reading the excel file using pandas to extract coordinates of dragino and deltaohm
filename = 'InventarStatischeSensoren.xlsx'
dragino = pd.read_excel(filename, sheet_name='Dragino LWS')
deltaohm = pd.read_excel(filename, sheet_name='DeltaOhm LWS')


# Function to extract and preprocess required data from the excel file
def extract_data(df):
    coordinates = df['Coordinates'].tolist()
    sensor_id = df['ID_TTS'].tolist()
    #eui = df['EUI'].tolist()
    laubwand = df['Laubwand'].tolist()
    ausbringungsort = df['Ausbringungsort'].tolist()
    df[['Latitude', 'Longitude']] = df['Coordinates'].apply(lambda x: pd.Series(str(x).split(",")))
    df_data = df[['Latitude', 'Longitude']]
    df_meta = df_data.assign(ID_TTS=sensor_id, Laubwand=laubwand, Ausbringungsort=ausbringungsort)
    df_preprocessed = df_meta.dropna()
    df_preprocessed.reset_index(drop=True, inplace=True)
    return df_preprocessed


# passing the data from excel file to extract_data function
df_dragino = extract_data(dragino)
df_deltaohm = extract_data(deltaohm)


# Function to loop over the coordinates of Dragino and Deltaohm and making meteoblue api calls to receive forecast data
def forecasts(df):
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

    # Looping over coordinates and making api calls
    for i in range(len(df)):
        latitude = df.loc[i, 'Latitude']
        longitude = df.loc[i, 'Longitude']
        sensor_id = df.loc[i, 'ID_TTS']
        #eui = df.loc[i, 'EUI']
        laubwand = df.loc[i, 'Laubwand']
        ausbringungsort = df.loc[i, 'Ausbringungsort']
        api_url = "https://my.meteoblue.com/packages/agro-1h_agromodelleafwetness-1h?apikey=946f8e5caba1&lat=" + latitude + "&lon=" + longitude + "&asl=75&format=json&tz=Europe%2FBerlin"
        response = requests.get(api_url).json()
        response['metadata']['name'] = sensor_id
        #response['metadata']['EUI'] = eui
        response['metadata']['Laubwand'] = laubwand
        response['metadata']['Ausbringungsort'] = ausbringungsort
        jsonresponse = json.dumps(response)

        # creating a new json file for each sensor for each run
        with open(sensor_id + ".json", 'w') as f:
            f.write(jsonresponse)

        # opening the json file created earlier to create a parquet file
        with open(sensor_id + ".json", 'r') as f:
            data = json.loads(f.read())

        # normalizing the json to extract only relevant data records using the key data_1h
        df_json = pd.json_normalize(data=data['data_1h'], sep='_')
        df_dict = df_json.to_dict(orient='records')[0]
        df_data = pd.DataFrame(df_dict)

        # extracting metadata and units from json to add custom metadata to parquet file
        metadata = data['metadata']
        units = data['units']
        custom_metadata = metadata | units
        for key, value in custom_metadata.items():
            if not isinstance(value, (str, bytes)):
                custom_metadata[key] = str(value)

        # writing data and metadata to parquet format
        # creating a new parquet file if the file does not exist already
        # if the parquet file exists already then data is appended to the existing file with fastparquet
        if not os.path.isfile(sensor_id + ".parquet"):
            write(sensor_id + ".parquet", df_data, custom_metadata=custom_metadata)
        else:
            write(sensor_id + ".parquet", df_data, custom_metadata=custom_metadata, append=True)

        # adding json files to minio bucket
        if project_bucket:
            client.fput_object("projekt-daten",
                               "UniKo/Egov/WetterDaten/Meteoblue/json-formatted/" + os.path.basename(f.name), f.name)

    # adding parquet files to minio bucket
    extension = "*.parquet"
    parquet_files_list = [f for f in glob.glob(extension)]
    for parquet_file_name in parquet_files_list:
        if project_bucket:
            client.fput_object("projekt-daten",
                               "UniKo/Egov/WetterDaten/Meteoblue/parquet-formatted/" + parquet_file_name, parquet_file_name)

    print("Successful responses uploaded to Minio")


'''
#adding forecasts function to the scheduler that makes api calls each hour
if __name__ == '__main__':

    scheduler = BackgroundScheduler()
    trigger = CronTrigger(
        year="*", month="*", day="*", hour="*", minute="0", second="0"
    )
    scheduler.add_job(forecasts(), trigger=trigger)
    scheduler.start()

    #print("Minio Objects" + str(round(objects_created._value.get(), 9)))

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

'''
forecasts(df_dragino)
forecasts(df_deltaohm)
