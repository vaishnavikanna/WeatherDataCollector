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
from datetime import datetime
import pyarrow as pa
import pyarrow.json as pj
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time

# loading environment variables
load_dotenv()

# Reading the excel file using pandas to extract coordinates of dragino and deltaohm
filename = 'InventarStatischeSensoren.xlsx'
df_coordinates = pd.read_excel(filename, sheet_name='All_Coordinates')


# Function to extract and preprocess required data from the excel file
def extract_data(df):
    ausbringungsort = df['Ausbringungsort'].tolist()
    heights = df['Heights'].tolist()
    df[['Latitude', 'Longitude']] = df['Coordinates'].apply(lambda x: pd.Series(str(x).split(",")))
    df_data = df[['Latitude', 'Longitude']]
    df_meta = df_data.assign(Ausbringungsort=ausbringungsort, Heights=heights)
    df_preprocessed = df_meta.dropna()
    df_preprocessed.reset_index(drop=True, inplace=True)
    print(ausbringungsort)
    return df_preprocessed


# passing the data from excel file to extract_data function
df_response = extract_data(df_coordinates)


# Function to loop over the coordinates of Dragino and Deltaohm and making meteoblue api calls to receive forecast data
def forecasts(df):
    # connecting to minio
    client = Minio(
        "data.smarter-weinberg.de:9000",
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
        secure=False
    )
    current_time = datetime.now().strftime("%Y%m%d-%H%M%S")
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
        height = df.loc[i, 'Heights']
        ausbringungsort = df.loc[i, 'Ausbringungsort']
        api_url = "https://my.meteoblue.com/packages/agro-1h_agromodelleafwetness-1h?apikey=##############&lat=" + latitude + "&lon=" + longitude + "&asl=" + str(height) + "&format=json&tz=Europe%2FBerlin"
        response = requests.get(api_url).json()
        response['metadata']['height'] = str(height)
        response['metadata']['Ausbringungsort'] = ausbringungsort
        jsonresponse = json.dumps(response)

        # creating a new json file for each sensor for each run
        with open(str(ausbringungsort) + "_" + current_time + ".json", 'w') as f:
            f.write(jsonresponse)

        # opening the json file created earlier to create a parquet file
        with open(str(ausbringungsort) + "_" + current_time + ".json", 'r') as f:
            data = json.loads(f.read())

        # normalizing the json to extract only relevant data records using the key data_1h
        df_json = pd.json_normalize(data=data['data_1h'], sep='_') #malformed json, no response, no upload, connection issues
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
        if not os.path.isfile(str(ausbringungsort) + ".parquet"):
            write(str(ausbringungsort) + ".parquet", df_data, custom_metadata=custom_metadata)
        else:
            write(str(ausbringungsort) + ".parquet", df_data, custom_metadata=custom_metadata, append=True)

        # adding json files to minio bucket
        if project_bucket:
            client.fput_object("projekt-daten", "UniKo/Egov/WetterDaten/Meteoblue/json-formatted/" + os.path.basename(f.name), f.name)

    # adding parquet files to minio bucket
    parquet_extension = "*.parquet"
    parquet_files_list = [f for f in glob.glob(parquet_extension)]
    for parquet_file_name in parquet_files_list:
        if project_bucket:
            client.fput_object("projekt-daten", "UniKo/Egov/WetterDaten/Meteoblue/parquet-formatted/" + parquet_file_name, parquet_file_name)

    print("Successful responses uploaded to Minio")

    #Deleting json and parquet files from local
    json_extension = "*.json"
    json_files_list = [f for f in glob.glob(json_extension)]

    for json_filename in json_files_list:
        if os.path.exists(json_filename):
            os.remove(json_filename)
        else:
            print("File not found")

    for parquet_filename in parquet_files_list:
        if os.path.exists(parquet_filename):
            os.remove(parquet_filename)
        else:
            print("File not found")
    print("Json and Parquet response files removed from local")



'''
#adding forecasts function to the scheduler that makes api calls each hour
if __name__ == '__main__':

    scheduler = BackgroundScheduler()
    trigger = CronTrigger(
        year="*", month="*", day="*", hour="*", minute="0", second="0"
    )
    scheduler.add_job(forecasts, args=[df_response], trigger=trigger)
    scheduler.start()

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
'''

forecasts(df_response)

