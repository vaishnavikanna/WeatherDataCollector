from datetime import datetime
import time
import os
from ftplib import FTP_TLS
from io import BytesIO
from dotenv.main import load_dotenv

from minio import Minio
from prometheus_client import Counter, start_http_server

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

#Prometheus Counter initialized to monitor the number of objects uploaded to Minio
objects_created = Counter('Objects_created', 'The total number of objects created in Minio')

#loading environment variables
load_dotenv()

def ftpdownloadfiles():
    #credentials for ftp login
    HOSTNAME = "dlr-web-ftp1.aspdienste.de"
    USERNAME = os.environ['FTP_USERNAME']
    PASSWORD = os.environ['FTP_PASSWORD']
    PORT = 21

    #credentials to connect to Minio
    client = Minio(
        "data.smarter-weinberg.de:9000",
        access_key= os.environ['MINIO_ACCESS_KEY'],
        secret_key= os.environ['MINIO_SECRET_KEY'],
        secure=False
    )
    ftp_server = FTP_TLS(host=HOSTNAME)
    # Connect FTP Server
    ftp_server.connect(host=HOSTNAME, port=PORT)
    ftp_server.login(user=USERNAME, passwd=PASSWORD)
    ftp_server.prot_p()
    ftp_server.encoding = "utf-8"

    #path in ftp server from where the files are retrieved
    path = 'out/wetterdaten/10min/'
    ftp_server.cwd(path)
    filenames = ftp_server.nlst()
    print('Files retrieved at: %s' % datetime.now())

    #Checking if project bucket exists in minio
    project_bucket = client.bucket_exists("projekt-daten")
    if not project_bucket:
        print("Bucket 'projekt-daten' does not exist")
    else:
        print("Bucket 'projekt-daten' already exists")

    #Retrieving the byte stream of the files from ftp server and uploading the bytestream to Minio in the path projekt-data/DLR/Wetterdaten
    for filename in filenames:
        file_bytes = BytesIO()
        ftp_server.retrbinary("RETR "+ filename, file_bytes.write)
        file_bytes.seek(0)
        if project_bucket:
            client.put_object("projekt-daten", "DLR/Wetterdaten/" + filename, file_bytes, length = len(file_bytes.getbuffer()))
        # Deleting files in ftp after upload to minio
        ftp_server.delete(filename)


    #closing connection to ftp server
    ftp_server.quit()

    print("Files uploaded to projekt-daten/DLR/Wetterdaten at %s" % datetime.now())

#Starting prometheus http server
start_http_server(8000)

#Adding the ftpdownloadfiles function to the scheduler that runs every one hour at the start of the hour
if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    trigger = CronTrigger(
        year="*", month="*", day="*", hour="*", minute="0", second="0"
    )
    scheduler.add_job(ftpdownloadfiles, trigger=trigger)
    scheduler.start()
    #incrmenting prometheus counter
    objects_created.inc()
    print("Minio Objects" + str(round(objects_created._value.get(), 9)))

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()



