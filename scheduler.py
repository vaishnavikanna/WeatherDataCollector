from datetime import datetime
import os
import time
from dotenv.main import load_dotenv
from minio import Minio
from prometheus_client import Counter, start_http_server
from ftpretty import ftpretty
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


#loading environment variables
load_dotenv()
# Starting prometheus http server
#start_http_server(8000)
# Prometheus Counter initialized to monitor the number of objects uploaded to Minio
#objects_created = Counter('Objects_created', 'The total number of objects created in Minio')

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

    #Using ftpretty to connect to ftp server
    ftp_server = ftpretty(host=HOSTNAME, user=USERNAME, password=PASSWORD, secure=True, timeout=300, port=PORT)
    ftp_server.prot_p()
    ftp_server.encoding = "utf-8"

    #path in ftp server from where the files are retrieved
    path = 'out/wetterdaten/10min/'

    #switching to the specified path in ftp
    ftp_server.cd(path)

    #list of files in the specified path
    filenames = ftp_server.list()

    print('Files retrieved at: %s' % datetime.now())

    #Checking if project bucket exists in minio
    project_bucket = client.bucket_exists("projekt-daten")
    if not project_bucket:
        print("Bucket 'projekt-daten' does not exist")
    else:
        print("Bucket 'projekt-daten' already exists")

    #Retrieving the list of files in the specified path from ftp and uploading to Minio
    for filename in filenames:
        #establishing a new conection to retrieve each file from ftp
        ftp_server = ftpretty(host=HOSTNAME, user=USERNAME, password=PASSWORD, secure=True, timeout=300, port=PORT)
        ftp_server.prot_p()
        ftp_server.encoding = "utf-8"
        ftp_server.cd(path)

        #creating a local copy to save the retrieved file from ftp
        local_copy = open(filename, 'wb')

        #Reattempting three times in case of EOF exception
        for i in range(3):
            try:
                #retriveing the file from ftp and writing to local copy
                ftp_server.get(filename, local_copy)
                #objects_created.inc()
                #uploading the retrieved file to Minio
                if project_bucket:
                    client.fput_object("projekt-daten", "DLR/Wetterdaten/" + filename, filename)
                break
            #catching EOF exception and reattempting
            except EOFError:
                print(f"EOFError occurred, trying again(attempt {i+1})")

        #closing each file
        local_copy.close()

    #Deleting downloaded files from local and from ftp server
    for file in filenames:
        if os.path.exists(file):
            os.remove(file)
        else:
            print("File not found")
        ftp_server.delete(file)

    print("Files successfully deleted from local disk and remote ftp server")


    #closing connection to ftp server
    ftp_server.close()

    print("Files uploaded to projekt-daten/DLR/Wetterdaten at %s" % datetime.now())



#Adding the ftpdownloadfiles function to the scheduler that runs every one hour at the start of the hour
if __name__ == '__main__':

    scheduler = BackgroundScheduler()
    trigger = CronTrigger(
        year="*", month="*", day="*", hour="*", minute="0", second="0"
    )
    scheduler.add_job(ftpdownloadfiles, trigger=trigger)
    scheduler.start()

    #print("Minio Objects" + str(round(objects_created._value.get(), 9)))

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()



