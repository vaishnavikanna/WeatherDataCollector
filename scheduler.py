import this
from datetime import datetime
import time
import os
from ftplib import FTP_TLS
from minio import Minio
from prometheus_client import Counter, start_http_server



from apscheduler.schedulers.background import BackgroundScheduler

#Prometheus Counter initialized to monitor the number of objects uploaded to Minio
objects_created = Counter('Objects_created', 'The total number of objects created in Minio')

def ftpdownloadfiles():
    #credentials for ftp login
    HOSTNAME = "dlr-web-ftp1.aspdienste.de"
    USERNAME = "uni_koblenz_iwvi"
    PASSWORD = "Nv30yOXLiA5$5LtM62.6g"
    PORT = 21

    #credentials to connect to Minio
    client = Minio(
        "data.smarter-weinberg.de:9000",
        access_key="J0lSpWY1rNulijB2",
        secret_key="qhjjTwA56aRcIDQvTorT131pYrWtXzme",
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

    #checking for projekt-daten bucket in Minio
    project_bucket = client.bucket_exists("projekt-daten")
    if not project_bucket:
        print("Bucket 'projekt-daten' does not exist")
    else:
        print("Bucket 'projekt-daten' already exists")

    #Every file retrieved from ftp is uploaded to the projekt-daten bucket in Minio and is placed under DLR/Wetterdaten folder
    # after which the files are deleted from ftp
    for filename in filenames:
        client.fput_object(
                "projekt-daten", "DLR/Wetterdaten/" + filename,
                filename
            )

        ftp_server.delete(filename)


    #closing connection to ftp server
    ftp_server.quit()

    print("Files uploaded to projekt-daten/DLR/Wetterdaten at %s" % datetime.now())

#Starting prometheus http server
start_http_server(8000)

#Adding the ftpdownloadfiles function to the scheduler that runs every 5 seconds for the purpose of testing
if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(ftpdownloadfiles, 'interval', seconds=5)
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



