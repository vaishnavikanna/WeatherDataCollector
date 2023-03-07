from ftplib import FTP_TLS



HOSTNAME = "dlr-web-ftp1.aspdienste.de"
USERNAME = "uni_koblenz_iwvi"
PASSWORD = "Nv30yOXLiA5$5LtM62.6g"
PORT = 21
ftp_server = FTP_TLS(host=HOSTNAME)
# Connect FTP Server
ftp_server.connect(host=HOSTNAME, port=PORT)
ftp_server.login(user=USERNAME, passwd=PASSWORD)
ftp_server.prot_p()
# force UTF-8 encoding
ftp_server.encoding = "utf-8"
path = 'out/wetterdaten/10min/'

ftp_server.cwd(path)
filenames = ftp_server.nlst()
print(filenames)
for filename in filenames:
    file = open(filename, 'wb')
    ftp_server.retrbinary("RETR " + filename, file.write)
    file.close()
ftp_server.quit()
