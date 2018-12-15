import sys
import ftplib
import os
import time

while True:
    server = "ftpreonenergy.qosenergy.com"
    user = "reonenergy"
    password = "@Reonenergy92"
    source = "/ALUCAN/ZL3"
    destination = "/home/reon/ftp/files/ALUCAN"
    interval = 0.05

    ftp = ftplib.FTP_TLS(host='ftpreonenergy.qosenergy.com',
                         passwd="@Reonenergy92",
                         user="reonenergy")

    try:
        ftp.cwd("/ALUCAN/ZL3")
    except OSError:
        pass
    except ftplib.error_perm:
        print("Error: could not change to " + path)
        sys.exit("Ending Application")
    filelist = ftp.nlst()
    for file in filelist:
        print(file)
        ftp.retrbinary("RETR " + file, open(os.path.join(destination, file), "wb").write)
        print("Downloaded: " + file)
        ftp.delete(file)
    ftp.close()
    time.sleep(900)