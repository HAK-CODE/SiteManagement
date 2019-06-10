import sys
import ftplib
import os
import time
import boto3
import ntpath
bucketName = "reon-historian"
s3 = boto3.client('s3')

while True:
    server = "ftpreonenergy.qosenergy.com"
    user = "reonenergy"
    password = "@Reonenergy92"
    source = "/ALUCAN/ZL3"
    destination = "/home/ubuntu/ALUCAN"
    interval = 0.05

    ftp = ftplib.FTP_TLS(host='ftpreonenergy.qosenergy.com',
                         passwd="@Reonenergy92",
                         user="reonenergy")

    try:
        ftp.cwd("/qos_archives/ALUCAN")
    except OSError:
        pass
    except ftplib.error_perm:
        print("Error: could not change to ")
        sys.exit("Ending Application")
    filelist = ftp.nlst()
    for file in filelist:
        if "log" in file and ".csv" in file:
            ftp.retrbinary("RETR " + file, open(os.path.join(destination, file), "wb").write)
            response = s3.upload_file(destination+"/"+file, bucketName, "ALUCAN/" + "MIGRATION" + "/" + file)
            print(response)
        # ftp.delete(file)
    ftp.close()