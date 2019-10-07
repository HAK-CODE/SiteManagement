'''
author: HAK
time  : 03:00 AM, 07/11/2017
'''

#import json
import sys
import time
import os
#from Config import predixConnection
#import datetime
from fileRelease import IOoperation
#import requests
#import ftpService
#import shutil
import ast
from colorama import Fore
from ElasticSearchService_v1Prod import ElasticSearchService as es
#import pandas as pd
import ntpath
import s3service
#import numpy as np

ioCheck = IOoperation()
objectRecieved = ast.literal_eval(sys.argv[1])
ioCheck.setFile(objectRecieved['fileReceived'])

if ioCheck.isFileRelease():
    if os.path.getsize(objectRecieved['fileReceived']) == 0:
        print('0 BYTE FILE.')
        # ftpObj = ftpService.FTP(filePath=objectRecieved['fileReceived'],
        #                         serverPath=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
        # ftpObj.sendFTP()
        sys.exit(0)
del ioCheck

if objectRecieved['db']['siteConfig']['siteInfo']['storeFiles']:
    s3 = s3service.S3(key=objectRecieved['fileReceived'],
                      path=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
    s3.send()
    #shutil.copy(objectRecieved['fileReceived'], objectRecieved['db']['siteConfig']['siteInfo']['siteFilesStorage'])
    #time.sleep(1)

os.remove(objectRecieved['fileReceived'])