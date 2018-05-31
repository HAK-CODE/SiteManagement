'''
author: HAK
time  : 02:00 AM, 07/11/2017
'''

import json
import pandas as pd
import sys
import time
import os
#from Config import predixConnection
from colorama import Fore
import datetime
from pandas.io.json import json_normalize
from fileRelease import IOoperation
import requests
import ftpService
import shutil
import ast

ioCheck = IOoperation()
objectRecieved = ast.literal_eval(sys.argv[1])
ioCheck.setFile(objectRecieved['fileReceived'])


ioCheck = IOoperation()
objectRecieved = ast.literal_eval(sys.argv[1])
ioCheck.setFile(objectRecieved['fileReceived'])


if ioCheck.isFileRelease():
    if os.path.getsize(objectRecieved['fileReceived']) == 0:
        print('0 BYTE FILE.')
        sys.exit(0)
del ioCheck

if objectRecieved['db']['siteConfig']['siteInfo']['storeFiles']:
    shutil.copy(objectRecieved['fileReceived'], objectRecieved['db']['siteConfig']['siteInfo']['siteFilesStorage'])

if os.path.getsize(objectRecieved['fileReceived']) != 0:
    if objectRecieved['db']['siteConfig']['siteInfo']['siteDeployed'] is True:
        if os.path.splitext(objectRecieved['fileReceived'])[-1] and len(objectRecieved['db']['siteConfig']['js']) != 0:
            data = json.load(open(objectRecieved['fileReceived'], mode='r'))
            dictionary = {x: 0 for x in objectRecieved['db']['siteConfig']['js']['jsCols']}

            for key, value in dictionary.items():
                point = data['Body'][key]['Values']
                for k, v in point.items():
                    if objectRecieved['db']['siteConfig']['js'][key]['applyChecks']:
                        if objectRecieved['db']['siteConfig']['js'][key]['minCheckApply']:
                            v = 0 if v < objectRecieved['db']['siteConfig']['js'][key]['min'] else v
"""

data = json.load(open(PATH_OF_JSON_FILE, mode='r'))
keys = {'DAY_ENERGY': 0, 'PAC': 0, 'TOTAL_ENERGY': 0, 'YEAR_ENERGY': 0}


for i in keys:
	json_data = (data['Body'][i]['Values'])
	for key, value in json_data.items():
		keys[i] += value
keys['Timestamp'] = data['Head']['Timestamp']
df1 = pd.DataFrame.from_dict(json_normalize(keys), orient='columns')
df1.set_index('Timestamp',inplace=True)




with open("Config/Tags_CAN.csv", "r") as file:
    tag = file.readlines()

k=0
for index, i in df1.iterrows():
    timeStamp = index.replace('T', ' ')
    timeStamp = timeStamp.replace('+05:00', '')
    unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S").timetuple()))
    unixTimeStamp=((unixTimeStamp * 1000))
    for j in range(len(df1.columns)):
        tag[k] = tag[k].replace("\n" , "")
        try:
            predixConnection.timeSeries.queue(tag[k], value=str(df1.iloc[0, j]), timestamp=unixTimeStamp , quality=3)
            a = predixConnection.timeSeries.send()
            print(a)
            print(tag[k], str(df1.iloc[0, j]))
        except Exception:
            print("No internet")
            with open("DefaultDataStore/Default_Store.csv", "a") as file:
                file.write(tag[k] + ";" + str(df1.iloc[0 , j]) + ";" + str(unixTimeStamp * 1000) + "\n")
                print(df1.iloc[0, j], tag[k])
        k=k+1
"""