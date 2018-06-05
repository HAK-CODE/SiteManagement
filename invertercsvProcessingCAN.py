'''
author: HAK
time  : 02:00 AM, 07/11/2017
'''

import json
import sys
import time
import os
from Config import predixConnection
import datetime
from fileRelease import IOoperation
import requests
import ftpService
import shutil
import ast

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


def CheckOldData():
    try:
        with open("DefaultDataStore/Default_Store.csv", "r") as file:
            lines = file.readlines()
        for i in lines:
            data = i.split(";")
            predixConnection.timeSeries.queue(data[0], value=data[1], timestamp=data[2].replace('\n', ''))
            predixConnection.timeSeries.send()
            print(data)
        os.remove("DefaultDataStore/Default_Store.csv")
    except Exception:
        print("No Internet :(")
        print("Old Data Not Found! :)")


if os.path.getsize(objectRecieved['fileReceived']) != 0:
    if objectRecieved['db']['siteConfig']['siteInfo']['siteDeployed'] is True:
        if os.path.splitext(objectRecieved['fileReceived'])[-1] and len(objectRecieved['db']['siteConfig']['js']) != 0:
            if os.path.basename(objectRecieved['fileReceived']).startswith('INVERTER') or os.path.basename(objectRecieved['fileReceived']).startswith('METER'):
                data = json.load(open(objectRecieved['fileReceived'], mode='r'))
                dictionary = {x: None for x in objectRecieved['db']['siteConfig']['js']['jsCols']}
                if os.path.basename(objectRecieved['fileReceived']).startswith('INVERTER'):
                    dictionary.__delitem__('EnergyReal_WAC_Sum_Consumed')
                    dictionary.__delitem__('PowerReal_P_Sum')
                    for key, value in dictionary.items():
                        if key in data['Body']:
                            dictionary[key] = 0
                            for k, v in data['Body'][key]['Values'].items():
                                if objectRecieved['db']['siteConfig']['js'][key]['applyChecks']:
                                    if objectRecieved['db']['siteConfig']['js'][key]['minCheckApply']:
                                        v = 0 if v < objectRecieved['db']['siteConfig']['js'][key]['min'] else v
                                    if objectRecieved['db']['siteConfig']['js'][key]['maxCheckApply']:
                                        v = 0 if v > objectRecieved['db']['siteConfig']['js'][key]['max'] else v
                                if objectRecieved['db']['siteConfig']['js'][key]['applyOperation']:
                                    v = (v / objectRecieved['db']['siteConfig']['js'][key]['multiplier']) + \
                                        objectRecieved['db']['siteConfig']['js'][key]['offset']
                                dictionary[key] += v

                elif os.path.basename(objectRecieved['fileReceived']).startswith('METER'):
                    dictionary.__delitem__('DAY_ENERGY')
                    dictionary.__delitem__('TOTAL_ENERGY')
                    dictionary.__delitem__('PAC')
                    dictionary.__delitem__('YEAR_ENERGY')
                    for key, value in dictionary.items():
                        if key in data['Body']['0']:
                            dictionary[key] = 0
                            v = data['Body']['0'][key]
                            if objectRecieved['db']['siteConfig']['js'][key]['applyChecks']:
                                if objectRecieved['db']['siteConfig']['js'][key]['minCheckApply']:
                                    v = 0 if v < objectRecieved['db']['siteConfig']['js'][key]['min'] else v
                                if objectRecieved['db']['siteConfig']['js'][key]['maxCheckApply']:
                                    v = 0 if v > objectRecieved['db']['siteConfig']['js'][key]['max'] else v
                            if objectRecieved['db']['siteConfig']['js'][key]['applyOperation']:
                                v = (v / objectRecieved['db']['siteConfig']['js'][key]['multiplier']) + objectRecieved['db']['siteConfig']['js'][key]['offset']
                            dictionary[key] += v

                dictionary['Timestamp'] = data['Head']['Timestamp']

                CheckOldData()

                timeStamp = dictionary['Timestamp'].replace('T', ' ')
                timeStamp = timeStamp.replace('+05:00', '')
                unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S").timetuple()))
                unixTimeStamp = unixTimeStamp * 1000
                dictionary.__delitem__('Timestamp')

                try:
                    for k, v in dictionary.items():
                        if v is not None:
                            print(v)
                            '''
                            predixConnection.timeSeries.queue(objectRecieved['db']['siteConfig']['js'][k]['tag'],
                                                              value=str(v),
                                                              timestamp=unixTimeStamp,
                                                              quality=3)
                            a = predixConnection.timeSeries.send()
                            print(a)
                            '''
                except Exception:
                    print("No internet")
                    with open("DefaultDataStore/Default_Store.csv", "a") as file:
                        file.write(objectRecieved['db']['siteConfig']['js'][k]['tag'] + ";" + str(v) + ";" + str(unixTimeStamp * 1000) + "\n")

                if requests.get('https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/clearcache',
                                headers={'x-api-key': 'gMhamr1lYt8KEy1F0rlRd5EJq8hyjJ7s6qIPKTTv'}).status_code == 200:
                    print('cache cleared')
                else:
                    print('cache cleared failed')

        ftpObj = ftpService.FTP(filePath=objectRecieved['fileReceived'],
                                serverPath=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
        ftpObj.sendFTP()
        
os.remove(objectRecieved['fileReceived'])
