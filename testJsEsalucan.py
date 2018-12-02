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
from colorama import Fore

ioCheck = IOoperation()
objectRecieved = ast.literal_eval(sys.argv[1])
ioCheck.setFile(objectRecieved['fileReceived'])

if ioCheck.isFileRelease():
    if os.path.getsize(objectRecieved['fileReceived']) == 0:
        print('0 BYTE FILE.')
        ftpObj = ftpService.FTP(filePath=objectRecieved['fileReceived'],
                                serverPath=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
        ftpObj.sendFTP()
        sys.exit(0)
del ioCheck

if objectRecieved['db']['siteConfig']['siteInfo']['storeFiles']:
    shutil.copy(objectRecieved['fileReceived'], objectRecieved['db']['siteConfig']['siteInfo']['siteFilesStorage'])


def dictionaryBuilder(key, v):
    if objectRecieved['db']['siteConfig']['js'][key]['applyChecks']:
        if objectRecieved['db']['siteConfig']['js'][key]['minCheckApply']:
            v = 0 if v < objectRecieved['db']['siteConfig']['js'][key]['min'] else v
        if objectRecieved['db']['siteConfig']['js'][key]['maxCheckApply']:
            v = 0 if v > objectRecieved['db']['siteConfig']['js'][key]['max'] else v
    if objectRecieved['db']['siteConfig']['js'][key]['applyOperation']:
        v = (v / objectRecieved['db']['siteConfig']['js'][key]['multiplier']) + objectRecieved['db']['siteConfig']['js'][key]['offset']

    return v

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
            if os.path.basename(objectRecieved['fileReceived']).startswith('INVERTER') or os.path.basename(objectRecieved['fileReceived']).startswith('METER') or os.path.basename(objectRecieved['fileReceived']).startswith('SENSOR'):
                index = {"index": {"_index": "rb", "_id": None}}
                buffer = ""
                data = json.load(open(objectRecieved['fileReceived'], encoding='ISO-8859-1', mode='r'))
                dictionary = {x: None for x in objectRecieved['db']['siteConfig']['js']['jsCols']}
                if os.path.basename(objectRecieved['fileReceived']).startswith('INVERTER'):
                    try:
                        dictionary.__delitem__('0')
                        dictionary.__delitem__('1')
                        dictionary.__delitem__('2')
                        dictionary.__delitem__('4')
                        dictionary.__delitem__('EnergyReal_WAC_Sum_Consumed')
                        dictionary.__delitem__('PowerReal_P_Sum')
                    except:
                        print("key not exist")
                    for key, value in dictionary.items():
                        if key in data['Body']:
                            dictionary[key] = 0
                            for k, v in data['Body'][key]['Values'].items():
                                dictionary[key] += dictionaryBuilder(key, v)
                    dictionary['type'] = "inverter"

                elif os.path.basename(objectRecieved['fileReceived']).startswith('METER'):
                    try:
                        dictionary.__delitem__('DAY_ENERGY')
                        dictionary.__delitem__('TOTAL_ENERGY')
                        dictionary.__delitem__('PAC')
                        dictionary.__delitem__('YEAR_ENERGY')
                        dictionary.__delitem__('0')
                        dictionary.__delitem__('1')
                        dictionary.__delitem__('2')
                        dictionary.__delitem__('4')
                    except:
                        print("key not exist")
                    for key, value in dictionary.items():
                        if key in data['Body']['0']:
                            dictionary[key] = 0
                            v = data['Body']['0'][key]
                            dictionary[key] += dictionaryBuilder(key, v)
                    dictionary['type'] = "sensor"

                elif os.path.basename(objectRecieved['fileReceived']).startswith('SENSOR'):
                    try:
                        dictionary.__delitem__('DAY_ENERGY')
                        dictionary.__delitem__('TOTAL_ENERGY')
                        dictionary.__delitem__('PAC')
                        dictionary.__delitem__('YEAR_ENERGY')
                        dictionary.__delitem__('EnergyReal_WAC_Sum_Consumed')
                        dictionary.__delitem__('PowerReal_P_Sum')
                    except:
                        print("key not exist")
                    for key, value in dictionary.items():
                        if key in data['Body']['1']:
                            dictionary[key] = 0
                            for k, v in data['Body']['1'][key].items():
                                if 'Value' in k:
                                    dictionary[key] += dictionaryBuilder(key, v)
                    dictionary['type'] = "meter"

                dictionary['Timestamp'] = data['Head']['Timestamp']
                print(dictionary)
                buffDict = dictionary.copy()
                del dictionary['type']
                buffDict['@timestamp'] = buffDict['Timestamp']
                del buffDict['Timestamp']
                type = buffDict['type']
                del buffDict['type']
                index["index"]["_id"] = str(buffDict['@timestamp'])
                buffer += str(json.dumps(index)+"\n")
                buffer += str(json.dumps({"@timestamp": buffDict["@timestamp"], "type":type, "data":buffDict})+"\n")
                a = requests.put(url='https://search-reon-yf6s4jcgv6tapjin4xblwtgk6y.us-east-2.es.amazonaws.com/alucan/_doc/_bulk',
                             headers={"content-type": "application/json"},
                             data=buffer)
                print(a.status_code)
                CheckOldData()
                timeStamp = dictionary['Timestamp'].replace('T', ' ')
                timeStamp = timeStamp.replace('+05:00', '')
                unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S").timetuple()))
                unixTimeStamp = unixTimeStamp * 1000
                dictionary.__delitem__('Timestamp')
                try:
                    for k, v in dictionary.items():
                        if v is not None:
                            predixConnection.timeSeries.queue(objectRecieved['db']['siteConfig']['js'][k]['tag'],
                                                              value=str(v),
                                                              timestamp=unixTimeStamp,
                                                              quality=3)
                            print(Fore.YELLOW + objectRecieved['db']['siteConfig']['js'][k]['tag'] + Fore.RESET)
                    print(Fore.GREEN + predixConnection.timeSeries.send() + Fore.RESET)
                except Exception:
                    print("No internet")
                    with open("DefaultDataStore/Default_Store.csv", "a") as file:
                        file.write(objectRecieved['db']['siteConfig']['js'][k]['tag'] + ";" + str(v) + ";" + str(
                            unixTimeStamp * 1000) + "\n")

                if requests.get('https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/clearcache',
                                headers={'x-api-key': 'gMhamr1lYt8KEy1F0rlRd5EJq8hyjJ7s6qIPKTTv'}).status_code == 200:
                    print('cache cleared')
                else:
                    print('cache cleared failed')

        ftpObj = ftpService.FTP(filePath=objectRecieved['fileReceived'],
                                serverPath=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
        ftpObj.sendFTP()
os.remove(objectRecieved['fileReceived'])