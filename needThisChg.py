'''
author: HAK
time  : 03:00 AM, 07/11/2017
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
from ElasticSearchService import ElasticSearchService as es
import pandas as pd
import ntpath
import s3service

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
        if os.path.splitext(objectRecieved['fileReceived'])[-1] and (len(objectRecieved['db']['siteConfig']['js']) or len(objectRecieved['db']['siteConfig']['csv'])) != 0:
            if '.js' in ntpath.basename(objectRecieved['fileReceived']):
                es = es(index=objectRecieved['db']['siteConfig']['siteInfo']['siteTag'])
                if os.path.basename(objectRecieved['fileReceived']).startswith('INVERTER') or os.path.basename(objectRecieved['fileReceived']).startswith('METER') or os.path.basename(objectRecieved['fileReceived']).startswith('SENSOR'):
                    data = json.load(open(objectRecieved['fileReceived'], encoding='ISO-8859-1', mode='r'))
                    dictionary = {x: None for x in objectRecieved['db']['siteConfig']['js']['jsCols']}
                    if os.path.basename(objectRecieved['fileReceived']).startswith('INVERTER'):
                        for key, value in dictionary.items():
                            if key in data['Body']:
                                dictionary[key] = 0
                                print("length of data "+str(len(data['Body'][key]['Values'].keys())))
                                print("inverter numbers "+str(objectRecieved['db']['siteConfig']['siteInfo']['siteInverterQuantity']))
                                if len(data['Body'][key]['Values'].keys()) == objectRecieved['db']['siteConfig']['siteInfo']['siteInverterQuantity']:
                                    print("now calculating inverter.")
                                    for k, v in data['Body'][key]['Values'].items():
                                        dictionary[key] += dictionaryBuilder(key, v)
                        dictionary['type'] = "inverter"

                    elif os.path.basename(objectRecieved['fileReceived']).startswith('METER'):
                        for key, value in dictionary.items():
                            if key in data['Body']['0']:
                                dictionary[key] = 0
                                v = data['Body']['0'][key]
                                dictionary[key] += dictionaryBuilder(key, v)
                        dictionary['type'] = "meter"

                    elif os.path.basename(objectRecieved['fileReceived']).startswith('SENSOR'):
                        for key, value in dictionary.items():
                            if key in data['Body']['1']:
                                dictionary[key] = 0
                                for k, v in data['Body']['1'][key].items():
                                    if 'Value' in k:
                                        dictionary[key] += dictionaryBuilder(key, v)
                        dictionary['type'] = "sensor"

                    dictionary['Timestamp'] = data['Head']['Timestamp']
                    dictionary = {k: v for k, v in dictionary.items() if v is not None}        # remove key with None values.
                    buffDict = dictionary.copy()
                    del dictionary['type']
                    buffDict['@timestamp'] = buffDict['Timestamp']
                    del buffDict['Timestamp']
                    type = buffDict['type']
                    del buffDict['type']
                    es.loadData({"@timestamp": buffDict["@timestamp"], "type":type, type:buffDict})

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
                            file.write(objectRecieved['db']['siteConfig']['js'][k]['tag'] + ";" + str(v) + ";" + str(unixTimeStamp * 1000) + "\n")

            elif '.csv' in ntpath.basename(objectRecieved['fileReceived']):
                es = es(index=objectRecieved['db']['siteConfig']['siteInfo']['siteTag'], isMultipleTS=True)
                df = pd.read_csv(objectRecieved['fileReceived'],
                                 sep='\s*,\s*',
                                 header=0,
                                 engine='python')
                df = df.loc[:, ~df.columns.str.replace("(\.\d+)$", "").duplicated()]

                col = objectRecieved['db']['siteConfig']['csv']['csvCols']
                missing = []
                for i, j in enumerate(col):
                    if col[i] not in df.columns:
                        missing.append(col[i])
                col = list(filter(lambda x: x not in missing, col))
                print(col)
                df_final = df[col].copy()

                df['TIMESTAMP'] = pd.to_datetime(df.TIMESTAMP, format='%d/%m/%Y %H:%M:%S')
                df['TIMESTAMP'] = df['TIMESTAMP'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:00')
                df.set_index('TIMESTAMP', inplace=True)
                save = []
                for index, j in df.iterrows():
                    builder = {}
                    innerDict = {}
                    builder['@timestamp'] = index
                    builder['type'] = "logger"
                    for k, v in j.iteritems():
                        if type(v) is int or type(v) is float or v is None:
                            innerDict[k] = v
                    builder['logger'] = innerDict
                    save.append(builder)
                es.loadData(save)

                df_final.set_index('TIMESTAMP', inplace=True)
                print(df_final)

                df_final.dropna(inplace=True)
                receiveTime = time.ctime(os.path.getctime(objectRecieved['fileReceived']))
                filename = ntpath.basename(objectRecieved['fileReceived'])
                col.remove('TIMESTAMP')
                for keys in col:
                    if objectRecieved['db']['siteConfig']['csv'][keys]['applyChecks']:
                        if objectRecieved['db']['siteConfig']['csv'][keys]['minCheckApply']:
                            df_final[keys].loc[
                                df_final[keys] < objectRecieved['db']['siteConfig']['csv'][keys]['min']] = None
                        if objectRecieved['db']['siteConfig']['csv'][keys]['minCheckApply']:
                            df_final[keys].loc[
                                df_final[keys] > objectRecieved['db']['siteConfig']['csv'][keys]['max']] = None
                    if objectRecieved['db']['siteConfig']['csv'][keys]['applyOperation']:
                        df_final[keys] = (df_final[keys] / objectRecieved['db']['siteConfig']['csv'][keys]['multiplier']) + \
                                         objectRecieved['db']['siteConfig']['csv'][keys]['offset']

                CheckOldData()

                timeStamp = df_final.index.astype(str).str.replace('/', '-')
                print(df_final)
                tagsShow = []

                for i in range(len(df_final.index)):
                    unixTimeStamp = int(
                        time.mktime(datetime.datetime.strptime(timeStamp[i], "%d-%m-%Y %H:%M:%S").timetuple()))
                    unixTimeStamp = unixTimeStamp * 1000
                    for j in df_final.columns:
                        try:
                            predixConnection.timeSeries.queue(objectRecieved['db']['siteConfig']['csv'][j]['tag'],
                                                              value=str(df_final.iloc[i][j]),
                                                              timestamp=unixTimeStamp,
                                                              quality=3)
                            tagsShow.append(objectRecieved['db']['siteConfig']['csv'][j]['tag'])
                        except Exception:
                            print("No internet")
                            with open("DefaultDataStore/Default_Store.csv", "a") as file:
                                file.write(objectRecieved['db']['siteConfig']['csv'][j]['tag'] + ";" + str(
                                    df_final.iloc[i, j]) + ";" + str(unixTimeStamp * 1000) + "\n")
                                print(df_final.iloc[i, j], objectRecieved['db']['siteConfig']['csv'][j]['tag'])
                print(Fore.YELLOW)
                print(df_final.columns)
                print(Fore.RESET)
                print(Fore.GREEN + predixConnection.timeSeries.send() + Fore.RESET)

        if requests.get('https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/clearcache', headers={'x-api-key': 'gMhamr1lYt8KEy1F0rlRd5EJq8hyjJ7s6qIPKTTv'}).status_code == 200:
            print('cache cleared')
        else:
            print('cache cleared failed')

        ftpObj = ftpService.FTP(filePath=objectRecieved['fileReceived'],
                                serverPath=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
        ftpObj.sendFTP()

        s3 = s3service.S3(key=objectRecieved['fileReceived'], path=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
        s3.send()

os.remove(objectRecieved['fileReceived'])