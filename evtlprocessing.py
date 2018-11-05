'''
author: HAK
time  : 11:00 PM, 27/10/2018
'''

from Config import predixConnection
import pandas as pd
import sys
import time
import datetime
import json
import os
import shutil
import ntpath
import ast
from fileRelease import IOoperation
import requests
import ftpService
from colorama import Fore

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
        if os.path.splitext(objectRecieved['fileReceived'])[-1] and (len(objectRecieved['db']['siteConfig']['csv']) or len(objectRecieved['db']['siteConfig']['js'])) != 0:
            print('Received is ', str(objectRecieved['fileReceived']))
            if '.csv' in ntpath.basename(objectRecieved['fileReceived']):
                df = pd.read_csv(objectRecieved['fileReceived'],
                                 sep='\s*,\s*',
                                 header=0,
                                 engine='python')
                col = objectRecieved['db']['siteConfig']['csv']['csvCols']
                print(col)
                missing = []
                for i, j in enumerate(col):
                    if col[i] not in df.columns:
                        missing.append(col[i])
                col = list(filter(lambda x: x not in missing, col))
                df_final = df[col].copy()
                df_final.set_index('TIMESTAMP', inplace=True)
                #totalrows = df_final.shape[0]
                df_final.dropna(inplace=True)
                #removedrows = totalrows - df_final.shape[0]
                receiveTime = time.ctime(os.path.getctime(objectRecieved['fileReceived']))
                filename = ntpath.basename(objectRecieved['fileReceived'])
                #df_csv = pd.DataFrame([receiveTime, filename, totalrows, removedrows, missing if len(missing) != 0 else None]).transpose()
                #df_csv.set_index(0)
                col.remove('TIMESTAMP')
                for keys in col:
                    if objectRecieved['db']['siteConfig']['csv'][keys]['applyChecks']:
                        if objectRecieved['db']['siteConfig']['csv'][keys]['minCheckApply']:
                            df_final[keys].loc[df_final[keys] < objectRecieved['db']['siteConfig']['csv'][keys]['min']] = None
                        if objectRecieved['db']['siteConfig']['csv'][keys]['minCheckApply']:
                            df_final[keys].loc[df_final[keys] > objectRecieved['db']['siteConfig']['csv'][keys]['max']] = None
                    if objectRecieved['db']['siteConfig']['csv'][keys]['applyOperation']:
                        df_final[keys] = (df_final[keys] / objectRecieved['db']['siteConfig']['csv'][keys]['multiplier']) + \
                                         objectRecieved['db']['siteConfig']['csv'][keys]['offset']

                CheckOldData()

                timeStamp = df_final.index.astype(str).str.replace('/', '-')
                print(df_final)
                tagsShow = []

                for i in range(len(df_final.index)):
                    unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp[i], "%d-%m-%Y %H:%M:%S").timetuple()))
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
                print(Fore.GREEN+predixConnection.timeSeries.send()+Fore.RESET)
                if requests.get('https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/clearcache',
                                headers={'x-api-key': 'gMhamr1lYt8KEy1F0rlRd5EJq8hyjJ7s6qIPKTTv'}).status_code == 200:
                    print('cache cleared')
                else:
                    print('cache cleared failed')

            elif '.json' in ntpath.basename(objectRecieved['fileReceived']) and os.path.basename(
                    objectRecieved['fileReceived']).startswith('SENSOR'):
                data = json.load(open(objectRecieved['fileReceived'], encoding='ISO-8859-1', mode='r'))
                dictionary = {x: None for x in objectRecieved['db']['siteConfig']['js']['jsCols']}
                for key, value in dictionary.items():
                    if key in data['Body']['1']:
                        dictionary[key] = 0
                        for k, v in data['Body']['1'][key].items():
                            if 'Value' in k:
                                if objectRecieved['db']['siteConfig']['js'][key]['applyChecks']:
                                    if objectRecieved['db']['siteConfig']['js'][key]['minCheckApply']:
                                        v = 0 if v < objectRecieved['db']['siteConfig']['js'][key]['min'] else v
                                    if objectRecieved['db']['siteConfig']['js'][key]['maxCheckApply']:
                                        v = 0 if v > objectRecieved['db']['siteConfig']['js'][key]['max'] else v
                                if objectRecieved['db']['siteConfig']['js'][key]['applyOperation']:
                                    v = (v / objectRecieved['db']['siteConfig']['js'][key]['multiplier']) + \
                                        objectRecieved['db']['siteConfig']['js'][key]['offset']
                                dictionary[key] += v
                    if key in data['Head']:
                        dictionary[key] = data['Head']['Timestamp']

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

            ftpObj = ftpService.FTP(filePath=objectRecieved['fileReceived'],
                                    serverPath=objectRecieved['db']['siteConfig']['siteInfo']['FTPpath'])
            ftpObj.sendFTP()


os.remove(objectRecieved['fileReceived'])