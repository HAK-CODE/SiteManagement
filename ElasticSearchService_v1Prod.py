import requests
import dateutil.parser
import json
import os
from requests.auth import HTTPBasicAuth

class ElasticSearchService:
    url = os.environ['es_url']
    index = None
    isMultipleTS = False
    indexPattern = {"index": {"_index":  None, "_id": None}}

    def __init__(self, index, isMultipleTS=False):
        self.index = index
        self.isMultipleTS = isMultipleTS

    def loadData(self, data):
        if self.isMultipleTS is False:
            print("from elastic search")
            print(data)
            indiceStatus = self.indiceController(data['@timestamp'])
            print(indiceStatus)
            if indiceStatus != False:
                buffer = ""
                self.indexPattern['index']['_index'] = indiceStatus[1]
                self.indexPattern['index']['_id'] = data['@timestamp']
                print(self.indexPattern)
                typeData = data['type']
                del data['type']
                buffer += str(json.dumps(self.indexPattern) + "\n")
                buffer += str(json.dumps(data) + '\n')

                isIdExist = requests.get(url=self.url+"/"+indiceStatus[1]+"/_doc/"+data['@timestamp'].replace("+", "%2B"), auth=(os.environ['es_user'], os.environ['es_pass']))
                if isIdExist.status_code == 200:
                    print("data already exist with id")
                    updateStatus = requests.post(url=self.url+"/"+indiceStatus[1]+"/_doc/"+data['@timestamp'].replace("+", "%2B")+"/_update",
                                                 auth=(os.environ['es_user'], os.environ['es_pass']),
                                                 headers={"content-type": "application/json"},
                                                 json={"doc": {typeData: data[typeData]}})
                    print(updateStatus.content)
                else:
                    newData = requests.put(url=self.url + "/" + self.index + "/_doc/_bulk",
                                           auth=(os.environ['es_user'], os.environ['es_pass']),
                                           headers={"content-type": "application/json"},
                                           data=buffer)
                    if newData.status_code == 200:
                        print("Data Loaded successfully.")
                        return True
                    else:
                        print("Data Loaded failed.")
                        return False
            return False
        else:
            print("from elastic search")
            print("length is "+str(len(data)))
            for stream in data:
                print("date stream "+str(stream['@timestamp']))
                indiceStatus = self.indiceController(stream['@timestamp'])
                if indiceStatus != False:
                    buffer = ""
                    self.indexPattern['index']['_index'] = indiceStatus[1]
                    self.indexPattern['index']['_id'] = stream['@timestamp']
                    typeData = stream['siteTag']
                    del stream['type']
                    buffer += str(json.dumps(self.indexPattern) + "\n")
                    buffer += str(json.dumps(stream) + '\n')
                    print(buffer)
                    isIdExist = requests.get(url=self.url + "/" + indiceStatus[1] + "/_doc/" + stream['@timestamp'].replace("+", "%2B"), auth=(os.environ['es_user'], os.environ['es_pass']))
                    if isIdExist.status_code == 200:
                        print("data already exist with id")
                        updateStatus = requests.post(url=self.url + "/" + indiceStatus[1] + "/_doc/" + stream['@timestamp'].replace("+","%2B") + "/_update",
                                                     auth=(os.environ['es_user'], os.environ['es_pass']),
                                                     headers={"content-type": "application/json"},
                                                     json={"doc": {typeData: stream[typeData]}})
                        print(updateStatus.content)
                    else:
                        newData = requests.put(url=self.url + "/" + self.index + "/_doc/_bulk",
                                               auth=(os.environ['es_user'], os.environ['es_pass']),
                                               headers={"content-type": "application/json"},
                                               data=buffer)
                        print(newData.text)
                        if newData.status_code == 200:
                            print("Data Loaded successfully.")
                        else:
                            print("Data Loaded failed.")
            return True

    def indiceController(self, date):
        extractdata = dateutil.parser.parse(date).date()
        indice = "site-"+str(self.index).lower()+str("-")+str(extractdata.year)+"."+str(extractdata.month)+"."+str(extractdata.day)
        isCreated = requests.get(url=self.url+"/"+indice, auth=(os.environ['es_user'], os.environ['es_pass']))
        print("indice pattern is "+indice)
        if isCreated.status_code != 200:
            createdIndice = requests.put(url=self.url+"/"+indice, auth=(os.environ['es_user'], os.environ['es_pass']))
            if createdIndice.status_code == 200:
                print("indice created "+indice)
                return [True, indice]
            return [True, indice]
        elif isCreated.status_code == 200:
            return [True, indice]
        return False
