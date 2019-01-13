import requests
import dateutil.parser
import json

class ElasticSearchService:
    url = "https://search-reon-yf6s4jcgv6tapjin4xblwtgk6y.us-east-2.es.amazonaws.com"
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

                isIdExist = requests.get(url=self.url+"/"+indiceStatus[1]+"/_doc/"+data['@timestamp'].replace("+", "%2B"))
                if isIdExist.status_code == 200:
                    print("data already exist with id")
                    updateStatus = requests.post(url=self.url+"/"+indiceStatus[1]+"/_doc/"+data['@timestamp'].replace("+", "%2B")+"/_update",
                                                 headers={"content-type": "application/json"},
                                                 json={"doc": {typeData: data[typeData]}})
                    print(updateStatus.content)
                else:
                    newData = requests.put(url=self.url + "/" + self.index + "/_doc/_bulk",
                                           headers={"content-type": "application/json"},
                                           data=buffer)
                    if newData.status_code == 200:
                        print("Data Loaded successfully.")
                        return True
                    else:
                        print("Data Loaded failed.")
                        return False
            return

    def indiceController(self, date):
        extractdata = dateutil.parser.parse(date).date()
        indice = str(self.index).lower()+str("-")+str(extractdata.year)+"."+str(extractdata.month)+"."+str(extractdata.day)
        isCreated = requests.get(url=self.url+"/"+indice)
        print("indice patern is "+indice)
        if isCreated.status_code != 200:
            createdIndice = requests.put(url=self.url+"/"+indice)
            if createdIndice.status_code == 200:
                print("indice created "+indice)
                return [True, indice]
            return [True, indice]
        elif isCreated.status_code == 200:
            return [True, indice]
        return False
