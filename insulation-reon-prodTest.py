from datetime import datetime, timedelta, date
import json
from apscheduler.schedulers.background import BackgroundScheduler
import time
import requests
import os
import pytz

def getDIfferenceMin(d1, d2):
    d1 = datetime.strptime(d1.replace("+05:00", ""), "%Y-%m-%dT%H:%M:%S")
    d2 = datetime.strptime(d2.replace("+05:00", ""), "%Y-%m-%dT%H:%M:%S")
    if d1 > d2:
        td = d1 - d2
    else:
        td = d2 - d1
    return {"status": int(round(td.total_seconds() / 60)) <= 15, "value": int(round(td.total_seconds() / 60))}


def getCurrentDate():
    localTime = pytz.timezone('Asia/Karachi')
    return datetime.now(localTime)
    

def getIrradiance(typeOf, fieldTag, sitePattern, date):
    # print("site pattern is "+sitePattern)
    # print("date is "+date)
    return requests.get(url=os.environ['es_url'] + "/" + sitePattern + "/_search",
                        auth=(os.environ['es_user'], os.environ['es_pass']),
                        headers={"content-type": "application/json"},
                        json={
                             "_source": [fieldTag, "@timestamp"],
                             "size": 10000,
                             "query": {
                                 "bool":{
                                    "filter":[
                                        {
                                            "exists": {"field": fieldTag}
                                        },
                                        {
                                            "range": {
                                                "@timestamp": {
                                                    "gte": date,
                                                    "lte": date,
                                                    "time_zone": "+05:00"
                                                }
                                            }
                                        }
                                    ]
                                 }
                             },
                             "sort": [
                                 {"@timestamp": typeOf}
                             ]
                         })


def getDayEnergyParams(typeOf, sitePattern, date, _source=[]):
    return requests.get(url=os.environ['es_url'] + "/" + sitePattern + "/_search",
                        auth=(os.environ['es_user'], os.environ['es_pass']),
                        json={
                            "_source": _source,
                            "query": {
                                "range": {
                                    "@timestamp": {
                                      "gte": date,
                                      "lte": date,
                                      "time_zone": "+05:00"
                                    }
                                }
                            },
                            "size": 350,
                            "sort": [
                                {
                                    "@timestamp": {
                                        "order": typeOf
                                    }
                                }
                            ]
                        })


def calculateDayEnergy(tagSite, X, X1=None, isX2=False):
    flag = False
    x_val = 0
    for entry in X:
        for key, value in entry['_source'].items():
            if key == tagSite:
                if 'EtSolar' in value:
                    if value['EtSolar'] != 0:
                        x_val = value['EtSolar']
                        if isX2 is False:
                            flag = True
                            break
                        else:
                            if x_val > X1:
                                flag = True
                                break
                else:
                    if value['TOTAL_ENERGY']['sum'] != 0:
                        x_val = value['TOTAL_ENERGY']['sum']
                        if isX2 is False:
                            flag = True
                            break
                        else:
                            if x_val > X1:
                                flag = True
                                break

        if flag:
            break

    if isX2:
        return x_val - X1
    return x_val
    

def calculateDayParams(X1, X2):
    x2_grid = None
    x2_genset = None
    x1_grid = None
    x1_genset = None
    grid = None
    genset = None
    for entry in X2:
        for key, value in entry['_source'].items():
            if 'EtGrid' in value:
                if x2_grid is None:
                    x2_grid = value['EtGrid']
            elif 'EtGenset' in value:
                if x2_genset is None:
                    x2_genset = value['EtGenset']
    
    for entry in X1:
        for key, value in entry['_source'].items():
            if 'EtGrid' in value:
                if x1_grid is None:
                    x1_grid = value['EtGrid']
            elif 'EtGenset' in value:
                if x1_genset is None:
                    x1_genset = value['EtGenset']
    
    if x1_grid is not None and x2_grid is not None:
        grid = x2_grid - x1_grid
    
    if x1_genset is not None and x2_genset is not None:
        genset = x2_genset - x1_genset
    
    return grid, genset


def calInsulation(sizeTag, date):
    # print(sizeTag['tag'])
    # print(sizeTag['size'])
    # print(sizeTag['siteName'])
    # print("start")
    tagSite = "logger"
    
    data = getIrradiance("asc", str(tagSite+".2"), sizeTag['tag'], date)
    if data.status_code != 200:
        # print(data.content)
        print("execution halt")
        return

    res = json.loads(data.text)
    print (res['hits']['hits'])
    
    if len(res['hits']['hits']) != 0:
        _id = res['hits']['hits'][0]['_source']['@timestamp'].replace("+05:00", "")
        _id = datetime.strptime(_id, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
        timeDiff = 0
        prev_value = 0
        insulation = 0
        timeDiff = res['hits']['hits'][0]['_source']['@timestamp']
        prev_value = res['hits']['hits'][0]['_source'][tagSite]['2']
        insulation += 0.5 * (res['hits']['hits'][0]['_source'][tagSite]['2'] + 0) * 0

        for i, v in enumerate(res['hits']['hits']):
            if i + 1 <= len(res['hits']['hits']) - 1:
                if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                    insulation += 0
                else:
                    insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['2'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
                prev_value = res['hits']['hits'][i + 1]['_source'][tagSite]['2']
                timeDiff = res['hits']['hits'][i + 1]['_source']['@timestamp']
                if i + 1 == len(res['hits']['hits']) - 1:
                    insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['2'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status']
                    if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                        insulation += 0
                    else:
                        insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['2'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']

        print("insulation using sensor "+str(insulation))
    else:
        #print("using MET-POAI")
        data = getIrradiance("asc", str(tagSite+".MET-POAI"), sizeTag['tag'], date)
        res = json.loads(data.text)

        if len(res['hits']['hits']) != 0:
            _id = res['hits']['hits'][0]['_source']['@timestamp'].replace("+05:00", "")
            _id = datetime.strptime(_id, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
            timeDiff = 0
            prev_value = 0
            insulation = 0
            timeDiff = res['hits']['hits'][0]['_source']['@timestamp']
            prev_value = res['hits']['hits'][0]['_source'][tagSite]['MET-POAI']
            insulation += 0.5 * (res['hits']['hits'][0]['_source'][tagSite]['MET-POAI'] + 0) * 0
            for i, v in enumerate(res['hits']['hits']):
                if i + 1 <= len(res['hits']['hits']) - 1:
                    if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                        insulation += 0
                    else:
                        insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['MET-POAI'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
                    prev_value = res['hits']['hits'][i + 1]['_source'][tagSite]['MET-POAI']
                    timeDiff = res['hits']['hits'][i + 1]['_source']['@timestamp']
                    if i + 1 == len(res['hits']['hits']) - 1:
                        insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['MET-POAI'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status']
                        if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                            insulation += 0
                        else:
                            insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['MET-POAI'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
            
        else:
            #print("using MET-GHI")
            data = getIrradiance("asc", str(tagSite+".MET-GHI"), sizeTag['tag'], date)
            res = json.loads(data.text)
            
            if len(res['hits']['hits']) == 0:
                print("length is 0")
                return
            
            _id = res['hits']['hits'][0]['_source']['@timestamp'].replace("+05:00", "")
            _id = datetime.strptime(_id, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
            timeDiff = 0
            prev_value = 0
            insulation = 0
            timeDiff = res['hits']['hits'][0]['_source']['@timestamp']
            prev_value = res['hits']['hits'][0]['_source'][tagSite]['MET-GHI']
            insulation += 0.5 * (res['hits']['hits'][0]['_source'][tagSite]['MET-GHI'] + 0) * 0
            for i, v in enumerate(res['hits']['hits']):
                if i + 1 <= len(res['hits']['hits']) - 1:
                    if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                        insulation += 0
                    else:
                        insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['MET-GHI'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
                    prev_value = res['hits']['hits'][i + 1]['_source'][tagSite]['MET-GHI']
                    timeDiff = res['hits']['hits'][i + 1]['_source']['@timestamp']
                    if i + 1 == len(res['hits']['hits']) - 1:
                        insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['MET-GHI'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status']
                        if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                            insulation += 0
                        else:
                            insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][tagSite]['MET-GHI'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
        
    print("insulation "+str(insulation))    
    insulation = insulation/60000

    print("insulation is "+str(insulation))

    X1 = getDayEnergyParams("asc", sizeTag['tag'], date, ["logger.TOTAL_ENERGY.sum", "logger.SiteEnergyTotal", "logger.EtSolar", "logger.EtGrid", "logger.EtGenset"])
    X1 = json.loads(X1.text)['hits']['hits']

    x1_val = calculateDayEnergy(tagSite, X1)

    X2 = getDayEnergyParams("desc", sizeTag['tag'], date, ["logger.TOTAL_ENERGY.sum", "logger.SiteEnergyTotal", "logger.EtSolar", "logger.EtGrid", "logger.EtGenset"])
    X2 = json.loads(X2.text)['hits']['hits']

    forPrcalculation = calculateDayEnergy(tagSite, X2, x1_val, True)
    grid, genset = calculateDayParams(X1, X2)
    
    # print("grid "+str(grid))
    # print("genset "+str(genset))
    # print("day energy "+str(forPrcalculation))

    p90_value = requests.get(
        url=os.environ['es_url'] + "/site-" + sizeTag['tag'].split('-')[1].lower() + "-pvalues/_search",
        auth=(os.environ['es_user'], os.environ['es_pass']),
        json={
            "_source": ["p90"],
            "size": 10,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": date,
                                    "lte": date
                                }
                            }
                        }
                    ]
                }
            }
        })

    deviation = None

    if p90_value.status_code == 200:
        if len(json.loads(p90_value.text)['hits']['hits'][0]['_source']) != 0:
            p90_value = json.loads(p90_value.text)['hits']['hits'][0]['_source']['p90']
            deviation = ((forPrcalculation - p90_value)/p90_value) * 100


    Yield = forPrcalculation/sizeTag['size']
    
    print(insulation)
    
    try:
        pr_ratio = ((forPrcalculation)/(insulation * sizeTag['size']) * 100)
        print (pr_ratio)
    except:
        print("pr not calculated")
        #traceback.print_exc()
        return None
    
    # print("_id is now "+str(_id))

    indice = str(sizeTag['tag']).split("-")[0].lower() + "-" + str(sizeTag['tag']).split("-")[1].lower() + "-insulation"
    isCreated = requests.get(url=os.environ['es_url'] + "/" + indice,
                             auth=(os.environ['es_user'], os.environ['es_pass']))
    if isCreated.status_code != 200:
        createdIndice = requests.put(url= os.environ['es_url'] + "/" + indice, auth=(os.environ['es_user'], os.environ['es_pass']))
        if createdIndice.status_code == 200:
            print("indice created " + indice)
    elif isCreated.status_code == 200:
        print("indice already created")

    # print("insulation "+str(insulation))
    # print("deviation "+str(deviation))

    buffer = ""
    buffer += str(json.dumps({"index": {"_index": indice, "_id": _id}}) + "\n")
    buffer += str(json.dumps({"insulation": {"value": insulation, "unit": "KW/m^2"},
                              "pr-ratio": {"value": pr_ratio if pr_ratio <=100 and pr_ratio >= 0 else None, "unit": "%"},
                              "yield":{"value": Yield, "unit": "kWh/kWp"},
                              "deviation":{"value": deviation},
                              "DAY_CALCULATION": forPrcalculation,
                              "ETGRID": grid,
                              "ETGENSET": genset,
                              "ETSOLAR": forPrcalculation,
                              "siteName": sizeTag['siteName'],
                              "system_size": sizeTag['size'],
                              "@timestamp": _id}) + "\n")
    isIdExist = requests.get(url= os.environ['es_url'] + "/" + indice + "/_doc/" + _id.replace("+", "%2B"),
                             auth=(os.environ['es_user'], os.environ['es_pass']))
    if isIdExist.status_code == 200:
        # print("data already exist with id")
        # print("id is "+str(_id))
        # print("indice "+indice)
        updateStatus = requests.post(url= os.environ['es_url'] + "/" + indice + "/_doc/" + _id.replace("+", "%2B"),
                                     headers={"content-type": "application/json"},
                                     auth=(os.environ['es_user'], os.environ['es_pass']),
                                     json={"insulation": {"value": insulation, "unit": "KW/m^2"},
                                           "pr-ratio": {"value": pr_ratio if pr_ratio <=100 and pr_ratio >= 0 else None, "unit": "%"},
                                           "yield": {"value": Yield, "unit": "kWh/kWp"},
                                           "deviation": {"value": deviation},
                                           "DAY_CALCULATION": forPrcalculation,
                                           "ETGRID": grid,
                                           "ETGENSET": genset,
                                           "ETSOLAR": forPrcalculation,
                                           "siteName": sizeTag['siteName'],
                                           "system_size": sizeTag['size'],
                                           "@timestamp": _id})
        print(updateStatus.content)
    else:
        newData = requests.put(
            url=os.environ['es_url'] + "/" + indice + "/_doc/_bulk",
            headers={"content-type": "application/json"},
            auth=(os.environ['es_user'], os.environ['es_pass']),
            data=buffer)
        if newData.status_code == 200:
            print("Data Loaded successfully.")
        else:
            print("Data Loaded failed.")


def getNOW(tag, i=29):
    #yesterday = date.today() - timedelta(1)
    
    # for moth migration
    print ([str("site-" + tag + "-2019.6"), "2019-06-"+"0"+str(i) if i < 10 else str(i)])
    return [str("site-" + tag + "-2019.6"), "2019-06-"+"0"+str(i) if i < 10 else str(i)]
    
    # return [str("site-" + tag + "-2019.6"), "2019-06-22"]
    #date = getCurrentDate()
    #return [str("site-" + tag + "-" + str(date.year) + "." + str(date.month)), str(date).split(' ')[0]]


def runThis():
    print("starting")
    tags = requests.get('https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/getallsitesinsulationflag',
                         headers={'x-api-key': 'gMhamr1lYt8KEy1F0rlRd5EJq8hyjJ7s6qIPKTTv'})
    #print (json.loads(tags.text)["response"][0])
    #faizi=json.loads(tags.text)["response"][0]
    for i in range(29,30):
        for tag in json.loads(tags.text)['response']:
           print (tag)
           calInsulation({"tag": getNOW(tag['tag'])[0], "size": float(tag['size']), "siteName": tag['name']}, getNOW(tag['tag'],i)[1])
           print (i)
           break
        #calInsulation({"tag": getNOW("ser"), "size": 1000.0, "siteName": "Servis"},getNOW("ser",i)[1])

runThis()