from datetime import datetime, timedelta, date
import json
from apscheduler.schedulers.background import BackgroundScheduler
import time
import requests
import os


def getDIfferenceMin(d1, d2):
    d1 = datetime.strptime(d1.replace("+05:00", ""), "%Y-%m-%dT%H:%M:%S")
    d2 = datetime.strptime(d2.replace("+05:00", ""), "%Y-%m-%dT%H:%M:%S")
    if d1 > d2:
        td = d1 - d2
    else:
        td = d2 - d1
    return {"status": int(round(td.total_seconds() / 60)) <= 15, "value": int(round(td.total_seconds() / 60))}


def calInsulation(sizeTag):
    print(sizeTag['tag'])
    print(sizeTag['size'])
    print(sizeTag['siteName'])
    tagSite = sizeTag['tag'].split('-')[1].upper()
    print(tagSite)
    url = os.environ['es_url']
    data = requests.post(
        url=url + "/" + sizeTag['tag'] + "/_search",
        auth=(os.environ['es_user'], os.environ['es_pass']),
        json={
            "_source": [sizeTag['tag']+".2", "@timestamp"],
            "size": 10000,
            "query": {
                "exists": {"field": sizeTag['tag']+".2"}
            },
            "sort": [
                {"@timestamp": "asc"}
            ]
        })

    if data.status_code != 200:
        print("execution halt")
        return

    res = json.loads(data.text)

    if len(res['hits']['hits']) != 0:
        _id = res['hits']['hits'][0]['_source']['@timestamp'].replace("+05:00", "")
        _id = datetime.strptime(_id, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
        timeDiff = 0
        prev_value = 0
        insulation = 0
        timeDiff = res['hits']['hits'][0]['_source']['@timestamp']
        prev_value = res['hits']['hits'][0]['_source'][sizeTag['tag']]['2']
        insulation += 0.5 * (res['hits']['hits'][0]['_source'][sizeTag['tag']]['2'] + 0) * 0

        for i, v in enumerate(res['hits']['hits']):
            if i + 1 <= len(res['hits']['hits']) - 1:
                if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                    insulation += 0
                else:
                    insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][sizeTag['tag']]['2'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
                prev_value = res['hits']['hits'][i + 1]['_source'][sizeTag['tag']]['2']
                timeDiff = res['hits']['hits'][i + 1]['_source']['@timestamp']
                if i + 1 == len(res['hits']['hits']) - 1:
                    insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][sizeTag['tag']]['2'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status']
                    if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                        insulation += 0
                    else:
                        insulation += 0.5 * (res['hits']['hits'][i + 1]['_source'][sizeTag['tag']]['2'] + prev_value) * getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
    else:
        print("using MET-GHI")
        data = requests.post(
            url= url + "/" + sizeTag['tag'] + "/_search",
            auth=(os.environ['es_user'], os.environ['es_pass']),
            json={
                "_source": [tagSite+".MET-GHI", "@timestamp"],
                "size": 10000,
                "query": {
                    "exists": {"field": tagSite+".MET-GHI"}
                },
                "sort": [
                    {"@timestamp": "asc"}
                ]
            })
        res = json.loads(data.text)
        if len(res['hits']['hits']) == 0:
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

    insulation = insulation/60000

    print("insulation is "+str(insulation))

    x1_val = 0
    x2_val = 0
    X1 = requests.get(url=url + "/" + sizeTag['tag'] +"/_search",
                      auth=(os.environ['es_user'], os.environ['es_pass']),
                      json= {
                          "_source": ["inverter.TOTAL_ENERGY.sum", tagSite+".EtSolar"],
                          "query": {
                              "match_all": {}
                          },
                          "size": 350,
                          "sort": [
                              {
                                  "@timestamp": {
                                      "order": "asc"
                                  }
                              }
                          ]
                      })

    X1 = json.loads(X1.text)['hits']['hits']
    flag_x1 = False
    for entry in X1:
        for key,value in entry['_source'].items():
            if key == tagSite:
                print("value is "+str(value['EtSolar']))
                if value['EtSolar'] != 0:
                    print("value not 0")
                    x1_val = value['EtSolar']
                    flag_x1 = True
                    break
            else:
                print("value is " + str(value['TOTAL_ENERGY']['sum']))
                if value['TOTAL_ENERGY']['sum'] != 0:
                    print("value not 0")
                    x1_val = value['TOTAL_ENERGY']['sum']
                    flag_x1 = True
                    break

        if flag_x1:
            break

    flag_x2 = False
    X2 = requests.get(
        url= url + "/" + sizeTag['tag'] + "/_search",
        auth=(os.environ['es_user'], os.environ['es_pass']),
        json={
            "_source": ["inverter.TOTAL_ENERGY.sum", tagSite+".EtSolar"],
            "query": {
                "match_all": {}
            },
            "size": 350,
            "sort": [
                {
                    "@timestamp": {
                        "order": "desc"
                    }
                }
            ]
        })

    X2 = json.loads(X2.text)['hits']['hits']

    for entry in X2:
        for key, value in entry['_source'].items():
            if key == tagSite:
                if value['EtSolar'] != 0:
                    x2_val = value['EtSolar']
                    flag_x2 = True
                    break
            else:
                if value['TOTAL_ENERGY']['sum'] != 0:
                    x2_val = value['TOTAL_ENERGY']['sum']
                    flag_x2 = True
                    break
        if flag_x2:
            break

    print(x1_val)
    print(x2_val)
    forPrcalculation = x2_val - x1_val

    print("day energy "+str(forPrcalculation))

    p90_value = requests.get(
        url=url + "/" + sizeTag['tag'] + "/_search",
        auth=(os.environ['es_user'], os.environ['es_pass']),
        json={
            "_source": ["p90"],
            "query": {
                "match_all": {}
            },
            "size": 1,
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc"
                    }
                }
            ]
        })

    deviation = None
    if len(json.loads(p90_value.text)['hits']['hits'][0]['_source']) != 0:
        p90_value = json.loads(p90_value.text)['hits']['hits'][0]['_source']['p90']
        deviation = ((forPrcalculation - p90_value)/p90_value) * 100

    Yield = forPrcalculation/sizeTag['size']

    pr_ratio = (forPrcalculation)/(insulation * sizeTag['size']) * 100

    print("ratio is "+str(pr_ratio))

    indice = str(sizeTag['tag']).split("-")[0].lower() + "-" + str(sizeTag['tag']).split("-")[1].lower() + "-insulation"
    isCreated = requests.get(url=url + "/" + indice,
                             auth=(os.environ['es_user'], os.environ['es_pass']))
    print("indice pattern is " + indice)
    if isCreated.status_code != 200:
        createdIndice = requests.put(
            url= url + "/" + indice,
            auth=(os.environ['es_user'], os.environ['es_pass']))
        if createdIndice.status_code == 200:
            print("indice created " + indice)
    elif isCreated.status_code == 200:
        print("indice already created")

    print("insulation "+str(insulation))

    buffer = ""
    buffer += str(json.dumps({"index": {"_index": indice, "_id": _id}}) + "\n")
    buffer += str(json.dumps({"insulation": {"value": insulation, "unit": "KW/m^2"},
                              "pr-ratio": {"value": pr_ratio if pr_ratio <=100 else 100, "unit": "%"},
                              "yield":{"value": Yield, "unit": "kWh/kWp"},
                              "deviation":{"value": deviation},
                              "DAY_CALCULATION": forPrcalculation,
                              "siteName": sizeTag['siteName'],
                              "@timestamp": _id}) + "\n")
    isIdExist = requests.get(url= url + "/" + indice + "/_doc/" + _id.replace("+", "%2B"),
                             auth=(os.environ['es_user'], os.environ['es_pass']))
    if isIdExist.status_code == 200:
        print("data already exist with id")
        updateStatus = requests.post(url= url + "/" + indice + "/_doc/" + _id.replace("+", "%2B"),
                                     headers={"content-type": "application/json"},
                                     auth=(os.environ['es_user'], os.environ['es_pass']),
                                     json={"insulation": {"value": insulation, "unit": "KW/m^2"},
                                           "pr-ratio": {"value": pr_ratio if pr_ratio <=100 else 100, "unit": "%"},
                                           "yield": {"value": Yield, "unit": "kWh/kWp"},
                                           "deviation": {"value": deviation},
                                           "DAY_CALCULATION": forPrcalculation,
                                           "siteName": sizeTag['siteName'],
                                           "@timestamp": _id})
        print(updateStatus.content)
    else:
        newData = requests.put(
            url=url + "/" + indice + "/_doc/_bulk",
            headers={"content-type": "application/json"},
            auth=(os.environ['es_user'], os.environ['es_pass']),
            data=buffer)
        if newData.status_code == 200:
            print("Data Loaded successfully.")
        else:
            print("Data Loaded failed.")


def getNOW(tag, i=None):
    yesterday = date.today() - timedelta(1)
    #return str("site-" + tag + "-2019.4."+str(i))
    return str("site-" + tag + "-2019.4.17")
    #return str("site-" + tag + "-" + str(yesterday.year) + "." + str(yesterday.month) + "." + str(yesterday.day))


def runThis():
    print("starting")
    tags = requests.get('https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/getallsitesinsulationflag',
                         headers={'x-api-key': 'gMhamr1lYt8KEy1F0rlRd5EJq8hyjJ7s6qIPKTTv'})
    # for i in range(1,25):
    #     for tag in json.loads(tags.text)['response']:
    #         calInsulation({"tag": getNOW(tag['tag'], i), "size": float(tag['size']), "siteName": tag['name']})

    for tag in json.loads(tags.text)['response']:
        calInsulation({"tag": getNOW(tag['tag']), "size": float(tag['size']), "siteName": tag['name']})

# sched = BackgroundScheduler()
# sched.add_job(runThis, trigger='cron', hour=3, minute=30)
# sched.start()
#
# while True:
#     time.sleep(30)

runThis()