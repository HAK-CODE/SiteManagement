from datetime import datetime, timedelta, date
import requests
import json
from apscheduler.schedulers.background import BackgroundScheduler
import time
import os

# import sys

def getDIfferenceMin(d1, d2):
    d1 = datetime.strptime(d1.replace("+05:00", ""), "%Y-%m-%dT%H:%M:%S")
    d2 = datetime.strptime(d2.replace("+05:00", ""), "%Y-%m-%dT%H:%M:%S")
    if d1 > d2:
        td = d1 - d2
    else:
        td = d2 - d1
    return {"status": int(round(td.total_seconds() / 60)) <= 15, "value": int(round(td.total_seconds() / 60))}


def calIrradianceUpTime(index, first_tag, second_tag):
    url = os.environ['es_url']
    list_tags = [first_tag, second_tag, "@timestamp"]
    inv_pow = [0] * 14
    for i in range(1, 15):
        list_tags.append("logger.INV" + str(i) + "-W")
    # print (list_tags)

    data = requests.post(
        url=url +"/" + index + "/_search",
        auth=(os.environ['es_user'], os.environ['es_pass']),
        json={
            "_source": list_tags,  # [first_tag,second_tag, "@timestamp"],
            "size": 10000,
            "query": {
                "exists": {"field": first_tag}
            },
            "sort": [
                {"@timestamp": "asc"}
            ]
        })

    res = json.loads(data.text)
    # print (res)
    _id = res['hits']['hits'][0]['_source']['@timestamp'].replace("+05:00", "")
    _id = datetime.strptime(_id, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
    timeDiff = 0
    IrradianceUpTime = 0
    InverterUpTime = 0
    timeDiff = res['hits']['hits'][0]['_source']['@timestamp']

    first_tag = first_tag.split(".")
    second_tag = second_tag.split(".")

    for i, v in enumerate(res['hits']['hits']):
        if i + 1 <= len(res['hits']['hits']) - 1:
            if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                IrradianceUpTime += 0
            else:
                irrad = res['hits']['hits'][i + 1]['_source'][first_tag[0]][first_tag[1]]
                if (irrad > 50):
                    IrradianceUpTime += getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
                    invUP = res['hits']['hits'][i + 1]['_source'][second_tag[0]][second_tag[1]]
                    if (invUP > 0):
                        InverterUpTime += getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']

                    for z in range(14):
                        invUP = res['hits']['hits'][i + 1]['_source'][second_tag[0]]["INV" + str(z + 1) + "-W"]

                        if (invUP > 0):
                            inv_pow[z] += getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']

            # prev_value = res['hits']['hits'][i + 1]['_source']['first_tag[0]'][first_tag[1]]
            timeDiff = res['hits']['hits'][i + 1]['_source']['@timestamp']

    indice = "site-"+str(index).split("-")[0].lower() + "-ir.up_time." + index[5:]
    isCreated = requests.get(url=url+"/" + indice, auth=(os.environ['es_user'], os.environ['es_pass']))
    # print("indice pattern is " + indice)

    if isCreated.status_code != 200:
        createdIndice = requests.put(
            url=url+"/" + indice, auth=(os.environ['es_user'], os.environ['es_pass']))
        if createdIndice.status_code == 200:
            print("indice created " + indice)
    elif isCreated.status_code == 200:
        print("indice already created")

    buffer = ""
    buffer += str(json.dumps({"index": {"_index": indice, "_id": _id}}) + "\n")
    json_temp = {"IrradianceUpTime": {"IrradianceUpTime": IrradianceUpTime, "unit": "Mins"},
                 "InvertersUpTime": {"All_InverterUpTime": InverterUpTime, "unit": "Mins"}, "@timestamp": _id}
    # json_temp.update({"InverterUpTime1": {"InverterUpTime1": InverterUpTime, "unit": "Mins"}})

    y = 1
    for z in inv_pow:
        json_temp["InvertersUpTime"].update({"Inverter" + str(y) + "UpTime": z, "unit": "Mins"})
        y += 1

    buffer += str(json.dumps(json_temp) + "\n")
    # print (buffer)

    isIdExist = requests.get(url=url+"/" + indice + "/_doc/" + _id.replace("+", "%2B"), auth=(os.environ['es_user'], os.environ['es_pass']))
    if isIdExist.status_code == 200:
        print("data already exist with id")
        updateStatus = requests.post(
            url= url+"/" + indice + "/_doc/" + _id.replace("+", "%2B") + "/_update",
            auth=(os.environ['es_user'], os.environ['es_pass']),
            headers={"content-type": "application/json"},
            json={"doc": {"value": IrradianceUpTime, "unit": "Mins", "@timestamp": _id}})
        print(updateStatus.content)
    else:
        newData = requests.put(
            url=url + "/" + indice + "/_doc/_bulk",
            auth=(os.environ['es_user'], os.environ['es_pass']),
            headers={"content-type": "application/json"},
            data=buffer)
        if newData.status_code == 200:
            print("Data Loaded successfully.")
        else:
            print("Data Loaded failed.")
    # IrradianceUpTime = IrradianceUpTime/
    # print (InverterUpTime)
    # return IrradianceUpTime

    print(IrradianceUpTime)
    print(InverterUpTime)
    print(inv_pow)


def getNOW():
    yesterday = date.today() - timedelta(1)
    return str("site-ktml-" + str(yesterday.year) + "." + str(yesterday.month) + "." + str(yesterday.day))
    # return "ktml-2019.3.1"


# def runThis():
#    print("starting")


first_tag = "logger.MET-GHI"
second_tag = "logger.PSolar"


# print (getNOW())
def runThis():
    calIrradianceUpTime(getNOW(), first_tag, second_tag)


# runThis()

sched = BackgroundScheduler()
sched.add_job(runThis, trigger='cron', hour=0, minute=9)
sched.start()

while True:
    time.sleep(30)
# print (getNOW())