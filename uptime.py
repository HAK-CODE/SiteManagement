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


def calIrradianceUpTime(index_half, first_tag, second_tag,sites_list_inv_qty,site_name):
    index = site_name+index_half
    url = os.environ['es_url']
    list_tags = [first_tag, second_tag, "@timestamp"]
    inv_pow = [0] * sites_list_inv_qty
    for i in range(1, sites_list_inv_qty+1):
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
    #print (res)
    _id = res['hits']['hits'][0]['_source']['@timestamp'].replace("+05:00", "")
    _id = datetime.strptime(_id, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
    timeDiff = 0
    IrradianceUpTime = 0
    InverterUpTime = 0
    timeDiff = res['hits']['hits'][0]['_source']['@timestamp']

    first_tag = first_tag.split(".")
    second_tag = second_tag.split(".")

    print(first_tag)
    print(second_tag)
    for i, v in enumerate(res['hits']['hits']):
        if i + 1 <= len(res['hits']['hits']) - 1:
            if getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['status'] == False:
                IrradianceUpTime += 0
            else:
                try:
                    irrad = res['hits']['hits'][i + 1]['_source'][first_tag[0]][first_tag[1]]
                except Exception:
                    irrad = 0
                    pass
                if (irrad > 50):
                    IrradianceUpTime += getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']
                    try:
                        invUP = res['hits']['hits'][i + 1]['_source'][second_tag[0]][second_tag[1]]
                    except Exception:
                        invUP=0
                        pass
                    if (invUP > 0):
                        InverterUpTime += getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']

                    for z in range(sites_list_inv_qty):
                        try:
                            invUP = res['hits']['hits'][i + 1]['_source'][second_tag[0]]["INV" + str(z + 1) + "-W"]
                        except Exception:
                            invUP=0
                            pass

                        if (invUP > 0):
                            inv_pow[z] += getDIfferenceMin(res['hits']['hits'][i + 1]['_source']['@timestamp'], timeDiff)['value']

            # prev_value = res['hits']['hits'][i + 1]['_source']['first_tag[0]'][first_tag[1]]
            timeDiff = res['hits']['hits'][i + 1]['_source']['@timestamp']

    indice = site_name + "ir-up_time"+"-"+index_half 
    isCreated = requests.get(url=url+"/" + indice, auth=(os.environ['es_user'], os.environ['es_pass']))
    print("indice pattern is " + indice)
    
    if isCreated.status_code != 200:
        createdIndice = requests.put(
            url=url+"/" + indice, auth=(os.environ['es_user'], os.environ['es_pass']))
        if createdIndice.status_code == 200:
            print("indice created " + indice)
    elif isCreated.status_code == 200:
        print("indice already created")

    Uptime_Perc = round(InverterUpTime/IrradianceUpTime*100,2)
    if (Uptime_Perc>100):
        Uptime_Perc=100
    buffer = ""
    buffer += str(json.dumps({"index": {"_index": indice, "_id": _id}}) + "\n")
    json_temp = {"IrradianceUpTime": {"IrradianceUpTime": IrradianceUpTime, "unit": "Mins"},
                 "InvertersUpTime": {"All_InverterUpTime": InverterUpTime, "unit": "Mins"},
                 "UpTimePerc": {"InverterUpTime_Perc": Uptime_Perc, "unit": "%"}, "@timestamp": _id}
    # json_temp.update({"InverterUpTime1": {"InverterUpTime1": InverterUpTime, "unit": "Mins"}})

    y = 1
    for z in inv_pow:
        json_temp["InvertersUpTime"].update({"Inverter" + str(y) + "UpTime": z, "unit": "Mins"})
        y += 1

    buffer += str(json.dumps(json_temp) + "\n")
    print (buffer)
    
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
    return str(str(yesterday.year) + "." + str(yesterday.month) + "." + str(yesterday.day))
    # return "ktml-2019.3.1"


# def runThis():
#    print("starting")


first_tag = "logger.MET-GHI"
second_tag = "logger.PSolar"


# print (getNOW())
sites_list = ["site-ktml-","site-enter-","site-dairy-"]#,"site-ser-"]
sites_list_inv_qty = [14,16,10]

def runThis():
    b=0
    for a in sites_list:
        """ if (f=="site-ser-"):
            first_tag = "logger.MET-GHI"
            second_tag = "logger.PSolar"
        else:
            first_tag = "logger.MET-GHI"
            second_tag = "logger.PSolar" """
        try:
            #site_index = a + getNOW() 
            calIrradianceUpTime(getNOW(), first_tag, second_tag,sites_list_inv_qty[b],a)
            print (sites_list_inv_qty[b])
            b+=1
            print ("successfull")
        except Exception:
            b+=1
            print ("filed to load")
            pass

#runThis()

sched = BackgroundScheduler()
sched.add_job(runThis, trigger='cron', hour=0, minute=30)
sched.start()

while True:
    time.sleep(30)
    #print (getNOW())