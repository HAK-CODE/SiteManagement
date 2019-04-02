import requests
import argparse
import sys
import json
import os
import datetime
import calendar

parser = argparse.ArgumentParser(prog='SITE LANCHER',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Launch a new site providing site id.')
parser.add_argument('--siteId', '-sid', type=str, help="Specify site id.", required=True)
args = parser.parse_args()

id = os.getpid()

try:
    siteConfig = requests.get(
        'https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/siteconfig?siteId=' + args.siteId)
except:
    print('Connection to server failed')
    sys.exit(1)

# if siteConfig.status_code != 200 or tmuxConfig.status_code != 200:
#    print('Site configuration or Tmux configuration return with non 200 code.')
#    sys.exit(1)

siteConfigData = json.loads(siteConfig.content.decode('utf-8'))
print(siteConfigData)
siteTag = "site-"+siteConfigData['siteInfo']['siteTag']+"-"
siteTag = siteTag.lower()

print(siteTag)

siteFullConfig = dict()
siteFullConfig['siteConfig'] = siteConfigData
datee = siteFullConfig['siteConfig']['siteForcasteInfo']['startingdate']
datee = datetime.datetime.strptime(datee, "%d-%m-%Y")
print(datee.month)
print(datee.year)

dgr_per = siteFullConfig['siteConfig']['siteForcasteInfo']['dgr']

print(dgr_per)
p90_factor = siteFullConfig['siteConfig']['siteForcasteInfo']['p90']
print(p90_factor)

start_month = datee.month
start_year = datee.year
bit = 0
dgr = 1
url = os.environ['es_url']
for i in range(1, 20):  # 25 years data
    print("DGR:" + str(dgr))
    for j in range(1, 13):
        indexPattern = {"index": {"_index": None, "_id": None}}
        if (start_month > 12):
            months = start_month - 12

            if (bit == 0):
                start_year += 1
                bit = 1
        else:
            months = start_month

        if (months != 2):
            month_name = calendar.month_name[months].lower()

        else:
            month_name = "feburary"

        days = calendar.monthrange(start_year, months)[1]

        p50 = round((siteFullConfig['siteConfig']['siteForcasteInfo'][month_name] /
                     siteFullConfig['siteConfig']['siteForcasteInfo']['multiplier'] / days) * dgr, 3)
        p90 = round(p50 * p90_factor, 3)
        buffer = ""
        for k in range(1, days + 1):
            timestamp = str(start_year) + "." + str(months) + "." + str(k)
            indexPattern['index']['_index'] = siteTag+timestamp
            format = datetime.datetime.strptime(str(str(start_year) + "-" + str(months) + "-" + str(k)), "%Y-%m-%d")
            format = str(format).replace(" ","T")+"+05:00"
            indexPattern['index']['_id'] = "site-ktml-pvalues"
            data = {
                "@timestamp": format,
                "p50": p50,
                "p90": p90,
                "unit": "kWh"
            }
            buffer += str(json.dumps(indexPattern) + "\n")
            buffer += str(json.dumps(data) + '\n')
            #print(buffer)

        isIdExist = requests.get(
            url=url + "/" + indexPattern['index']['_index'] + "/_doc/" + data['@timestamp'].replace("+", "%2B"),
            auth=(os.environ['es_user'], os.environ['es_pass']))
        print(isIdExist.content)
        if isIdExist.status_code == 200:
            print("data already exist with id")
            updateStatus = requests.post(
                url=url + "/" + indexPattern['index']['_index'] + "/_doc/" + data['@timestamp'].replace("+", "%2B"),
                auth=(os.environ['es_user'], os.environ['es_pass']),
                headers={"content-type": "application/json"},
                json=data)
            print(updateStatus.content)
        else:
            newData = requests.put(url=url + "/" + indexPattern['index']['_index'] + "/_doc/_bulk",
                                   auth=(os.environ['es_user'], os.environ['es_pass']),
                                   headers={"content-type": "application/json"},
                                   data=buffer)
            if newData.status_code == 200:
                print("Data Loaded successfully.")
            else:
                print("Data Loaded failed.")
        start_month = datee.month + j
    bit = 0
    dgr = 1 - dgr_per * i