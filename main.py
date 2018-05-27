import requests
import argparse
import sys
import json
import os
from Watch import Watcher


parser = argparse.ArgumentParser(prog='SITE LANCHER',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Launch a new site providing site id.')
parser.add_argument('--siteId', '-sid', type=str, help="Specify site id.", required=True)
args = parser.parse_args()

id = os.getpid()

try:
    siteConfig = requests.get('http://0.0.0.0:5000/api/siteserviceapi/siteconfig?siteId='+args.siteId)
    tmuxConfig = requests.get('http://0.0.0.0:5000/api/siteserviceapi/tmuxconfig?siteId='+args.siteId)
except:
    print('Connection to server failed')
    sys.exit(1)


if siteConfig.status_code != 200 or tmuxConfig.status_code != 200:
    print('Site configuration or Tmux configuration return with non 200 code.')
    sys.exit(1)

siteConfigData = json.loads(siteConfig.content)
tmuxConfigData = json.loads(tmuxConfig.content)

if os.path.isdir(siteConfigData['siteInfo']['observingPath']) is False:
    print('Observing path not exist.')
    sys.exit(1)

if os.path.isfile(siteConfigData['siteInfo']['processPyFilePath']) is False:
    print('Processing file not exist.')
    sys.exit(1)

siteFullConfig = dict()
siteFullConfig['siteConfig'] = siteConfigData
siteFullConfig['tmuxConfig'] = tmuxConfigData

try:
    Watcher(DIRECTORY=siteConfigData['siteInfo']['observingPath'],
            PYFILE=siteConfigData['siteInfo']['processPyFilePath'],
            DBINFO=siteFullConfig,
            SITEID=siteConfigData['siteInfo']['id']).run()
except:
    print('unable to start main.py')
    sys.exit(1)

print('calling')
requests.get('http://0.0.0.0:5000/api/siteserviceapi/removesiteProcessId')