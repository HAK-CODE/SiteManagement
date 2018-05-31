'''
author: HAK
time  : 10:00 PM, 28/10/2017
'''

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import time
from INFO_PROVIDER.fileInfo import FILE_INFO
from INFO_PROVIDER.directoryInfo import PATH_INFO_PROVIDER
import subprocess
from colorama import Fore
import requests
import sys
import json
import emailWatcher
import datetime as dt

'''
An event handler that use to listen triggered events from FileSystemEvent
This class onsist of one function on_created which provide two basic event logging generators
1. When a file is created
2. When a directory is created

Returns:
-------FILE--------
1. Path
2. Name
3. Extension
4. Time of creation
5. Size (BYTES)
------------------- 

-----DIRECTORY-----
1. Path
2. Name
3. Time of creation
4. Size (BYTES)
-------------------
'''


class Handler(FileSystemEventHandler):
    data = None

    def objectInfo(self, getInfo):
        self.data = getInfo

    def on_modified(self, event):
        print("Modified " + event.src_path)

    def on_created(self, event):
        PATH = event.src_path
        # Check if a event is generated for directory or not
        if event.is_directory:
            DIR = PATH_INFO_PROVIDER(PATH)
            print(Fore.LIGHTCYAN_EX,
                  '----------------------------- NEW DIRECTORY CREATED -----------------------------')
            print(Fore.LIGHTCYAN_EX, "PATH  : ", DIR.DIRBASIC()[0])
            print(Fore.LIGHTCYAN_EX, "NAME  : ", DIR.DIRBASIC()[1])
            print(Fore.LIGHTCYAN_EX, "CTIME : ", DIR.DIRBASIC()[2])
            print(Fore.LIGHTCYAN_EX, "SIZE  : ", DIR.DIRBASIC()[3], "BYTES")
            print(Fore.LIGHTCYAN_EX,
                  '---------------------------------------------------------------------------------', Fore.RESET)
        else:
            FILE = FILE_INFO(event.src_path)
            print(Fore.LIGHTCYAN_EX)
            print('-------------------------------- NEW FILE CREATED -------------------------------')
            print("PATH  : ", FILE.FILEBASIC()[0])
            print("NAME  : ", FILE.FILEBASIC()[1])
            print("EXT   : ", FILE.FILEBASIC()[2])
            print("CTIME : ", FILE.FILEBASIC()[3])
            print("SIZE  : ", FILE.FILEBASIC()[4], "BYTES")
            print('---------------------------------------------------------------------------------', Fore.RESET)
            CHG_PATH = '\"' + event.src_path + '\"' if ' ' in event.src_path else event.src_path
            #self.data['email'].sendMailForInconsistentFiles()
            # may call call instead of Popen
            self.data['fileReceived'] = CHG_PATH
            subprocess.Popen(['python3', self.data['pyfile'], str(self.data)])


'''
class: watcher
used to watch for events create and modified
'''


class Watcher:
    PYFILE = None
    DBINFO = None
    DIRECTORY = None
    SITENAME = None
    MAIL = None

    def __init__(self, DIRECTORY=None, PYFILE=None, DBINFO=None, SITEID=None):
        self.PYFILE = PYFILE
        self.DBINFO = DBINFO
        self.DIRECTORY = DIRECTORY
        self.SITEID = SITEID

    def getInfo(self, email=None):
        return {
            'pyfile': self.PYFILE,
            'db': self.DBINFO,
            'fileReceived': None,
            'email': 'No'
        }

    def run(self):
        try:
            event_handler = Handler()
            emailService = requests.get('https://x45k5kd3hj.execute-api.us-east-2.amazonaws.com/dev/emailconfig?siteId='+self.SITEID)
            emailConfig = json.loads(emailService.content.decode('utf-8'))
            email = emailWatcher.EmailHandler(path=self.DIRECTORY,
                                              timeThreshold=emailConfig['timeDiffer'],
                                              siteId=self.SITEID,
                                              inconsistentFiles=emailConfig['checkInconsistentFiles'],
                                              inconsistenTime=emailConfig['checkTimeInconsistencies'])
            event_handler.objectInfo(getInfo=self.getInfo(email=email))
            observer = Observer()
            observer.schedule(event_handler, self.DIRECTORY, recursive=True)
            observer.start()
        except:
            observer.stop()
            #requests.put('http://0.0.0.0:5000/api/emailapi/emailstatusupdate?siteId='+str(self.SITEID)+'&serviceStatus='+str(0))
            print("Observer Stopped Unexpectedly.")
            sys.exit(1)
        try:
            while True:
                email.update()
                time.sleep(emailConfig['monitoringPing'])
        except:
            observer.stop()
            print("Observer Stopped Manually.")
            #requests.put('http://0.0.0.0:5000/api/emailapi/emailstatusupdate?siteId=' + str(self.SITEID) + '&serviceStatus=' + str(0))
            #requests.put('http://0.0.0.0:5000/api/emailapi/sendmailtogroupid?siteId=' + str(self.SITEID) +
            #             '&isMaintainance=' + str(1) +
            #             '&emailType=' + str('INTERNAL_ISSUE') +
            #             '&datetime=' + str(dt.datetime.now()))
        observer.join()
