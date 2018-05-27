import os
from datetime import datetime as dt
import time
import requests
import sys


class EmailHandler:
    path = None
    timeThreshold = None
    siteId = None
    checkForInconsistentFiles = False
    ForInconsistentFiles = None
    checkForTimeInconsistencies = False
    ForTimeInconsistencies = None
    mailSentClients = False
    mailSentDevs = False
    timeAhead = None
    noReceiving = None
    inconsistentFiles = None
    directoryCreationTime = None

    def __init__(self, path, timeThreshold, siteId, inconsistentFiles, inconsistenTime):
        self.path = path
        self.timeThreshold = timeThreshold
        self.siteId = siteId
        self.checkForInconsistentFiles = inconsistentFiles
        self.checkForTimeInconsistencies = inconsistenTime
        self.directoryCreationTime = str(time.ctime(os.path.getctime(self.path)))
        try:
            if requests.put('http://0.0.0.0:5000/api/emailapi/emailstatusupdate?siteId=' + self.siteId + '&serviceStatus=1').status_code == 200:
                print('Email service successfully started')
            else:
                print('failed to start')
        except:
            print('Failed to start mail service at initialization.')
            sys.exit(1)

    def update(self):
        self.directoryCreationTime = str(time.ctime(os.path.getctime(self.path)))
        currTime = dt.now()
        if int(round((currTime - dt.strptime(self.directoryCreationTime.rstrip(),
                                             '%a %b %d %H:%M:%S %Y')).total_seconds()) / 60) > self.timeThreshold and \
                self.mailSentClients is False:
            print('sending mail to clients')
            requests.put('http://0.0.0.0:5000/api/emailapi/sendmailtogroupid?siteId=' + str(self.siteId) +
                         '&isMaintainance=' + str(0) +
                         '&emailType=' + str('RECEIVING_STOP') +
                         '&datetime=' + self.directoryCreationTime)
            self.mailSentClients = True
            self.noReceiving = str(dt.now().strftime('%a %b %d %H:%M:%S %Y'))
        self.updateVar()
        print('updating')

    def updateVar(self):
        currTime = dt.now()
        if self.mailSentClients is True:
            if self.noReceiving is None:
                if (currTime - dt.strptime(self.noReceiving.rstrip(), '%a %b %d %H:%M:%S %Y')).days >= 1:
                    self.mailSentClients = False
                    self.noReceiving = None

            if self.checkForInconsistentFiles is None:
                if (currTime - dt.strptime(self.ForInconsistentFiles.rstrip(), '%a %b %d %H:%M:%S %Y')).days >= 1:
                    self.mailSentClients = False
                    self.checkForInconsistentFiles = None

            if self.checkForTimeInconsistencies is None:
                if (currTime - dt.strptime(self.ForTimeInconsistencies.rstrip(), '%a %b %d %H:%M:%S %Y')).days >= 1:
                    self.mailSentClients = False
                    self.checkForTimeInconsistencies = None

    def sendMailForInconsistentFiles(self):
        if self.checkForInconsistentFiles:
            if self.ForInconsistentFiles is None:
                self.ForInconsistentFiles = str(dt.now().strftime('%a %b %d %H:%M:%S %Y'))
                self.mailSentClients = True

    def sendMailForInconsistentTime(self):
        if self.checkForTimeInconsistencies:
            if self.ForTimeInconsistencies is None:
                self.ForTimeInconsistencies = str(dt.now().strftime('%a %b %d %H:%M:%S %Y'))
                self.mailSentClients = True
