import argparse
import sys
import configparser
import os
from subprocess import check_output
from mailSender import Mail
from fileManager import FileManager
import time
from datetime import datetime as dt
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

parser = argparse.ArgumentParser(prog='MAILER',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Send mail watching the directory.')

parser.add_argument('--path', '-p', type=str, help="Specify full path to a directory.")
parser.add_argument('--time', '-t', type=int, default=10, help="Specify time frequency to watch directory.")
parser.add_argument('--maillist', '-l', type=str, help="Specify sender email list.")
parser.add_argument('--timeDiff', '-tf', type=int, default=15, help="Specify time difference threshold to trigger.")
parser.add_argument('--file', '-f', type=str, help="Specify file name.")
parser.add_argument('--screen', '-sr', type=str, help="Specify screen name.")
parser.add_argument('--siteName', '-st', type=str, help="Specify site name.")
parser.add_argument('--isCSV', '-csv', type=int, help="Specify if files are in json or csv.")
args = parser.parse_args()

programStart = False
emailtoDIH = False
emailtoClients = False

if args.path is None or \
        args.maillist is None or \
        args.timeDiff is None or \
        args.file is None or \
        args.screen is None or \
        args.siteName is None or \
        args.isCSV is None:
    print('One of the parameter missing.')
    sys.exit(0)

if os.path.isdir(args.path) is not True:
    print('Directory Not exist.')
    sys.exit(0)

f = FileManager()
f.setFile(args.file)


def sendMailtoDIH(screen=False, timeAhead=False):
    global emailtoDIH
    if not emailtoDIH:
        print('sending to dih')
        config = configparser.ConfigParser()
        config.read('./Config/mailList.ini')
        if config.has_section('dih.support'):
            senderList = [x[1] for x in config.items('dih.support')]
            config.clear()
            config.read('./Config/messages.cfg')
            messageList = [x[1] for x in config.items('dih.support')]
            service = Mail()
            service.send_mail(messageList[0],
                              messageList[1],
                              senderList)
            f.addLine(dt.now().strftime('%a %b %d %H:%M:%S %Y'), 1)
            emailtoDIH = True
    latestTime = dt.now()
    screenTime = f.getEmailTime(screen, timeAhead)
    if screen is True and timeAhead is False:
        if (latestTime - screenTime).days > 1:
            print('screen email set again')
            f.addLine(dt.now().strftime('%a %b %d %H:%M:%S %Y'), 1)
            emailtoDIH = False


def sendMailtoClients(receving=None, screen=False, timeAhead=False):
    global emailtoClients
    if not emailtoClients:
        print('sending to clients')
        config = configparser.ConfigParser()
        config.read('./Config/mailList.ini')
        if config.has_section(args.maillist):
            senderList = [x[1] for x in config.items(args.maillist)]
            ccList = [x[1] for x in config.items('dih.support')]
            config.clear()
            config.read('./Config/messages.cfg')
            service = Mail()
            if config.has_section(args.maillist) and timeAhead is False:
                service.send_mail(config.get(args.maillist, 'subject'),
                                  config.get(args.maillist, 'message',
                                             vars={'sitename': args.siteName, 'lasttime': str(receving)}),
                                  senderList,
                                  ccList)
                f.addLine(dt.now().strftime('%a %b %d %H:%M:%S %Y'), 2)
            else:
                service.send_mail('Files time inconsistent', 'Dear Reon files are ahead of time', senderList, ccList)
                f.addLine(dt.now().strftime('%a %b %d %H:%M:%S %Y'), 3)
            emailtoClients = True
    latestTime = dt.now()
    dirTime = f.getEmailTime(screen, timeAhead)
    print((latestTime - dirTime).days)
    if timeAhead is True and screen is False:
        if (latestTime - dirTime).days > 1:
            print('dir email set again')
            f.addLine(dt.now().strftime('%a %b %d %H:%M:%S %Y'), 3)
            emailtoClients = False
    elif timeAhead is False and screen is False:
        if (latestTime - dirTime).days > 1:
            print('dir email set again')
            f.addLine(dt.now().strftime('%a %b %d %H:%M:%S %Y'), 2)
            emailtoClients = False


class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if args.isCSV == 1:
            print(dt.strptime(filter(lambda x: x.isdigit(), os.path.basename(event.src_path)),'%Y%m%d%H%M%S'))
            if (round((dt.now() - dt.strptime(''.join(filter(lambda x: x.isdigit(), os.path.basename(event.src_path))),
                                              '%Y%m%d%H%M%S')).total_seconds()) / 60) < 0:
                sendMailtoClients(receving=None, screen=False, timeAhead=True)


event_handler = Handler()
observer = Observer()
observer.schedule(event_handler, args.path, recursive=False)
observer.start()
try:
    print('starting')
    while True:
        screenNames = check_output(["screen -ls; true"], shell=True)
        if not "." + args.screen + "\t(" in screenNames.decode('utf-8'):
            sendMailtoDIH(screen=True, timeAhead=False)
        if not programStart:
            f.addLine(str(time.ctime(os.path.getctime(args.path))), 0)
            programStart = True
        else:
            f.addLine(str(time.ctime(os.path.getctime(args.path))), 0)
            dirTime = f.getDirTime()
            currTime = dt.now()
            if int(round((currTime - dirTime).total_seconds()) / 60) > args.timeDiff:
                sendMailtoClients(receving=dirTime, screen=False, timeAhead=False)
        time.sleep(args.time)
except:
    observer.stop()
    print("Observer Stopped.")
observer.join()
