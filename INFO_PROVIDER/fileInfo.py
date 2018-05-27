'''
author: HAK
time  : 10:00 PM, 28/10/2017
'''

import os
import time

'''
class FILE_INFO is collection of functions applied 
to get information and meta data of files.
'''


class FILE_INFO:
    FILE_PATH = ""

    def __init__(self, FILE_PATH):
        self.FILE_PATH = FILE_PATH

    def FILESTAT(self):
        return os.stat(self.FILE_PATH)

    def FILECTIME(self):
        return time.ctime(os.path.getctime(self.FILE_PATH))

    def FILESIZE(self):
        return os.path.getsize(self.FILE_PATH)

    def FILEBASIC(self):
        filepath, filename = os.path.split(self.FILE_PATH)
        filename = filename.split('.')[0]
        fileext = os.path.splitext(self.FILE_PATH)[-1]
        return filepath, filename, fileext, FILE_INFO.FILECTIME(self), FILE_INFO.FILESIZE(self)
