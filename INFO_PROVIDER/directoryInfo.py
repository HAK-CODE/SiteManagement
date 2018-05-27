'''
author: HAK
time  : 10:00 PM, 28/10/2017
'''
import os
import time

'''
class PATH_INFO_PROVIDER used to get information about directory.
'''
class PATH_INFO_PROVIDER:
    DIRECTORY = ""

    def __init__(self, path):
        self.DIRECTORY = path
        if self.DIRECTORY == '.':
            self.DIRECTORY = os.getcwd()

    #Is the path given a directory or not
    def ISDIR(self):
        return os.path.isdir(self.DIRECTORY)

    #Get info against a path defined
    def DIRSTAT(self):
        return os.stat(self.DIRECTORY)

    #Get the size of directory size
    def DIRSIZE(self):
        return os.stat(self.DIRECTORY).st_size

    #Get directory creation time
    def DIRCTIME(self):
        return time.ctime(os.path.getctime(self.DIRECTORY))

    #Get directory recent access time
    def DIRATIME(self):
        return time.ctime(os.path.getatime(self.DIRECTORY))

    #Get directory name for this object
    def DIRNAME(self):
        return self.DIRECTORY

    #Get directory path, name, creation time and size(BYTES)
    def DIRBASIC(self):
        dirpath, dirname = os.path.split(self.DIRECTORY)
        return dirpath, dirname, PATH_INFO_PROVIDER.DIRCTIME(self), PATH_INFO_PROVIDER.DIRSIZE(self)
