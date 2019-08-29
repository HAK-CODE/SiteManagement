import os
import time


class IOoperation:
    FILENAME = None
    FILESIZE = None
    COUNT = 2
    SLEEP = 4

    def setFile(self, FILENAME):
        if os.path.isfile(FILENAME):
            print('file exist')
            self.FILENAME = FILENAME

    def isFileRelease(self):
        FLAG = False
        self.FILESIZE = os.path.getsize(self.FILENAME)
        while True:
            self.FILESIZE = os.path.getsize(self.FILENAME)
            time.sleep(self.SLEEP)
            if self.COUNT == 0:
                break
            currSize = os.path.getsize(self.FILENAME)
            if currSize != self.FILESIZE:
                FLAG = False
            else:
                FLAG = True
            if FLAG is not False:
                self.COUNT -= 1
        return True
