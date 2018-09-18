import os
import time


class IOoperation:
    FILENAME = None
    FILESIZE = None
    COUNT = 5
    TIMEOUT = 10

    def setFile(self, FILENAME):
        if os.path.isfile(FILENAME):
            print('file exist')
            self.FILENAME = FILENAME

    def isFileRelease(self):
        self.FILESIZE = os.path.getsize(self.FILENAME)
        while True:
            if self.TIMEOUT == 0:
                break
            currSize = os.path.getsize(self.FILENAME)
            if currSize != self.FILESIZE:
                print('size not same')
                self.FILESIZE = currSize
            else:
                while True:
                    time.sleep(1)
                    if self.COUNT == 0:
                        self.COUNT = 2
                        break
                    self.COUNT -= 1
                if currSize == self.FILESIZE:
                    print('size same')
                    break
            self.TIMEOUT -= 1
        return True
