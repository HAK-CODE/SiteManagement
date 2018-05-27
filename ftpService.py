import ftplib
import traceback
import os


class FTP:

    def __init__(self, filePath=None, serverPath=None):
        self.filePath = filePath
        self.serverPath = serverPath

    def sendFTP(self):
        try:
            try:
                session = ftplib.FTP_TLS(host='ftpreonenergy.qosenergy.com',
                                         passwd="@Reonenergy92",
                                         user="reonenergy")
                session.cwd(self.serverPath)
                file = open(self.filePath, 'rb')  # file to send
                session.storbinary('STOR ' + os.path.basename(self.filePath), file)
                file.close()
            except ftplib.all_errors:
                return False
            finally:
                session.quit()
                #print('finished sending')
                return True
        except:
            #print('failed sending')
            traceback.print_exc()
            return False

