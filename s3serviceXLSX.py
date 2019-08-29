import boto3
import ntpath
bucketName = "reon-historian"
import datetime
from time import sleep

class S3:
    def __init__(self, key=None, path=None):
        self.key = key
        self.path = path

    def send(self):
        s3 = boto3.client('s3')
        print(self.key)
        print(self.path)
        folder = str(datetime.datetime.now()).split(' ')[0]
        if str(self.path).startswith('/'):
            self.path = str(self.path[1:])
        response = s3.upload_file(self.key, bucketName, str(self.path)+"/"+folder+"/"+ntpath.basename(self.key))
        print(response)
        sleep (5)
        return