import boto3
import ntpath
bucketName = "reon-historian"
import datetime

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

        newkey=(self.key).replace('(','_')
        newkey = newkey.replace(')','')
        newkey = newkey.replace(' ','')
        response = s3.upload_file(self.key, bucketName, str(self.path)+"/"+folder+"/"+ntpath.basename(newkey))
        print(response)
        return