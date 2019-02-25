import boto3

bucketName = "reon-archival"

class S3:
    def __init__(self, key=None, path=None):
        self.key = key
        self.path = path

    def send(self):
        s3 = boto3.client('s3')
        print("key "+str(self.key))
        print("key " + str(self.path))
        response = s3.upload_file(self.key, bucketName, self.path)
        print(response)
        return