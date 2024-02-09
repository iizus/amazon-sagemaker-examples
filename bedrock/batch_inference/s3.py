from boto3 import resource

class Bucket:
    def __init__(self, name:str) -> Bucket:
        s3 = resource('s3')
        self.bucket:s3.Bucket = s3.Bucket(name=name)

    def Object(self, key:str):
        return self.bucket.Object(key=key)