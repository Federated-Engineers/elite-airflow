import boto3
from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key()
print(fernet_key.decode())


s3 = boto3.resource('s3')
for bucket in s3.buckets.all():
    print(bucket.name)
