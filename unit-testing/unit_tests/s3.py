import boto3
from exceptions import (
    AccessDeniedByLock,
    BucketAlreadyExists,
    BucketNeedsToBeNew,
    MissingBucket,
    InvalidBucketName,
    InvalidPart,
    InvalidRequest,
    EntityTooSmall,
    MissingKey,
    InvalidNotificationDestination,
    MalformedXML,
    InvalidStorageClass,
    InvalidTargetBucketForLogging,
    CrossLocationLoggingProhibitted,
    NoSuchPublicAccessBlockConfiguration,
    InvalidPublicAccessBlockConfiguration,
    WrongPublicAccessBlockAccountIdError,
    NoSuchUpload,
    ObjectLockConfigurationNotFoundError,
    InvalidTagError,
)

class MyS3Client:
    def __init__(self, region_name="us-east-1"):
        self.client = boto3.client("s3", region_name=region_name)
    
    def get_bucket(self, bucket_name):
        try:
            response = self.client.list_buckets()
            for bucket in response["Buckets"]:
                return bucket if bucket["Name"] == bucket_name else ['Bucket not found ']
        except KeyError:
            raise MissingBucket(bucket=bucket_name)
    
    def list_buckets(self):
        """Returns a list of bucket names."""
        response = self.client.list_buckets()
        return [bucket["Name"] for bucket in response["Buckets"]]

    def list_objects(self, bucket_name, prefix):
        """Returns a list all objects with specified prefix."""
        response = self.client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix,
        )
        return [object["Key"] for object in response["Contents"]]

    def get_bucket_location(self, bucket_name):
        """Returns bucket location"""
        bucket = self.get_bucket(bucket_name)
        return bucket.location


    