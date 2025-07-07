from .singleton_base import SingletionBase
import boto3
import botocore
from .data_models import S3ConnectionParams

class S3BucketManager(SingletionBase):
    # gen new access keys
    s3_resource = None
    bucket_name: str = None
    s3 = None

    @classmethod
    def connect_to_s3(
        cls,
        bucket_name: str, 
        params: S3ConnectionParams):
        # set up session
        session = boto3.Session(
            aws_access_key_id=params.access_id,
            aws_secret_access_key=params.secret_access_key,
            region_name=params.region_name
        )

        cls.s3 = session.client('s3')
        cls.s3_resource = session.resource('s3')
        cls.bucket_name = bucket_name

    @classmethod
    def upload(cls, content, path):
        try:
            cls.__check_service_avalability(cls)

            object = cls.s3_resource.Object(cls.bucket_name, path)
            result = object.put(Body=content)
            print("------------- update to s3 bucket success -------------")
            print(result)
        except Exception as e:
            raise(e)
        
    @classmethod
    def load(cls, path_to_file):
        try:
            cls.__check_service_avalability(cls)
            obj = cls.s3_resource.Object(cls.bucket_name, path_to_file)
            
            return obj
        except Exception as e:
            raise(e)

    @classmethod
    def file_exist(cls, bucket, key):
        try:
            cls.s3.head_object(Bucket=bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise

    def __check_service_avalability(cls):
        if cls.s3_resource == None:
            raise ValueError("S3 service unavalaible")
        if cls.bucket_name == None:
            raise ValueError("Bucket name is undefined")