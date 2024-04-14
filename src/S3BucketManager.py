import os
import boto3
from tqdm import tqdm
from botocore.exceptions import BotoCoreError, ClientError
from log_config import configure_logging

class S3BucketManager:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.s3_client = self._initialize_client(aws_access_key_id, aws_secret_access_key, region_name)
        self.logger = configure_logging()

    def _initialize_client(self, aws_access_key_id, aws_secret_access_key, region_name):
        return boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def download_bucket_contents(self, bucket_name):
        self.logger.info(f"Downloading contents of bucket {bucket_name}")
        # Create the download directory if it doesn't exist
        if not os.path.exists(bucket_name):
            self.logger.info(f"Creating directory {bucket_name}")
            os.makedirs(bucket_name)

        # List objects within the bucket and download each one
        bucket_objects = self.s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in bucket_objects:
            for obj in tqdm(bucket_objects['Contents'], desc="Downloading"):
                file_name = obj['Key']
                file_path = os.path.join(bucket_name, file_name)
                self.s3_client.download_file(bucket_name, file_name, file_path)
                print(f"Downloaded {file_name} to {file_path}")

    def upload_directory_to_s3(self, bucket_name, directory_path):
        self.logger.info(f"Uploading directory {directory_path} to bucket {bucket_name}")

        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)

                if not file_path.endswith('.py'):
                    s3_key = os.path.relpath(file_path, directory_path)
                    self.s3_client.upload_file(file_path, bucket_name, s3_key)
                    self.logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
                    os.remove(file_path)
                    self.logger.info(f"Deleted {file_path}")

    def clean_bucket(self, bucket_name):
        self.logger.info(f"Cleaning bucket {bucket_name}")

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        self.s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                        self.logger.info(f"Deleted {obj['Key']} from {bucket_name}")

        except BotoCoreError as e:
            self.logger.error(f"An AWS SDK error occurred: {e}")
        except ClientError as e:
            self.logger.error(f"An AWS client error occurred: {e}")


if __name__ == "__main__":
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION')
    bucket_name = os.getenv('BUCKET_NAME')
    directory_path = os.getcwd()

    if aws_access_key_id is None or aws_secret_access_key is None or region is None or bucket_name is None:
        print(
            "Please set the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION and BUCKET_NAME environment variables")
        exit(1)

    manager = S3BucketManager(aws_access_key_id, aws_secret_access_key, region)
    manager.download_bucket_contents(bucket_name)
    manager.upload_directory_to_s3(bucket_name, directory_path)
    manager.clean_bucket(bucket_name)
