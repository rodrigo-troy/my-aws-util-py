import argparse
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tqdm import tqdm

from log_config import configure_logging


class S3BucketManager:
    def __init__(self, access_key_id, secret_access_key, region_name):
        self.s3_client = self._initialize_client(access_key_id, secret_access_key, region_name)
        self.logger = configure_logging()

    @staticmethod
    def _initialize_client(access_key_id, secret_access_key, region_name_value):
        return boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region_name_value
        )

    def download_bucket_contents(self, bucket_name, directory_path, extension):
        self.logger.info(f"Downloading contents of bucket {bucket_name}")

        if not os.path.exists(directory_path):
            self.logger.info(f"Creating directory {directory_path}")
            os.makedirs(directory_path)

        try:
            bucket_objects = self.s3_client.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in bucket_objects:
                for obj in tqdm(bucket_objects['Contents'], desc="Downloading"):
                    file_name = obj['Key']

                    if file_name.endswith(extension):
                        file_path = os.path.join(directory_path, file_name)
                        self.s3_client.download_file(bucket_name, file_name, file_path)
        except BotoCoreError as e:
            self.logger.error(f"An AWS SDK error occurred: {e}")
        except ClientError as e:
            self.logger.error(f"An AWS client error occurred: {e}")

    def upload_directory_to_s3(self, bucket_name, directory_path, extension):
        self.logger.info(f"Uploading directory {directory_path} to bucket {bucket_name}")

        if not os.path.exists(directory_path):
            self.logger.error(f"Directory {directory_path} does not exist")
            return

        try:
            for root, dirs, files in os.walk(directory_path):
                self.logger.info(f"Uploading files from {root} to s3://{bucket_name}")

                for file in files:
                    file_path = os.path.join(root, file)
                    self.logger.info(f"Uploading {file_path} to s3://{bucket_name}")

                    if file_path.endswith(extension):
                        s3_key = os.path.relpath(file_path, directory_path)
                        self.s3_client.upload_file(file_path, bucket_name, s3_key)
                        self.logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
                        os.remove(file_path)
                        self.logger.info(f"Deleted {file_path}")
        except BotoCoreError as e:
            self.logger.error(f"An AWS SDK error occurred: {e}")
        except ClientError as e:
            self.logger.error(f"An AWS client error occurred: {e}")

    def clean_bucket(self, bucket_name, extension):
        self.logger.info(f"Cleaning bucket {bucket_name}")

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith(extension):
                            self.s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                            self.logger.info(f"Deleted {obj['Key']} from {bucket_name}")
        except BotoCoreError as e:
            self.logger.error(f"An AWS SDK error occurred: {e}")
        except ClientError as e:
            self.logger.error(f"An AWS client error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload directory to S3 Bucket.')
    parser.add_argument('command', type=str, help='Command to execute. Options: "upload" or "download"')
    parser.add_argument('directory', type=str, help='Directory to upload.')
    parser.add_argument('extension', type=str, help='File extension to upload or download.')
    args = parser.parse_args()

    if args.extension is None or args.extension.strip() == "":
        parser.error("The `extension` argument is required")

    if args.command not in ['upload', 'download']:
        parser.error('Error: The command parameter must be either "upload" or "download".')

    if args.directory is None or args.directory.strip() == "":
        parser.error("The `directory` argument is required")

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_REGION')
    bucket_name = os.getenv('BUCKET_NAME')

    if not all([aws_access_key_id, aws_secret_access_key, region, bucket_name, args, args.directory]):
        raise EnvironmentError(
            "Please set the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME environment variables"
        )

    manager = S3BucketManager(aws_access_key_id, aws_secret_access_key, region)

    if args.command == 'upload':
        manager.upload_directory_to_s3(bucket_name, args.directory, args.extension)

    if args.command == 'download':
        manager.download_bucket_contents(bucket_name, args.directory, args.extension)
        manager.clean_bucket(bucket_name, args.extension)
