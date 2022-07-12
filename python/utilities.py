import boto3


def download_from_s3(local_file_path, bucket_name, bucket_filepath):
   s3 = boto3.client('s3')
   with open(local_file_path, "wb") as f:
       s3.download_fileobj(bucket_name, bucket_filepath, f)


def upload_to_s3(local_file_path, bucket_name, bucket_filepath):
   s3 = boto3.client('s3')
   with open(local_file_path, "rb") as f:
       s3.upload_fileobj(f, bucket_name, bucket_filepath)