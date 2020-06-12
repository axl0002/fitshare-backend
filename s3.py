import boto3
import logging
from botocore.exceptions import ClientError

# IMPORTANT
# create ~/.aws/credentials
# [default]
# aws_access_key_id = YOUR_ACCESS_KEY_ID
# aws_secret_access_key = YOUR_SECRET_ACCESS_KEY

# create ~/.aws/config
# [default]
# region = YOUR_PREFERRED_REGION

AWS_BUCKET_NAME = 'fitshare-app'


def init():
    return boto3.client('s3')


def upload_file(filename, key):
    s3_client = init()
    try:
        s3_client.upload_file(filename, AWS_BUCKET_NAME, key)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file_obj(fileobj, key):
    s3_client = init()
    try:
        s3_client.upload_fileobj(fileobj, AWS_BUCKET_NAME, key)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def show_file(key):
    s3_client = init()
    try:
        response_url = s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': AWS_BUCKET_NAME,
                                                                'Key': key})
    except ClientError as e:
        logging.error(e)
        return None
    return response_url


def download_as(key, filename_to):
    s3_client = init()
    s3_client.download_file(AWS_BUCKET_NAME, key, filename_to)


def delete(key):
    s3_resource = boto3.resource('s3')
    s3_resource.Object(AWS_BUCKET_NAME, key).delete()


if __name__ == '__main__':
    url = show_file('test.mp4')
    print(url)
