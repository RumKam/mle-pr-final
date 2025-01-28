import boto3
import csv, io


class Config:
    # Актуальные ключи
    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY = ''

    S3_SERVICE_NAME = 's3'
    S3_ENDPOINT_URL = 'https://storage.yandexcloud.net'


def get_session():
    session = boto3.session.Session()

    return session.client(
        service_name=Config.S3_SERVICE_NAME,
        endpoint_url=Config.S3_ENDPOINT_URL,
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
    )


def save_to_storage(data: dict, key_name: str, bucket_name: str, cohort_num: int,
                    report_name: str, is_project=False) -> str:

    s3 = get_session()

    bucket_name.replace(' ', '')

    full_key_name = 'test/test.sh'

    path_for_result = f'{Config.S3_ENDPOINT_URL}/{bucket_name}/{full_key_name}'

    # csv module can write data in io.StringIO buffer only
    s = io.StringIO()
    csv.writer(s).writerows(data)
    s.seek(0)

    # convert it to bytes and write to buffer
    buf = io.BytesIO()
    buf.write(s.getvalue().encode())
    buf.seek(0)

    s3.upload_fileobj(buf, Bucket=bucket_name, Key=full_key_name)

    return path_for_result


def get_list_objects(bucket_name):
    s3 = get_session()

    if s3.list_objects(Bucket=bucket_name).get('Contents'):
        for key in s3.list_objects(Bucket=bucket_name)['Contents']:
            print(key['Key'])
    else:
        print('Bucker empty')
    return True


def delete_all_obj(bucket_name):
    s3 = boto3.resource(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY)

    bucket = s3.Bucket(bucket_name)

    # Deleting all versions (works for non-versioned buckets too).
    bucket.object_versions.delete()

    # Aborting all multipart uploads, which also deletes all parts.
    for multipart_upload in bucket.multipart_uploads.iterator():
        # Part uploads that are currently in progress may or may not succeed,
        # so it might be necessary to abort a multipart upload multiple times.
        while len(list(multipart_upload.parts.all())) > 0:
            multipart_upload.abort()


if __name__ == '__main__':
    bucket_name = 's3-student-mle-20240824-ff21c1bdfa' 

    # delete_all_obj(bucket_name)

    res = get_list_objects(bucket_name)

    res = save_to_storage(
        {'1':'1','2':'2','3':'4'},
        'key_name',
        bucket_name,
        '...',
        '...'
    )
    print(res)

    res = get_list_objects(bucket_name)