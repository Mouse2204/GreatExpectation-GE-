import boto3
from botocore.exceptions import ClientError

def create_bucket():
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9010',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    bucket_name = "datalake"

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Đã tạo bucket '{bucket_name}' thành công!")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyOwnedByYou':
            print(f"Bucket '{bucket_name}' đã tồn tại.")
        else:
            print(f"Lỗi: {e}")

if __name__ == "__main__":
    create_bucket()