import boto3

s3 = boto3.client("s3")

def list_files(bucket):
    response = s3.list_objects_v2(Bucket=bucket)
    return [obj["Key"] for obj in response.get("Contents", [])]

def move_file(source_bucket, key, target_bucket, target_key):
    s3.copy_object(
        Bucket=target_bucket,
        CopySource={'Bucket': source_bucket, 'Key': key},
        Key=target_key
    )

    s3.delete_object(
        Bucket=source_bucket,
        Key=key
    )