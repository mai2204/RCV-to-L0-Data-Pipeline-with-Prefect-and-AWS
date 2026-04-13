from prefect import flow, task
import boto3
from datetime import datetime

REGION = "ap-northeast-1"
L0_BUCKET = "dev-prefect-l0-aws-bucket"

# ---------- S3 ----------
def get_s3():
    return boto3.client("s3", region_name=REGION)

# ---------- LIST FILE ----------
@task
def list_files(bucket):
    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=bucket)

    return [obj["Key"] for obj in response.get("Contents", [])]

# ---------- PARSE ----------
@task
def parse_filename(filename):
    name = filename.replace(".csv", "")
    parts = name.split("_")

    # CASE 1: MMDDYYYY_system_table
    if parts[0].isdigit():
        date_str = parts[0]
        system = parts[1]
        table = "_".join(parts[2:])

    # CASE 2: schema_table_system_MMDDYYYY
    elif parts[-1].isdigit():
        date_str = parts[-1]
        system = parts[-2]
        table = parts[1]   # items
        # schema = parts[0] (nếu cần)

    else:
        raise ValueError(f"Unknown format: {filename}")

    # parse date
    try:
        date = datetime.strptime(date_str, "%m%d%Y")
    except:
        raise ValueError(f"Invalid date: {filename}")

    return {
        "system": system,
        "table": table,
        "date": date,
        "filename": filename
    }

# ---------- BUILD PATH ----------
@task
def build_path(meta):
    d = meta["date"]

    return (
        f"data/{meta['system']}/"
        f"{meta['table']}/"
        f"{d.year}/{d.month:02d}/{d.day:02d}/"
        f"{meta['filename']}"
    )

# ---------- COPY ----------
@task
def copy_file(src_bucket, src_key, dest_key):
    s3 = get_s3()

    s3.copy_object(
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Bucket=L0_BUCKET,
        Key=dest_key
    )

    print(f"{src_key} → {dest_key}")

# ---------- FLOW ----------
@flow
def rcv_to_l0_flow():

    src_buckets = [
        "dev-crm-prefect-rcv-aws-bucket",
        "dev-ecom-prefect-rcv-aws-bucket"
    ]

    for bucket in src_buckets:

        files = list_files(bucket)

        for file in files:
            meta = parse_filename(file)
            dest_path = build_path(meta)
            copy_file(bucket, file, dest_path)

# ---------- RUN ----------
if __name__ == "__main__":
    rcv_to_l0_flow()