import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from prefect import flow, task
from utils.s3_helper import list_files, move_file
from utils.parser import parse_ecom_filename
from utils.config import CONFIG
from dags.L1.ecom.construct_s3_key import build_ecom_key


@task
def get_files(bucket):
    return list_files(bucket)


@task
def process_file(bucket, target_bucket, key):
    filename = key.split("/")[-1]

    info = parse_ecom_filename(filename)

    print("FILENAME:", filename)
    print("INFO:", info)

    if not info or info["system"] != "ecom":
        return

    target_key = build_ecom_key(info, filename)

    move_file(bucket, key, target_bucket, target_key)

    return f"Moved {filename}"

@flow(name="ecom_rcv_to_l0")
def ecom_flow():
    bucket = CONFIG["ecom"]
    target_bucket = CONFIG["target_bucket"]

    files = get_files(bucket)

    for key in files:
        process_file(bucket, target_bucket, key)

if __name__ == "__main__":
    ecom_flow()