import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from prefect import flow, task
from utils.s3_helper import list_files, move_file
from utils.parser import parse_ecom_filename
from utils.config import CONFIG
from dags.L1.ecom.construct_s3_key import build_ecom_key
import re

@task
def parse_trigger(s3_key: str):
    match = re.match(r"trigger_(.+)_(\d{8})\.txt", s3_key)

    if not match:
        raise ValueError("Invalid trigger format")

    return {
        "table": match.group(1),
        "date": match.group(2)  # YYYYMMDD
    }

@task
def convert_date(date):
    # YYYYMMDD → MMDDYYYY
    return f"{date[4:6]}{date[6:]}{date[:4]}"


@task
def get_matching_files(bucket: str, table: str, date: str):
    files = list_files(bucket)

    matched = []

    for f in files:
        filename = f.split("/")[-1]

        if filename.startswith("trigger_"):
            continue

        if table in filename and date in filename:
            matched.append(f)

    print("Matched:", matched)
    return matched

@task
def process_file(bucket, target_bucket, key):
    filename = key.split("/")[-1]

    info = parse_ecom_filename(filename)

    if not info:
        print(f"Skip invalid file: {filename}")
        return

    target_key = build_ecom_key(info, filename)

    move_file(bucket, key, target_bucket, target_key)

    print(f"Moved: {filename}")

@flow(name="ecom_rcv_to_l0")
def ecom_flow(s3_key: str):
    bucket = CONFIG["ecom"]["source_bucket"]
    target_bucket = CONFIG["target_bucket"]

    trigger = parse_trigger(s3_key)

    date_converted = convert_date(trigger["date"])

    files = get_matching_files(bucket, trigger["table"], date_converted)

    if not files:
        print("No files found")
        return
    
    for f in files:
        process_file(bucket, target_bucket, f)