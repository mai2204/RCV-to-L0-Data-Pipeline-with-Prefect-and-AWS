import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from prefect import flow, task
from utils.s3_helper import list_files, move_file
from utils.parser import parse_ecom_filename
from utils.config import CONFIG
from dags.L1.ecom.construct_s3_key import build_ecom_key
from prefect import flow, task
from utils.s3_helper import list_files, move_file
from utils.config import CONFIG
import re

@task
def extract_date_from_trigger(s3_key: str):
    match = re.search(r"(\d{8})", s3_key)
    if not match:
        raise ValueError("Invalid trigger file name")

    return match.group(1)  # YYYYMMDD

from datetime import datetime

@task
def convert_trigger_to_date(date: str):
    # YYYYMMDD
    return datetime.strptime(date, "%Y%m%d")


@task
def get_files_by_date(bucket: str, trigger_date):
    files = list_files(bucket)

    matched = []

    for f in files:
        if f.startswith("trigger_"):
            continue

        filename = f.split("/")[-1]
        parts = filename.replace(".csv", "").split("_")

        date_str = parts[-1]  # MMDDYYYY

        try:
            # 🔥 FIX Ở ĐÂY
            file_date = datetime.strptime(date_str, "%m%d%Y")

            if file_date == trigger_date:
                matched.append(f)

        except Exception as e:
            print("Skip file (parse error):", filename)

    print("FILES TO PROCESS:", matched)
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

    # 1. lấy date từ trigger
    date_yyyymmdd = extract_date_from_trigger(s3_key)

    # 2. convert sang format của data file
    date_ddmmyyyy = convert_trigger_to_date(date_yyyymmdd)

    # 3. lọc file đúng ngày
    files = get_files_by_date(bucket, date_ddmmyyyy)

    if not files:
        print("No files to process")
        return

    # 4. xử lý từng file
    for f in files:
        process_file(bucket, target_bucket, f)

if __name__ == "__main__":
    ecom_flow.serve(name="ecom-deployment")