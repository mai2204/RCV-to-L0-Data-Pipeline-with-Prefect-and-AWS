import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

from utils.config import CONFIG
from utils.parser import parse_filename
from utils.s3_helper import list_files, move_file

# TASKS
@task
def fetch_files():
    bucket = CONFIG["crm"]

    files = list_files(bucket)   # ✔ phải có trước

    print("FILES:", files)

    if not files:
        print("No files found")
        return []

    print("TYPE:", type(files))
    print("FIRST TYPE:", type(files[0]))

    return files


@task
def process_file(key):
    logger = get_run_logger()

    filename = key.split("/")[-1]
    info = parse_filename(filename)

    logger.info(f"Filename: {filename}")
    logger.info(f"Parsed info: {info}")

    # VALIDATION

    if not info:
        logger.warning(f"Invalid filename: {filename}")
        return

    if info["system"] != "crm":
        logger.warning(f"Skip wrong system: {filename}")
        return

    # PROCESS

    target_key = (
        f"data/crm/{info['table']}/"
        f"{info['year']}/{info['month']}/{info['day']}/"
        f"{filename}"
    )

    move_file(CONFIG["crm"], key, CONFIG["target_bucket"], target_key)

    logger.info(f"Moved file: {filename}")

# FLOW
@flow(
    name="crm_rcv_to_l0",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def crm_flow():
    logger = get_run_logger()

    files = fetch_files()

    if not files:
        logger.warning("No CRM files found")
        return

    process_file.map(files)

# ENTRYPOINT
if __name__ == "__main__":
    crm_flow()