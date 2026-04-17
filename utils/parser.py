import re
from datetime import datetime

def parse_filename(filename):
    pattern = r"(\d{2})(\d{2})(\d{4})_([^_]+)_(.+)\.csv"
    match = re.match(pattern, filename)

    if not match:
        return None

    month, day, year, system, table = match.groups()

    date_obj = datetime.strptime(f"{day}{month}{year}", "%d%m%Y")

    return {
        "day": date_obj.strftime("%d"),
        "month": date_obj.strftime("%m"),
        "year": date_obj.strftime("%Y"),
        "system": system.lower(),
        "table": table.lower()
    }

def parse_ecom_filename(filename):
    pattern = r"(.+)_([^_]+)_(\d{2})(\d{2})(\d{4})\.csv"
    match = re.match(pattern, filename)

    if not match:
        return None

    table, system, month, day, year = match.groups()

    date_obj = datetime.strptime(f"{day}{month}{year}", "%d%m%Y")

    return {
        "day": date_obj.strftime("%d"),
        "month": date_obj.strftime("%m"),
        "year": date_obj.strftime("%Y"),
        "system": system.lower(),
        "table": table.lower()
    }