def build_ecom_key(info, filename):
    return (
        f"data/ecom/{info['table']}/"
        f"{info['year']}/{info['month']}/{info['day']}/"
        f"{filename}"
    )