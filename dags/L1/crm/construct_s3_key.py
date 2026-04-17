def build_crm_key(info, filename):
    return f"data/crm/{info['table']}/{info['year']}/{info['month']}/{info['day']}/{filename}"