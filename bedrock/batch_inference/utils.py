from datetime import timezone, datetime, timedelta

def get_formatted_time() -> str:
    now = datetime.now(timezone.utc)
    time_delta = timedelta(hours = 9)
    jst = timezone(time_delta)
    formatted_time = now.astimezone(jst).strftime("%Y%m%d-%H%M%S")
    return formatted_time


from yaml import safe_load

def load_config(file_name:str="config.yaml") -> dict:
    with open(file_name) as config_file:
        return safe_load(config_file)