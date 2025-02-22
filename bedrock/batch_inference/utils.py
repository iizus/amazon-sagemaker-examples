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



def wait_until_complete(get_status, stopped_status:tuple):
    status:str = get_status()
    print(status)
    while status not in stopped_status:
        status, time_delta = __wait(get_status=get_status, monitored_status=status)
        print(f" | {time_delta} sec")
        print(status)


from time import perf_counter, sleep

def __wait(get_status, monitored_status:str):
    status:str = monitored_status

    started_time = perf_counter()
    while status == monitored_status:
        status:str = get_status()
        sleep(1)
    ended_time = perf_counter()

    time_delta = round(ended_time - started_time)
    return status, time_delta