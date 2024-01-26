from datetime import timezone, datetime, timedelta

def get_formatted_time() -> str:
    now = datetime.now(timezone.utc)
    time_delta = timedelta(hours = 9)
    jst = timezone(time_delta)
    formatted_time = now.astimezone(jst).strftime("%Y%m%d-%H%M%S")
    return formatted_time