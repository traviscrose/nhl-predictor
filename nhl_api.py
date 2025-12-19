import requests

BASE_URL = "https://api-web.nhle.com/v1"

def get_schedule_for_date(date_str: str):
    """
    Fetch NHL schedule for a specific YYYY-MM-DD date.
    """
    url = f"{BASE_URL}/schedule/{date_str}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()
