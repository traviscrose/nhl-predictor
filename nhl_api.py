import requests

BASE_URL = "https://api-web.nhle.com/v1"

def get_schedule(start_date: str, end_date: str):
    """
    Fetch NHL schedule from the official NHL API for a date range.
    """
    url = f"{BASE_URL}/schedule?startDate={start_date}&endDate={end_date}"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()
