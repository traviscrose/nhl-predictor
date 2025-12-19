import requests

BASE_URL = "https://api-web.nhle.com/v1"

def get_schedule():
    """
    Fetch the current NHL schedule from the API.
    Returns JSON.
    """
    r = requests.get(f"{BASE_URL}/schedule/now", timeout=10)
    r.raise_for_status()
    return r.json()
