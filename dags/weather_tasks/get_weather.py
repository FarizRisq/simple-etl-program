from airflow.models import Variable
import requests
import json
from datetime import datetime
from pathlib import Path

def get_weather():
    WEATHER_API_KEY = Variable.get("WEATHER_API_KEY")
    CITY = "Jakarta"

    RAW_DIR = Path("/tmp/weather/raw")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    url = "https://api.weatherapi.com/v1/current.json"
    params = {
        "key": WEATHER_API_KEY,
        "q": CITY,
        "aqi": "no"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    raw_file = RAW_DIR / f"weather_{CITY}_{timestamp}.json"

    with open(raw_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"[EXTRACT] Saved raw file: {raw_file}")
    return str(raw_file)

if __name__ == "__main__":
    get_weather()

