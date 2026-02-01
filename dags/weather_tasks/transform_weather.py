import json
import pandas as pd
from pathlib import Path

def transform_weather(raw_file_path: str):
    """
    raw_file_path: path JSON dari extract_weather
    """

    RAW_FILE = Path(raw_file_path)
    CLEAN_DIR = Path("/tmp/weather/clean")
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # =========================
    # READ RAW JSON
    # =========================
    with open(RAW_FILE, "r") as f:
        raw_data = json.load(f)

    # =========================
    # TRANSFORM / CLEAN
    # =========================
    clean_data = {
        "city": raw_data["location"]["name"],
        "country": raw_data["location"]["country"],
        "local_time": raw_data["location"]["localtime"],
        "temperature_c": raw_data["current"]["temp_c"],
        "feelslike_c": raw_data["current"]["feelslike_c"],
        "humidity_pct": raw_data["current"]["humidity"],
        "wind_kph": raw_data["current"]["wind_kph"],
        "condition": raw_data["current"]["condition"]["text"]
    }

    df = pd.DataFrame([clean_data])

    # =========================
    # SAVE CLEAN CSV
    # =========================
    csv_file = CLEAN_DIR / RAW_FILE.name.replace(".json", ".csv")
    df.to_csv(csv_file, index=False)

    print(f"[TRANSFORM] Saved clean file: {csv_file}")

    # =========================
    # RETURN FOR NEXT TASK
    # =========================
    return {
        "raw_file": str(RAW_FILE),
        "csv_file": str(csv_file)
    }
