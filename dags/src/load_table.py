from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path

def load_table(files: dict):
    """
    files = {
        "raw_file": "/tmp/weather/raw/xxx.json",
        "csv_file": "/tmp/weather/clean/xxx.csv"
    }
    """

    BUCKET_NAME = "data-cuaca-jakarta"
    DEST_FOLDER = "weatherapi/raw"

    raw_file = Path(files["raw_file"])
    csv_file = Path(files["csv_file"])

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    # =========================
    # UPLOAD RAW JSON
    # =========================
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=f"{DEST_FOLDER}/{raw_file.name}",
        filename=str(raw_file)
    )

    # =========================
    # UPLOAD CLEAN CSV
    # =========================
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=f"{DEST_FOLDER}/{csv_file.name}",
        filename=str(csv_file)
    )

    print("===================================")
    print("LOAD TO GCS SUCCESS ✅")
    print(f"RAW  : gs://{BUCKET_NAME}/{DEST_FOLDER}/{raw_file.name}")
    print(f"CSV  : gs://{BUCKET_NAME}/{DEST_FOLDER}/{csv_file.name}")
    print("===================================")
