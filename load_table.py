from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path

def load_table(**context):
    ti = context["ti"]

    files = ti.xcom_pull(task_ids="transform_weather")

    if not isinstance(files, dict):
        raise ValueError(f"Expected dict from XCom, got {type(files)}")

    BUCKET_NAME = "data-cuaca-jakarta"
    DEST_FOLDER = "weatherapi/raw"

    raw_file = Path(files["raw_file"])
    csv_file = Path(files["csv_file"])

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=f"{DEST_FOLDER}/{raw_file.name}",
        filename=str(raw_file),
    )

    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=f"{DEST_FOLDER}/{csv_file.name}",
        filename=str(csv_file),
    )

    print("===================================")
    print("LOAD TO GCS SUCCESS ✅")
    print(f"RAW  : gs://{BUCKET_NAME}/{DEST_FOLDER}/{raw_file.name}")
    print(f"CSV  : gs://{BUCKET_NAME}/{DEST_FOLDER}/{csv_file.name}")
    print("===================================")
