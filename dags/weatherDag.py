from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from weather_tasks.get_weather import get_weather
from weather_tasks.transform_weather import transform_weather
from weather_tasks.load_table import load_table


# ===============================
# DEFAULT ARGS
# ===============================
default_args = {
    "owner": "Fariz",
    "depends_on_past": False,
    "email": ["farizr.official@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

# ===============================
# DAG DEFINITION
# ===============================
with DAG(
    dag_id="weather_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=get_weather,
    )

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
        op_args=["{{ ti.xcom_pull(task_ids='extract_weather') }}"],
    )

    load = PythonOperator(
        task_id="load_table",
        python_callable=load_table,
        op_args=["{{ ti.xcom_pull(task_ids='transform_weather') }}"],
    )

    extract >> transform >> load
