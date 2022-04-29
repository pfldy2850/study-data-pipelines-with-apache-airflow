from urllib import request

import airflow
from airflow.operators.python import PythonOperator
from airflow import DAG


dag = DAG(
    dag_id="dag_4_5",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly"
)


def _get_data(execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "http://dumps.wikimedia.org/other/pageviews"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/tmp/wikipageviews.gz"
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="dag_4_5",
    python_callable=_get_data,
    dag=dag
)
