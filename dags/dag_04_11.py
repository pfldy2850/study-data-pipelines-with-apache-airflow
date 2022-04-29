from urllib import request

import airflow
from airflow.operators.python import PythonOperator
from airflow import DAG


dag = DAG(
    dag_id="dag_4_11",
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
    task_id="task_4_11_get_data_1",
    python_callable=_get_data,
    op_args=["/tmp/wikipageviews.gz"],
    dag=dag
)


get_data = PythonOperator(
    task_id="task_4_11_get_data_2",
    python_callable=_get_data,
    op_kwargs={
        "output_path": "/tmp/wikipageviews.gz"
    },
    dag=dag
)


get_data = PythonOperator(
    task_id="task_4_11_get_data_3",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz"
    },
    dag=dag
)
