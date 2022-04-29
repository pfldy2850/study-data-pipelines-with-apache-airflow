from urllib import request

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def _get_data(output_path, execution_date):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "http://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


def _fetch_pageviews(filename, filename_sql, pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open(filename, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open(filename_sql, "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )


with DAG(
    dag_id="dag_04_15",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/",
    catchup=False
) as dag:
    output_path = "/opt/airflow/tmp/wikipageviews"

    get_data = PythonOperator(
        task_id="dag_04_15_get_data",
        python_callable=_get_data,
        op_kwargs={
            "output_path": f"{output_path}.gz"
        },
        dag=dag
    )

    extract_gz = BashOperator(
        task_id="dag_04_15_extract_gz",
        bash_command=(
            "gunzip --force "
            f"{output_path}.gz"
        ),
        dag=dag
    )

    fetch_pageviews = PythonOperator(
        task_id="dag_04_15_fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "filename": f"{output_path}",
            "filename_sql": f"{output_path}.sql",
            "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
        },
        dag=dag
    )

    write_to_postgres = PostgresOperator(
        task_id="dag_04_15_write_to_postgres",
        postgres_conn_id="postgres",
        sql=f"{output_path}.sql",
        dag=dag
    )

    get_data >> extract_gz >> fetch_pageviews >> write_to_postgres
