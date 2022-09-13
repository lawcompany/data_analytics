from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="batch_lawtalk_mart",
    description ="lawtalk data mart",
    start_date = datetime(2022, 9, 11, tzinfo = KST),
    schedule_interval = '0 5 * * *',
    tags=["jungarui","lawtalk","mart"],
    default_args={
        "owner": "jungarui"#,
        #"retries": 3,  # Task가 실패한 경우, 3번 재시도
        #"retry_delay": timedelta(minutes=3),  # 재시도하는 시간 간격은 3분
    }
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    query="delete from `lawtalk-bigquery.mart.lja_test'` where b_date = '{{next_ds}}'"
    delete_test = BigQueryOperator(
        task_id = 'delete_test',
        use_legacy_sql = False,
        sql = query
    )

start >> delete_test
