from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="f_lnc_kpi",
    description ="lawncompany kpi for shareholders",
    start_date = datetime(2022, 8, 3, tzinfo = KST),
    schedule_interval = '0 10 * * *',
    tags=["jungarui","KPI"],
    default_args={
        "owner": "jungarui",
        "retries": 3,  # Task가 실패한 경우, 3번 재시도
        "retry_delay": timedelta(minutes=3),  # 재시도하는 시간 간격은 3분
    }
) as dag:


    start = DummyOperator(
        task_id="start"
    )


    summary_kpi = BigQueryExecuteQueryOperator(
        task_id = 'summary_kpi',
        destination_dataset_table = 'lawtalk-bigquery.for_shareholder.f_lnc_kpi${{ ds_nodash }}',
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        sql = """
        select date(current_datetime('Asia/Seoul')) as batch_date
             , x.b_week
        	 , x.week_start_date
        	 , x.week_end_date
        	 , '로톡' as service
             , '050 전화 상담 연결 수' as cat
             , count(distinct a._id) as f_value
        from `common.d_calendar` x
        inner join `raw.callevents` a
        on x.full_date between date_sub(date(current_datetime('Asia/Seoul')),interval 7 day) and date_sub(date(current_datetime('Asia/Seoul')),interval 1 day)
        and FORMAT_TIMESTAMP('%Y%m%d', startedAT, 'Asia/Seoul') = x.b_date
        and a.type = 'profile'
        group by 1,2,3,4
        """
    )

start >> summary_kpi
