from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="shjeong_tmp_user_advice",
    description ="shjeong_tmp_user_advice",
    start_date = datetime(2022, 6, 26, tzinfo = KST),
    schedule_interval = '0 15 * * *',
    tags=["shjeong","advice"],
    default_args={  
        "owner": "shjeong", 
        "retries": 3,  # Task가 실패한 경우, 3번 재시도
        "retry_delay": timedelta(minutes=3),  # 재시도하는 시간 간격은 3분
    }
) as dag:


    start = DummyOperator(
        task_id="start"
    )

    advice_user_snapshot = BigQueryExecuteQueryOperator(
        task_id = 'advice_user_snapshot',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.advice_user_snapshot_{{ tomorrow_ds_nodash }}',
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        sql = """
        WITH t_advice AS (
        SELECT 
            _id advice_id
            ,adCategory
            ,DATETIME(createdAt,'Asia/Seoul') createdAt_KST
            ,DATETIME(DATE_ADD(DATETIME(dayString), INTERVAL CAST(time * 30 AS INT) MINUTE)) actual_advice_datetime
            ,status
            ,user
            ,kind
        FROM `lawtalk-bigquery.raw.advice`
        WHERE TRUE

        )
        , t_non_member AS (
        SELECT 
            _id user_id
            ,phone
            ,username
        FROM `lawtalk-bigquery.raw.users` WHERE username LIKE 'nonmember-%'
        )

        SELECT
            advice_id
            ,adcategory
            ,name category_name
            ,createdAt_KST
            ,actual_advice_datetime
            ,status
            ,kind
            ,CASE WHEN username IS NULL THEN 'member' ELSE 'non_member' END user_type
        FROM t_advice LEFT JOIN t_non_member ON t_advice.user = t_non_member.user_id
                    LEFT JOIN `lawtalk-bigquery.raw.adcategories` t_adcategories ON adcategory = t_adcategories._id
        ORDER BY 3 DESC
        """
    )

    start >> advice_user_snapshot