from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="shjeong_userside_dag",
    description ="shjeong_userside_dag",
    start_date = datetime(2022, 4, 11, tzinfo = KST),
    schedule_interval = '0 6 * * *',
    tags=["shjeong","userside"],
    default_args={  
        "owner": "shjeong", 
        "retries": 3,  # Task가 실패한 경우, 3번 재시도
        "retry_delay": timedelta(minutes=3),  # 재시도하는 시간 간격은 3분
    }
) as dag:


    start = DummyOperator(
        task_id="start"
    )

    user_advice_question = BigQueryExecuteQueryOperator(
        task_id = 'user_advice_question',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.user_advice_question',
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        sql = """
        WITH t_user_advice AS (
        SELECT 
            user
            ,COUNT(_id) total_advice_cnt
            ,COUNT(CASE WHEN kind = 'phone' OR kind IS NULL THEN _id END) phone_advice_cnt
            ,COUNT(CASE kind WHEN 'video' THEN _id END) video_advice_cnt
            ,COUNT(CASE kind WHEN 'visiting' THEN _id END) visiting_advice_cnt
            ,COUNT(CASE WHEN DATE_ADD(DATETIME(dayString), INTERVAL CAST(time * 30 AS INT) MINUTE) BETWEEN DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -6 DAY) AND CURRENT_DATE('Asia/Seoul') THEN _id END) last7days_advice_cnt
            ,COUNT(CASE WHEN DATE_ADD(DATETIME(dayString), INTERVAL CAST(time * 30 AS INT) MINUTE) BETWEEN DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -29 DAY) AND CURRENT_DATE('Asia/Seoul') THEN _id END) last30days_advice_cnt
        FROM `lawtalk-bigquery.raw.advice`
        WHERE status IN ('complete','reservation')
        GROUP BY 1
        )
        , t_user_question AS (
        SELECT 
            user
            ,COUNT(_id) total_question_cnt
            ,COUNT(CASE WHEN DATE(createdAt,'Asia/Seoul') BETWEEN DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -6 DAY) AND CURRENT_DATE('Asia/Seoul') THEN _id END) last7days_question_cnt
            ,COUNT(CASE WHEN DATE(createdAt,'Asia/Seoul') BETWEEN DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL -29 DAY) AND CURRENT_DATE('Asia/Seoul') THEN _id END) last30days_question_cnt
        FROM `lawtalk-bigquery.raw.questions`
        GROUP BY 1
        )
        , t_user AS (
        SELECT
            _id user
            ,sex
            ,EXTRACT(YEAR FROM birth) birth_year
            ,EXTRACT(YEAR FROM CURRENT_DATE('Asia/Seoul')) - EXTRACT(YEAR FROM birth) + 1 AS korean_age
        FROM `lawtalk-bigquery.raw.users`
        WHERE role = 'user'
        )

        SELECT
            *
        FROM t_user_advice FULL OUTER JOIN t_user_question USING (user)
                        INNER JOIN t_user USING (user)
        """
    )

    start >> user_advice_question