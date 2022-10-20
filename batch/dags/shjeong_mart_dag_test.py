from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="shjeong_mart_dag_test",
    description ="[test] lawtalk data mart",
    start_date = datetime(2022, 10, 20, tzinfo = KST),
    schedule_interval = '0 5 * * *',
    tags=["shjeong","test","mart"],
    default_args={  
        "owner": "shjeong", 
        "retries": 3,  # Task가 실패한 경우, 3번 재시도
        "retry_delay": timedelta(minutes=3),  # 재시도하는 시간 간격은 3분
    }
) as dag:

    start = DummyOperator(
        task_id="start"
    )


    ########################################################
    #dataset: mart
    #table_name: lt_r_lawyer_slot
    #description: [로톡] 변호사의 상담 슬롯 오픈과 유료 상담 여부
    #table_type: raw data
    #reprocessing date range: b_date기준 12일치 재처리(D-12 ~ D-1) : 슬롯 오픈 시 해당일자를 포함하여 D+7까지로 상담일자를 설정할 수 있고 상담일로부터 D+5까지 상담결과지 및 후기 작성이 가능하여 D+12까지 데이터 변경될 가능성 있음
    ########################################################

    delete_lt_r_lawyer_slot = BigQueryOperator(
        task_id = 'delete_lt_r_lawyer_slot',
        use_legacy_sql = False,
        sql = "delete from `lawtalk-bigquery.mart.lt_r_lawyer_slot` where b_date between date('{{next_ds}}') -5 and date('{{next_ds}}') + 7 " ## 새벽에 변호사가 상담 슬롯을 오픈할 수 있음을 고려 +1
    )

    insert_lt_r_lawyer_slot = BigQueryExecuteQueryOperator(
        task_id='insert_lt_r_lawyer_slot',
        use_legacy_sql = False,
        destination_dataset_table='lawtalk-bigquery.mart.lt_r_lawyer_slot',
        write_disposition = 'WRITE_APPEND',
        sql='''
        WITH t_lawyer AS (
        SELECT
            _id lawyer_id
            ,CASE WHEN slug = '5e314482c781c20602690b79' AND _id = '5e314482c781c20602690b79' THEN '탈퇴한 변호사' 
                    WHEN slug = '5e314482c781c20602690b79' AND _id = '616d0c91b78909e152c36e71' THEN '미활동 변호사'
                    WHEN slug LIKE '%탈퇴한%' THEN CONCAT(slug,'(탈퇴보류)')
                    ELSE slug END slug
            ,manager
            ,name
        FROM `lawtalk-bigquery.raw.lawyers`
        WHERE REGEXP_CONTAINS(slug,r'^[0-9]{4,4}-') OR slug = '5e314482c781c20602690b79' 
        )
        , BASE AS (
        SELECT
            lawyer
            ,daystring
            ,NULLIF(phone_times,'') phone_times
            ,NULLIF(video_times,'') video_times
            ,NULLIF(visiting_times,'') visiting_times
            ,DATETIME(createdAt,'Asia/Seoul') slot_crt_dt
            ,ROW_NUMBER() OVER (PARTITION BY lawyer, daystring ORDER BY DATETIME(createdAt,'Asia/Seoul') DESC) rn
        FROM `lawtalk-bigquery.raw.adviceschedules`, UNNEST(REGEXP_EXTRACT_ALL(times,r"'phone': \[(.*?)\]")) phone_times, UNNEST(REGEXP_EXTRACT_ALL(times,r"'video': \[(.*?)\]")) video_times, UNNEST(REGEXP_EXTRACT_ALL(times,r"'visiting': \[(.*?)\]")) visiting_times
        WHERE DATE(daystring) BETWEEN date('{{next_ds}}') -5 and date('{{next_ds}}') + 7
        QUALIFY rn = 1
        ) 

        SELECT 
            DATE(slot_opened_dt) as b_date
            ,t_slot.lawyer lawyer_id
            ,IFNULL(slug,'탈퇴/휴면 변호사') slug
            ,name
            ,manager
            ,slot_crt_dt
            ,slot_opened_dt
            ,EXTRACT(DATE FROM slot_opened_dt) slot_opened_date
            ,FORMAT_DATETIME('%R', slot_opened_dt) slot_opened_time
            ,CASE EXTRACT(DAYOFWEEK FROM slot_opened_dt) WHEN 1 THEN '일' 
                                                        WHEN 2 THEN '월'
                                                        WHEN 3 THEN '화'
                                                        WHEN 4 THEN '수'
                                                        WHEN 5 THEN '목'
                                                        WHEN 6 THEN '금'
                                                        WHEN 7 THEN '토' 
            END slot_day_of_week
            ,t_slot.kind
            ,CASE WHEN counsel_exc_dt = slot_opened_dt THEN 1 ELSE 0 END is_reserved
            ,t_advice._id counsel_id
            ,counsel_crt_dt
            ,status counsel_status
        FROM (
        SELECT
            lawyer
            ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(phone_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
            ,'phone' as kind
            ,slot_crt_dt
        FROM BASE, UNNEST(SPLIT(phone_times,', ')) phone_time_slot

        UNION ALL 

        SELECT
            lawyer
            ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(video_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
            ,'video' as kind
            ,slot_crt_dt
        FROM BASE, UNNEST(SPLIT(video_times,', ')) video_time_slot

        UNION ALL 

        SELECT
            lawyer
            ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(visiting_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
            ,'visiting' as kind
            ,slot_crt_dt
        FROM BASE, UNNEST(SPLIT(visiting_times,', ')) visiting_time_slot

        ) t_slot LEFT JOIN (SELECT 
                                DATE_ADD(DATETIME(dayString), INTERVAL CAST(time AS INT) * 30 MINUTE) counsel_exc_dt
                                ,DATETIME(createdAt,'Asia/Seoul') counsel_crt_dt
                                ,IFNULL(kind,'phone') kind
                                ,lawyer
                                ,status
                                ,_id
                            FROM `raw.advice` 
                            WHERE DATE(daystring) BETWEEN date('{{next_ds}}') -5 and date('{{next_ds}}')
                            AND status != 'reserved') t_advice
                        ON t_slot.lawyer = t_advice.lawyer
                        AND t_slot.slot_opened_dt = t_advice.counsel_exc_dt
                        AND t_slot.kind = t_advice.kind
                LEFT JOIN t_lawyer ON t_slot.lawyer = t_lawyer.lawyer_id            
            '''
    )

start >> delete_lt_r_lawyer_slot >> insert_lt_r_lawyer_slot
