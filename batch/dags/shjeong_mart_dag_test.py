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
        "retry_delay": timedelta(minutes=1),  # 재시도하는 시간 간격은 1분
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
        sql = "delete from `lawtalk-bigquery.mart.lt_r_lawyer_slot` where b_date between date('{{next_ds}}') -5 and date('{{next_ds}}') +7 " ## 새벽에 변호사가 상담 슬롯을 오픈할 수 있음을 고려 +1
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
        WHERE DATE(daystring) BETWEEN date('{{next_ds}}') -5 and date('{{next_ds}}') +7
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

    ########################################################
    #dataset: mart
    #table_name: lt_w_lawyer_slot
    #description: [로톡] 일자별 변호사별 슬롯 오픈 및 예약 현황
    #table_type: w단 일자별 집계
    #reprocessing date range: b_date기준 12일치 재처리(D-12 ~ D-1) : 슬롯 오픈 시 해당일자를 포함하여 D+7까지로 상담일자를 설정할 수 있고 상담일로부터 D+5까지 상담결과지 및 후기 작성이 가능하여 D+12까지 데이터 변경될 가능성 있음
    ########################################################

    delete_lt_w_lawyer_slot = BigQueryOperator(
        task_id = 'delete_lt_w_lawyer_slot',
        use_legacy_sql = False,
        sql = "delete from `lawtalk-bigquery.mart.lt_w_lawyer_slot` where b_date between date('{{next_ds}}') -5 and date('{{next_ds}}') +7 " ## 새벽에 변호사가 상담 슬롯을 오픈할 수 있음을 고려 +1
    )

    insert_lt_w_lawyer_slot = BigQueryExecuteQueryOperator(
        task_id='insert_lt_w_lawyer_slot',
        use_legacy_sql = False,
        destination_dataset_table='lawtalk-bigquery.mart.lt_w_lawyer_slot',
        write_disposition = 'WRITE_APPEND',
        sql='''
        SELECT 
            b_date
            ,slot_day_of_week
            ,lawyer_id
            ,slug
            ,name
            ,manager
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'phone' THEN slot_opened_dt END) + COUNT(DISTINCT CASE WHEN kind = 'video' THEN slot_opened_dt END) + COUNT(DISTINCT CASE WHEN kind = 'visiting' THEN slot_opened_dt END) AS numeric) total_slot_opened_cnt
            ,CAST(COUNT(DISTINCT slot_opened_dt) AS numeric) total_slot_opened_time_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'phone' THEN slot_opened_dt END) AS numeric) phone_slot_opened_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'phone' AND is_reserved = 1 THEN slot_opened_dt END) AS numeric) phone_reserve_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'phone' AND is_reserved = 1 AND counsel_status = 'complete' THEN slot_opened_dt END) AS numeric) phone_complete_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'video' THEN slot_opened_dt END) AS numeric) video_slot_opened_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'video' AND is_reserved = 1 THEN slot_opened_dt END) AS numeric) video_reserve_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'video' AND is_reserved = 1 AND counsel_status = 'complete' THEN slot_opened_dt END) AS numeric) video_complete_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'visiting' THEN slot_opened_dt END) AS numeric) visiting_slot_opened_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'visiting' AND is_reserved = 1 THEN slot_opened_dt END) AS numeric) visiting_reserve_cnt
            ,CAST(COUNT(DISTINCT CASE WHEN kind = 'visiting' AND is_reserved = 1 AND counsel_status = 'complete' THEN slot_opened_dt END) AS numeric) visiting_complete_cnt
        FROM `lawtalk-bigquery.mart.lt_r_lawyer_slot` 
        WHERE b_date BETWEEN date('{{next_ds}}') -5 and date('{{next_ds}}') +7
        GROUP BY 1,2,3,4,5,6
            '''
    )

    ########################################################
    #dataset: mart
    #table_name: lt_s_user_info
    #description: [로톡] 유저 정보 (의뢰인, 변호사, 변호사 승인대기)
    #table_type: s단 일자별 스냅샷
    #reprocessing date range: b_date 기준 1일치 처리 (해당일자 시점의 스냅샷 형태로 하루치만 처리하면 됨)
    ########################################################

    delete_lt_s_user_info = BigQueryOperator(
        task_id = 'delete_lt_s_user_info',
        use_legacy_sql = False,
        sql = "delete from `lawtalk-bigquery.mart.lt_s_user_info` where b_date = date('{{next_ds}}')"
    )
    insert_lt_s_user_info = BigQueryExecuteQueryOperator(
        task_id='insert_lt_s_user_info',
        use_legacy_sql = False,
        destination_dataset_table='lawtalk-bigquery.mart.lt_s_user_info',
        write_disposition = 'WRITE_APPEND',
        sql='''
        WITH t_lawyer AS (
        SELECT
            _id lawyer_id
            ,CASE WHEN slug = '5e314482c781c20602690b79' AND _id = '5e314482c781c20602690b79' THEN '탈퇴한 변호사' 
                    WHEN slug = '5e314482c781c20602690b79' AND _id = '616d0c91b78909e152c36e71' THEN '미활동 변호사'
                    WHEN slug LIKE '%탈퇴한%' THEN CONCAT(slug,'(탈퇴보류)')
                    ELSE slug END slug
        FROM `lawtalk-bigquery.raw.lawyers`
        WHERE REGEXP_CONTAINS(slug,r'^[0-9]{4,4}-') OR slug = '5e314482c781c20602690b79' 
        )
        , BASE AS (
        SELECT 
            date('{{next_ds}}') as b_date
            ,role
            ,_id user_id
            ,username user_nickname
            ,CASE WHEN _id = '620a0996ee8c9876d5f62d6a' OR slug = '탈퇴한 변호사' THEN '탈퇴'
                    WHEN _id = '620a0a07ee8c9876d5f671d8' OR slug = '미활동 변호사' THEN '미활동'
                    WHEN slug LIKE '%(탈퇴보류)' THEN '탈퇴 보류'
                    WHEN role = 'lawyer_waiting' THEN '승인 대기'
                    ELSE '활동'
            END user_status
            ,email user_email
            ,CASE isNonMember WHEN 'True' THEN 1 ELSE 0 END is_non_member
            ,EXTRACT(year FROM birth) birth_year
            ,EXTRACT(year FROM CURRENT_DATE('Asia/Seoul')) - EXTRACT(year FROM birth) + 1 korean_age
            ,sex
            ,countryCode country_code
            ,DATETIME(createdAt,'Asia/Seoul') crt_dt
            ,DATETIME(updatedAt,'Asia/Seoul') upd_dt
            ,CASE isEmailAccept WHEN True THEN 1 ELSE 0 END is_email_accept
            -- ,REGEXP_EXTRACT_ALL(emailMarketingAccept, r"'status': (.*?),") email_marketing_status
            -- ,REGEXP_EXTRACT_ALL(emailMarketingAccept, r"'startDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") email_marketing_start_date
            -- ,REGEXP_EXTRACT_ALL(emailMarketingAccept, r"'endDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") email_marketing_end_date
            ,CASE isSMSAccept WHEN True THEN 1 ELSE 0 END is_sms_accept
            -- ,REGEXP_EXTRACT_ALL(smsMarketingAccept, r"'status': (.*?),") sms_marketing_status
            -- ,REGEXP_EXTRACT_ALL(smsMarketingAccept, r"'startDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") sms_marketing_start_date
            -- ,REGEXP_EXTRACT_ALL(smsMarketingAccept, r"'endDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") sms_marketing_end_date
            ,provider
            ,referrer
            ,referrerOther referrer_other
            ,recommender recommender_id
            ,CAST(null AS string) recommender_name
            ,CASE reviewCouponCheck WHEN 'True' THEN 1 
                                    WHEN 'False' THEN 0 
                                    ELSE null
            END is_review_coupon
            ,REGEXP_EXTRACT(utm, r"'utm_source': '(.*?)'") utm_source
            ,REGEXP_EXTRACT(utm, r"'utm_medium': '(.*?)'") utm_medium
            ,REGEXP_EXTRACT(utm, r"'utm_campaign': '(.*?)'") utm_campaign
            ,REGEXP_EXTRACT(utm, r"'utm_content': '(.*?)'") utm_content
        FROM `raw.users` LEFT JOIN t_lawyer ON `raw.users`.lawyer = t_lawyer.lawyer_id
        WHERE role IN ('lawyer', 'user', 'lawyer-waiting') 
        )

        , t_sms_marketing AS (
        SELECT
            user_id
            ,sms_marketing_status_unnested_offset history_number
            ,CASE WHEN sms_marketing_status_unnested = 'True' THEN 1 ELSE 0 END sms_marketing_status
            ,DATETIME(PARSE_TIMESTAMP('%Y, %m, %e, %H, %M, %S',sms_marketing_start_date_unnested),'Asia/Seoul') sms_marketing_start_date
            ,DATETIME(PARSE_TIMESTAMP('%Y, %m, %e, %H, %M, %S',sms_marketing_end_date_unnested),'Asia/Seoul') sms_marketing_end_date
        FROM (
        SELECT
            _id user_id
            ,REGEXP_EXTRACT_ALL(smsMarketingAccept, r"'status': (.*?),") sms_marketing_status
            ,REGEXP_EXTRACT_ALL(smsMarketingAccept, r"'startDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") sms_marketing_start_date
            ,REGEXP_EXTRACT_ALL(smsMarketingAccept, r"'endDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") sms_marketing_end_date
        FROM `raw.users`) , UNNEST (sms_marketing_status) sms_marketing_status_unnested WITH OFFSET sms_marketing_status_unnested_offset
                        , UNNEST (sms_marketing_start_date) sms_marketing_start_date_unnested WITH OFFSET sms_marketing_start_date_unnested_offset
                        , UNNEST (sms_marketing_end_date) sms_marketing_end_date_unnested WITH OFFSET sms_marketing_end_date_unnested_offset
        WHERE sms_marketing_status_unnested_offset = sms_marketing_start_date_unnested_offset
        AND sms_marketing_start_date_unnested_offset = sms_marketing_end_date_unnested_offset
        AND sms_marketing_status_unnested_offset = sms_marketing_end_date_unnested_offset
        ) 

        , t_sms_marketing2 AS (
        SELECT
            t_sms_marketing.user_id 
            ,sms_marketing_status
            ,sms_marketing_start_date
            ,sms_marketing_end_date
        FROM t_sms_marketing INNER JOIN (SELECT user_id, MAX(history_number) history_number FROM t_sms_marketing GROUP BY 1) sub 
                                        ON t_sms_marketing.user_id = sub.user_id 
                                        AND t_sms_marketing.history_number = sub.history_number
        )

        , t_email_marketing AS (
        SELECT
            user_id
            ,email_marketing_status_unnested_offset history_number
            ,CASE WHEN email_marketing_status_unnested = 'True' THEN 1 ELSE 0 END email_marketing_status
            ,DATETIME(PARSE_TIMESTAMP('%Y, %m, %e, %H, %M, %S',email_marketing_start_date_unnested),'Asia/Seoul') email_marketing_start_date
            ,DATETIME(PARSE_TIMESTAMP('%Y, %m, %e, %H, %M, %S',email_marketing_end_date_unnested),'Asia/Seoul') email_marketing_end_date
        FROM (
        SELECT
            _id user_id
            ,REGEXP_EXTRACT_ALL(emailMarketingAccept, r"'status': (.*?),") email_marketing_status
            ,REGEXP_EXTRACT_ALL(emailMarketingAccept, r"'startDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") email_marketing_start_date
            ,REGEXP_EXTRACT_ALL(emailMarketingAccept, r"'endDate': datetime\.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") email_marketing_end_date
            FROM `raw.users`) , UNNEST (email_marketing_status) email_marketing_status_unnested WITH OFFSET email_marketing_status_unnested_offset
                            , UNNEST (email_marketing_start_date) email_marketing_start_date_unnested WITH OFFSET email_marketing_start_date_unnested_offset
                            , UNNEST (email_marketing_end_date) email_marketing_end_date_unnested WITH OFFSET email_marketing_end_date_unnested_offset
        WHERE email_marketing_status_unnested_offset = email_marketing_start_date_unnested_offset
        AND email_marketing_start_date_unnested_offset = email_marketing_end_date_unnested_offset
        AND email_marketing_status_unnested_offset = email_marketing_end_date_unnested_offset
        ) 

        , t_email_marketing2 AS (
        SELECT
            t_email_marketing.user_id 
            ,email_marketing_status
            ,email_marketing_start_date
            ,email_marketing_end_date
        FROM t_email_marketing INNER JOIN (SELECT user_id, MAX(history_number) history_number FROM t_email_marketing GROUP BY 1) sub 
                                        ON t_email_marketing.user_id = sub.user_id 
                                        AND t_email_marketing.history_number = sub.history_number
        )

        SELECT
            b_date
            ,role
            ,user_id
            ,user_nickname
            ,user_status
            ,user_email
            ,is_non_member
            ,CAST(birth_year as numeric) birth_year
            ,CAST(korean_age as numeric) korean_age
            ,sex
            ,CAST(country_code as numeric) country_code
            ,crt_dt
            ,upd_dt
            ,is_email_accept
            ,email_marketing_status is_email_marketing_accept
            -- ,email_marketing_start_date email_marketing_accept_start_date
            ,sms_marketing_end_date email_marketing_accept_end_date 
            ,is_sms_accept
            ,sms_marketing_status is_sms_marketing_accept
            -- ,sms_marketing_start_date sms_marketing_accept_start_date
            ,email_marketing_end_date sms_marketing_accept_end_date
            ,provider
            ,referrer
            ,referrer_other
            ,recommender_id
            ,recommender_name
            ,is_review_coupon
            ,utm_source
            ,utm_medium
            ,utm_campaign
            ,utm_content
        FROM BASE LEFT JOIN t_sms_marketing2 USING (user_id)
                  LEFT JOIN t_email_marketing2 USING (user_id)
            '''
    )

start >> delete_lt_r_lawyer_slot >> insert_lt_r_lawyer_slot >> delete_lt_w_lawyer_slot >> insert_lt_w_lawyer_slot
start >> delete_lt_s_user_info >> insert_lt_s_user_info
