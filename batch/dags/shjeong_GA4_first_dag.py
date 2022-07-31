from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="shjeong_GA4_first_dag",
    description ="GA4_first_dag",
    start_date = datetime(2022, 6, 14, tzinfo = KST),
    schedule_interval = '0 5 * * *',
    tags=["shjeong","GA4"],
    default_args={  
        "owner": "shjeong", 
        "retries": 3,  # Task가 실패한 경우, 3번 재시도
        "retry_delay": timedelta(minutes=3),  # 재시도하는 시간 간격은 3분
    }
) as dag:


    start = DummyOperator(
        task_id="start"
    )

    check_ga4_table_exists = BigQueryTableExistenceSensor(
        task_id="check_ga4_table_exists",
        project_id='lawtalk-bigquery',
        dataset_id='analytics_265523655',
        table_id='events_{{ tomorrow_ds_nodash }}',
        poke_interval=600
    )

    view_event = BigQueryExecuteQueryOperator(
        task_id = 'view_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.view_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT
            CONCAT(event_timestamp, row_number() over (order by event_timestamp)) event_id
            ,DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_date
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime
            ,event_name
            ,user_id
            ,user_pseudo_id
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id
            ,device.category as device_type
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_path') as page_path
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_referrer') as page_referrer
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'lawyer_id') as lawyer_id
            ,traffic_source.name traffic_source_name
            ,traffic_source.medium traffic_source_medium
            ,traffic_source.source traffic_source_source
        FROM `lawtalk-bigquery.analytics_265523655.events_*`
        WHERE TRUE 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
        AND event_name in ('tag.qna.온라인상담글조회','tag.posts.포스트조회','tag.videos.법률tip조회','tag.프로필페이지진입(page_view)')
        """
    )

    first_visit_from_qna_user_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_qna_user_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_qna_user_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT 
            CONCAT(event_timestamp, row_number() over (order by event_timestamp)) event_id 
            ,DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_date 
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime 
            ,event_name 
            ,user_pseudo_id 
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id 
            ,device.category as device_type 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_referrer') as page_referrer 
            ,traffic_source.name traffic_source_name 
            ,traffic_source.medium traffic_source_medium 
            ,traffic_source.source traffic_source_source 
        FROM `lawtalk-bigquery.analytics_265523655.events_*` 
        WHERE TRUE 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
        -- AND event_name = 'session_start' 
        AND user_pseudo_id IN (SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.tmp_shjeong.first_visit_from_qna_user_event` 
                                UNION ALL 
                                SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.analytics_265523655.events_*` 
                                WHERE TRUE 
                                AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
                                AND event_name = 'first_visit' 
                                AND REGEXP_CONTAINS((SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location'),'qna/[0-9]+') 
                                )
        """
    )

    first_visit_from_lawyer_profile_user_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_lawyer_profile_user_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_lawyer_profile_user_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT 
            CONCAT(event_timestamp, row_number() over (order by event_timestamp)) event_id 
            ,DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_date 
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime 
            ,event_name 
            ,user_pseudo_id 
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id 
            ,device.category as device_type 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_referrer') as page_referrer 
            ,traffic_source.name traffic_source_name 
            ,traffic_source.medium traffic_source_medium 
            ,traffic_source.source traffic_source_source 
        FROM `lawtalk-bigquery.analytics_265523655.events_*` 
        WHERE TRUE 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}' 
        -- AND event_name = 'session_start' 
        AND user_pseudo_id IN (SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.tmp_shjeong.first_visit_from_lawyer_profile_user_event` 
                                UNION ALL 
                                SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.analytics_265523655.events_*` 
                                WHERE TRUE AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
                                AND event_name = 'first_visit' 
                                AND REGEXP_CONTAINS((SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location'),'directory/profile/[0-9]+') 
                                )
        """
    )

    first_visit_from_posts_user_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_posts_user_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_posts_user_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT 
            CONCAT(event_timestamp, row_number() over (order by event_timestamp)) event_id 
            ,DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_date 
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime 
            ,event_name 
            ,user_pseudo_id 
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id 
            ,device.category as device_type 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_referrer') as page_referrer 
            ,traffic_source.name traffic_source_name 
            ,traffic_source.medium traffic_source_medium 
            ,traffic_source.source traffic_source_source 
        FROM `lawtalk-bigquery.analytics_265523655.events_*` 
        WHERE TRUE 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}' 
        -- AND event_name = 'session_start' 
        AND user_pseudo_id IN (SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.tmp_shjeong.first_visit_from_posts_user_event` 
                                UNION ALL 
                                SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.analytics_265523655.events_*` 
                                WHERE TRUE AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
                                AND event_name = 'first_visit' 
                                AND REGEXP_CONTAINS((SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location'),'posts/[0-9]+') 
                                )
        """
    )

    first_visit_from_videos_user_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_videos_user_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_videos_user_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT 
            CONCAT(event_timestamp, row_number() over (order by event_timestamp)) event_id 
            ,DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_date 
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime 
            ,event_name 
            ,user_pseudo_id 
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id 
            ,device.category as device_type 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_referrer') as page_referrer 
            ,traffic_source.name traffic_source_name 
            ,traffic_source.medium traffic_source_medium 
            ,traffic_source.source traffic_source_source 
        FROM `lawtalk-bigquery.analytics_265523655.events_*` 
        WHERE TRUE 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}' 
        -- AND event_name = 'session_start' 
        AND user_pseudo_id IN (SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.tmp_shjeong.first_visit_from_videos_user_event` 
                                UNION ALL 
                                SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.analytics_265523655.events_*` 
                                WHERE TRUE AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
                                AND event_name = 'first_visit' 
                                AND REGEXP_CONTAINS((SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location'),'videos/[0-9]+') 
                                )
        """
    )

    first_visit_from_main_user_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_main_user_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_main_user_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT 
            CONCAT(event_timestamp, row_number() over (order by event_timestamp)) event_id 
            ,DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_date 
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime 
            ,event_name 
            ,user_pseudo_id 
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id 
            ,device.category as device_type 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location 
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_referrer') as page_referrer 
            ,traffic_source.name traffic_source_name 
            ,traffic_source.medium traffic_source_medium 
            ,traffic_source.source traffic_source_source 
        FROM `lawtalk-bigquery.analytics_265523655.events_*` 
        WHERE TRUE 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}' 
        -- AND event_name = 'session_start' 
        AND user_pseudo_id IN (SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.tmp_shjeong.first_visit_from_main_user_event` 
                                UNION ALL 
                                SELECT DISTINCT user_pseudo_id 
                                FROM `lawtalk-bigquery.analytics_265523655.events_*` 
                                WHERE TRUE AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
                                AND event_name = 'first_visit' 
                                AND REGEXP_CONTAINS((SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location'),r'lawtalk.co.kr\/$|lawtalk.co.kr\/[?]') 
                                )
        """
    )


    qna_first_visit_UV_base = BigQueryExecuteQueryOperator(
        task_id = 'qna_first_visit_UV_base',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.qna_first_visit_UV_base',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT
            CONCAT(event_timestamp, row_number() over (order by event_timestamp)) event_id
            ,DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_date
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') AS event_datetime
            ,event_name
            ,user_pseudo_id
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id
            ,device.category as device_type
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_referrer') as page_referrer
            ,traffic_source.name traffic_source_name
            ,traffic_source.medium traffic_source_medium
            ,traffic_source.source traffic_source_source
        FROM `lawtalk-bigquery.analytics_265523655.events_*`
        WHERE TRUE 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
        AND event_name = 'session_start'
        AND user_pseudo_id IN (SELECT DISTINCT user_pseudo_id
                                FROM `lawtalk-bigquery.tmp_shjeong.qna_first_visit_UV_base`

                                UNION ALL

                                SELECT DISTINCT user_pseudo_id
                                FROM `lawtalk-bigquery.analytics_265523655.events_*`
                                WHERE TRUE
                                AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
                                AND event_name = 'first_visit'
                                AND REGEXP_CONTAINS((SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location'),'qna/[0-9]+')
            )
        """
    )

    qna_more_cases = BigQueryExecuteQueryOperator(
        task_id = 'qna_more_cases',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.qna_more_cases',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT
            CONCAT(event_timestamp,row_number() OVER (ORDER BY event_timestamp)) as event_id
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_datetime
            ,user_id
            ,user_pseudo_id
            ,(SELECT param.value.int_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') ga_session_id
            ,device.category as device_type
            ,event_name
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'order') as sort
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'category') as category
            ,(SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'content') as content
        FROM `lawtalk-bigquery.analytics_265523655.events_*`
        WHERE 1=1 
        AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
        AND event_name in ('tag.qna.사례더보기클릭_category','tag.qna.사례더보기클릭_order')
        ORDER BY 2
        """
    )

    user_pseudo_id_AND_user_id = BigQueryExecuteQueryOperator(
        task_id = 'user_pseudo_id_AND_user_id',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.user_pseudo_id_AND_user_id',
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        sql = """
        SELECT
            user_pseudo_id
            ,user_id
            ,COUNT(DISTINCT user_id) OVER (PARTITION BY user_pseudo_id) uid_cnt_per_upid
        FROM `lawtalk-bigquery.analytics_265523655.events_*`
        WHERE user_id IS NOT NULL
        GROUP BY 1,2
        """
    )

    UV_daily = BigQueryExecuteQueryOperator(
        task_id = 'UV_daily',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.UV_daily',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        WITH BASE AS (
        SELECT 
        DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') event_date
        ,user_pseudo_id
        FROM `lawtalk-bigquery.analytics_265523655.events_*` 
        WHERE _TABLE_SUFFIX BETWEEN REPLACE(CAST(DATE_ADD(DATE('{{ tomorrow_ds }}'), INTERVAL -29 DAY) AS STRING),'-','') AND '{{ tomorrow_ds_nodash }}'
        )
        , T_DAU AS (
        SELECT 
            event_date
            ,COUNT(DISTINCT user_pseudo_id) dau
        FROM BASE
        WHERE event_date = '{{ tomorrow_ds }}'
        GROUP BY 1
        )
        , UV as (
        SELECT 
            event_date
            ,dau
            ,(SELECT COUNT(DISTINCT user_pseudo_id)
                FROM BASE
                WHERE event_date BETWEEN T_DAU.event_date - 6 AND T_DAU.event_date) AS wau
            ,(SELECT COUNT(DISTINCT user_pseudo_id)
                FROM BASE
                WHERE event_date BETWEEN T_DAU.event_date - 29 AND T_DAU.event_date ) AS mau
        FROM T_DAU
        ORDER BY 1
        )

        SELECT * FROM UV
        """
    )

    UV_daily_NewAndRepeat = BigQueryExecuteQueryOperator(
        task_id = 'UV_daily_NewAndRepeat',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.UV_daily_NewAndRepeat',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        WITH t_generate_date AS (
        SELECT
            *
        FROM UNNEST(GENERATE_DATE_ARRAY('{{ tomorrow_ds }}', '{{ tomorrow_ds }}')) generate_date
        )
        , t_user_first_visit AS (
        SELECT
            user_pseudo_id
            ,MIN(DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul')) first_visit_date
        FROM `lawtalk-bigquery.analytics_265523655.events_*`
        WHERE event_name = 'first_visit'
        GROUP BY 1
        )
        , t_user_event AS (
        SELECT
            user_pseudo_id
            ,DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') event_date
        FROM `lawtalk-bigquery.analytics_265523655.events_*`
        WHERE event_name != 'first_visit'
        )
        -----------------------------------------------------------------------------
        , t_DAU AS (
        SELECT 
            generate_date
            ,COUNT(DISTINCT user_pseudo_id) DAU
            ,COUNT(DISTINCT CASE WHEN first_visit_date = generate_date THEN user_pseudo_id END) DAU_new_user
            ,COUNT(DISTINCT CASE WHEN (first_visit_date IS NOT NULL) AND (first_visit_date != generate_date) THEN user_pseudo_id END) DAU_repeat_user
            ,COUNT(DISTINCT CASE WHEN first_visit_date IS NULL THEN user_pseudo_id END) DAU_unknown_user
        FROM t_user_event LEFT JOIN t_user_first_visit USING(user_pseudo_id)
        CROSS JOIN t_generate_date
        WHERE event_date = generate_date
        GROUP BY generate_date
        ORDER BY 1
        )
        , t_WAU AS (
        SELECT 
            generate_date
            ,COUNT(DISTINCT user_pseudo_id) WAU
            ,COUNT(DISTINCT CASE WHEN first_visit_date BETWEEN DATE_ADD(generate_date, INTERVAL -6 DAY) AND generate_date THEN user_pseudo_id END) WAU_new_user
            ,COUNT(DISTINCT CASE WHEN (first_visit_date IS NOT NULL) AND ((first_visit_date < DATE_ADD(generate_date, INTERVAL -6 DAY)) OR (first_visit_date > generate_date)) THEN user_pseudo_id END) WAU_repeat_user
            ,COUNT(DISTINCT CASE WHEN first_visit_date IS NULL THEN user_pseudo_id END) WAU_unknown_user
        FROM t_user_event LEFT JOIN t_user_first_visit USING(user_pseudo_id)
            CROSS JOIN t_generate_date
        WHERE event_date BETWEEN DATE_ADD(generate_date, INTERVAL -6 DAY) AND generate_date
        GROUP BY generate_date
        ORDER BY 1
        )
        , t_MAU AS (
        SELECT 
            generate_date
            ,COUNT(DISTINCT user_pseudo_id) MAU
            ,COUNT(DISTINCT CASE WHEN first_visit_date BETWEEN DATE_ADD(generate_date, INTERVAL -29 DAY) AND generate_date THEN user_pseudo_id END) MAU_new_user
            ,COUNT(DISTINCT CASE WHEN (first_visit_date IS NOT NULL) AND ((first_visit_date < DATE_ADD(generate_date, INTERVAL -29 DAY)) OR (first_visit_date > generate_date)) THEN user_pseudo_id END) MAU_repeat_user
            ,COUNT(DISTINCT CASE WHEN first_visit_date IS NULL THEN user_pseudo_id END) MAU_unknown_user
        FROM t_user_event LEFT JOIN t_user_first_visit USING(user_pseudo_id)
            CROSS JOIN t_generate_date
        WHERE event_date BETWEEN DATE_ADD(generate_date, INTERVAL -29 DAY) AND generate_date
        GROUP BY generate_date
        ORDER BY 1
        )

        SELECT
            generate_date as event_date
            ,* EXCEPT(generate_date)
        FROM t_DAU INNER JOIN t_WAU USING (generate_date)
                INNER JOIN t_MAU USING (generate_date)
        ORDER BY 1
        """
    )

    user_first_visit_from = BigQueryExecuteQueryOperator(
        task_id = 'user_first_visit_from',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.user_first_visit_from',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT
            user_pseudo_id
            ,DATE(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') first_visit_date
            ,DATETIME(TIMESTAMP_MICROS(event_timestamp),'Asia/Seoul') first_visit_datetime
            ,(SELECT param.value.string_value FROM UNNEST(event_params) param WHERE param.key = 'page_location') page_location
            ,(SELECT param.value.string_value FROM UNNEST(event_params) param WHERE param.key = 'page_referrer') page_referrer
            ,traffic_source.name traffic_source_name
            ,traffic_source.medium traffic_source_medium
            ,traffic_source.source traffic_source_source
            FROM `lawtalk-bigquery.analytics_265523655.events_*`
            WHERE event_name = 'first_visit'
            AND _TABLE_SUFFIX = '{{ tomorrow_ds_nodash }}'
        """
    )
    
    first_visit_from_kin_naver_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_kin_naver_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_kin_naver_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT
            user_pseudo_id
            ,first_visit_date
            ,DATE(TIMESTAMP_MICROS(t_event.event_timestamp),'Asia/Seoul') event_date
            ,DATETIME(TIMESTAMP_MICROS(t_event.event_timestamp),'Asia/Seoul') event_datetime
            ,event_name
        FROM `lawtalk-bigquery.analytics_265523655.events_{{ tomorrow_ds_nodash }}` t_event INNER JOIN (
                                                                                                        SELECT
                                                                                                            user_pseudo_id
                                                                                                            ,MIN(first_visit_date) first_visit_date
                                                                                                        FROM `lawtalk-bigquery.tmp_shjeong.user_first_visit_from`
                                                                                                        WHERE REGEXP_CONTAINS(page_referrer,r'kin\.naver\.') AND NOT REGEXP_CONTAINS(page_referrer,r'https://www\.lawtalk\.co\.kr')
                                                                                                        GROUP BY 1 )  t_first_visit
                                                                                                            USING (user_pseudo_id)
        WHERE t_event.event_name != 'first_visit'
        """
    )

    first_visit_from_google_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_google_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_google_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT
            user_pseudo_id
            ,first_visit_date
            ,DATE(TIMESTAMP_MICROS(t_event.event_timestamp),'Asia/Seoul') event_date
            ,DATETIME(TIMESTAMP_MICROS(t_event.event_timestamp),'Asia/Seoul') event_datetime
            ,event_name
        FROM `lawtalk-bigquery.analytics_265523655.events_{{ tomorrow_ds_nodash }}` t_event INNER JOIN (
                                                                                                        SELECT
                                                                                                            user_pseudo_id
                                                                                                            ,MIN(first_visit_date) first_visit_date
                                                                                                        FROM `lawtalk-bigquery.tmp_shjeong.user_first_visit_from`
                                                                                                        WHERE REGEXP_CONTAINS(page_referrer,r'google\.') AND NOT REGEXP_CONTAINS(page_referrer,r'https://www\.lawtalk\.co\.kr|android')
                                                                                                        GROUP BY 1 )  t_first_visit
                                                                                                            USING (user_pseudo_id)
        WHERE t_event.event_name != 'first_visit'
        """
    )
    first_visit_from_search_naver_event = BigQueryExecuteQueryOperator(
        task_id = 'first_visit_from_search_naver_event',
        destination_dataset_table = 'lawtalk-bigquery.tmp_shjeong.first_visit_from_search_naver_event',
        use_legacy_sql = False,
        write_disposition = 'WRITE_APPEND',
        sql = """
        SELECT
            user_pseudo_id
            ,first_visit_date
            ,DATE(TIMESTAMP_MICROS(t_event.event_timestamp),'Asia/Seoul') event_date
            ,DATETIME(TIMESTAMP_MICROS(t_event.event_timestamp),'Asia/Seoul') event_datetime
            ,event_name
        FROM `lawtalk-bigquery.analytics_265523655.events_{{ tomorrow_ds_nodash }}` t_event INNER JOIN (
                                                                                                        SELECT
                                                                                                            user_pseudo_id
                                                                                                            ,MIN(first_visit_date) first_visit_date
                                                                                                        FROM `lawtalk-bigquery.tmp_shjeong.user_first_visit_from`
                                                                                                        WHERE REGEXP_CONTAINS(page_referrer,r'search\.naver\.') AND NOT REGEXP_CONTAINS(page_referrer,r'https://www\.lawtalk\.co\.kr')
                                                                                                        GROUP BY 1 )  t_first_visit
                                                                                                            USING (user_pseudo_id)
        WHERE t_event.event_name != 'first_visit'
        """
    )


start >> check_ga4_table_exists >> [view_event,  
                                    first_visit_from_qna_user_event, 
                                    first_visit_from_lawyer_profile_user_event, 
                                    first_visit_from_posts_user_event,
                                    first_visit_from_videos_user_event,
                                    first_visit_from_main_user_event,
                                    qna_first_visit_UV_base, 
                                    qna_more_cases,
                                    UV_daily,
                                    UV_daily_NewAndRepeat,
                                    user_pseudo_id_AND_user_id,
                                    user_first_visit_from]
user_first_visit_from >> [first_visit_from_kin_naver_event,
                          first_visit_from_google_event,
                          first_visit_from_search_naver_event]