#로앤컴퍼니는 Universal Time Coordinated(이하 UTC) 기준이다. 그러므로 Airflow execution_date 설정할 때 고려해야 함.


######################################################################################################

import datetime

from airflow import models
from airflow.operators import bash
from airflow.operators import dummy

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.transfers import bigquery_to_gcs


######################################################################################################

default_args = {
    'owner': 'CAHNJUN KIM',
    'email': ['cj.kim@lawcompany.co.kr'],
    'email_on_retry': True,
    'email_on_failure': True,
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=3),
    'max_active_runs' : 1,
    'max_active_tasks' : 1,
    'start_date': '2022-06-15',
}


with models.DAG(
        dag_id = 'lawyer_management_220615_04_CJK',
        description = 'Data Workflow for Data Mart of lawyer_management after 20200615',
        schedule_interval=datetime.timedelta(days=1),
        # schedule_interval= '00 1 * * *', #한국시간기준 매일 아침 10시 00분에 스타트
        default_args=default_args) as dag:

    start = dummy.DummyOperator(task_id='start')
'''
    check_bm_activities = BigQueryTableExistenceSensor(
        #빅쿼리 예약된 쿼리로 만들어진 테이블
        task_id = 'check_bm_activities',
        project_id = 'lawtalk-bigquery',
        timeout = 1 * 60 * 60,
        dataset_id= 'lawyer_management_bm_activities',
        table_id = '0_total_{{ ds_nodash }}',
    )
'''

    # start>>check_bm_activities

    users_x = BigQueryExecuteQueryOperator(
        task_id='users_x',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.users_x',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            WITH users as
            (
              SELECT
                _id as users_id
                ,username
                ,DATETIME(createdAt, 'Asia/Seoul') as createdAt
                ,DATETIME(updatedAt, 'Asia/Seoul') as updatedAt
                ,date(DATETIME(birth, 'Asia/Seoul')) as birth_date
                ,email
                ,isEmailAccept
                ,emailMarketingAccept
                ,smsMarketingAccept
                ,isSMSAccept
                ,lawyer
                ,role
                ,sex
              FROM `lawtalk-bigquery.raw.users`
              WHERE 1=1
            --     AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '2021-12-12'
                  AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            )

            , emailMarketingAccept_unwind as
            (
              SELECT
                users_id
                ,emailMarketingAccept
                ,emailMarketingAccept_status
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', emailMarketingAccept_startDate), 'Asia/Seoul') AS emailMarketingAccept_startDate
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', emailMarketingAccept_endDate), 'Asia/Seoul') AS emailMarketingAccept_endDate
              FROM
              (
                SELECT
                  users_id
                  ,emailMarketingAccept
                  ,regexp_extract_all(emailMarketingAccept, r"'status': (\w+)") as emailMarketingAccept_status
                  ,regexp_extract_all(emailMarketingAccept, r"'startDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as emailMarketingAccept_startDate
                  ,regexp_extract_all(emailMarketingAccept, r"'endDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as emailMarketingAccept_endDate
                FROM users
                WHERE 1=1
              )
              ,unnest(emailMarketingAccept_status) as emailMarketingAccept_status with offset as pos1
              ,unnest(emailMarketingAccept_startDate) as emailMarketingAccept_startDate with offset as pos2
              ,unnest(emailMarketingAccept_endDate) as emailMarketingAccept_endDate with offset as pos3
              WHERE 1=1
                AND (pos1=pos2 AND pos2=pos3 and pos1=pos3)
            )

            , smsMarketingAccept_unwind as
            (
              SELECT
                users_id
                ,smsMarketingAccept
                ,smsMarketingAccept_status
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', smsMarketingAccept_startDate), 'Asia/Seoul') AS smsMarketingAccept_startDate
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', smsMarketingAccept_endDate), 'Asia/Seoul') AS smsMarketingAccept_endDate
              FROM
              (
                SELECT
                  users_id
                  ,smsMarketingAccept
                  ,regexp_extract_all(smsMarketingAccept, r"'status': (\w+)") as smsMarketingAccept_status
                  ,regexp_extract_all(smsMarketingAccept, r"'startDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as smsMarketingAccept_startDate
                  ,regexp_extract_all(smsMarketingAccept, r"'endDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as smsMarketingAccept_endDate
                FROM users
                WHERE 1=1
              )
              ,unnest(smsMarketingAccept_status) as smsMarketingAccept_status with offset as pos1
              ,unnest(smsMarketingAccept_startDate) as smsMarketingAccept_startDate with offset as pos2
              ,unnest(smsMarketingAccept_endDate) as smsMarketingAccept_endDate with offset as pos3
              WHERE 1=1
                AND (pos1=pos2 AND pos2=pos3 and pos1=pos3)
            )

            SELECT
              A.users_id
              ,createdAt
              ,updatedAt
              ,username
              ,role
              ,lawyer
              ,birth_date
              ,sex
              ,email
              ,isEmailAccept
              ,case when B.users_id is not null then True else False end as isEmailMarketingAccept
              ,emailMarketingAccept
              ,isSMSAccept
              ,case when C.users_id is not null then True else False end as isSMSMarketingAccept
              ,smsMarketingAccept
              FROM users as A
            LEFT OUTER JOIN
            (
              SELECT
                users_id
              FROM emailMarketingAccept_unwind
              WHERE 1=1
                and emailMarketingAccept_status = 'True'
              --     and '2021-12-12' between date(emailMarketingAccept_startDate) and (emailMarketingAccept_endDate)
                and '{{ ds }}' between date(emailMarketingAccept_startDate) and date(emailMarketingAccept_endDate)
              group by 1
            ) as B on A.users_id = B.users_id
            LEFT OUTER JOIN
            (
              SELECT
                users_id
              FROM smsMarketingAccept_unwind
              WHERE 1=1
                and smsMarketingAccept_status = 'True'
              --     and '2021-12-12' between date(smsMarketingAccept_startDate) and date(smsMarketingAccept_endDate)
                and '{{ ds }}' between date(smsMarketingAccept_startDate) and date(smsMarketingAccept_endDate)
              group by 1
            ) as C on A.users_id = C.users_id
            '''
    )

    users_x_snapshot = BigQueryExecuteQueryOperator(
        task_id='users_x_snapshot',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.users_x_snapshot_{{ ds_nodash }}',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            WITH users as
            (
              SELECT
                _id as users_id
                ,username
                ,DATETIME(createdAt, 'Asia/Seoul') as createdAt
                ,DATETIME(updatedAt, 'Asia/Seoul') as updatedAt
                ,date(DATETIME(birth, 'Asia/Seoul')) as birth_date
                ,email
                ,isEmailAccept
                ,emailMarketingAccept
                ,smsMarketingAccept
                ,isSMSAccept
                ,lawyer
                ,role
                ,sex
              FROM `lawtalk-bigquery.raw.users`
              WHERE 1=1
            --     AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '2021-12-12'
                  AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            )

            , emailMarketingAccept_unwind as
            (
              SELECT
                users_id
                ,emailMarketingAccept
                ,emailMarketingAccept_status
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', emailMarketingAccept_startDate), 'Asia/Seoul') AS emailMarketingAccept_startDate
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', emailMarketingAccept_endDate), 'Asia/Seoul') AS emailMarketingAccept_endDate
              FROM
              (
                SELECT
                  users_id
                  ,emailMarketingAccept
                  ,regexp_extract_all(emailMarketingAccept, r"'status': (\w+)") as emailMarketingAccept_status
                  ,regexp_extract_all(emailMarketingAccept, r"'startDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as emailMarketingAccept_startDate
                  ,regexp_extract_all(emailMarketingAccept, r"'endDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as emailMarketingAccept_endDate
                FROM users
                WHERE 1=1
              )
              ,unnest(emailMarketingAccept_status) as emailMarketingAccept_status with offset as pos1
              ,unnest(emailMarketingAccept_startDate) as emailMarketingAccept_startDate with offset as pos2
              ,unnest(emailMarketingAccept_endDate) as emailMarketingAccept_endDate with offset as pos3
              WHERE 1=1
                AND (pos1=pos2 AND pos2=pos3 and pos1=pos3)
            )

            , smsMarketingAccept_unwind as
            (
              SELECT
                users_id
                ,smsMarketingAccept
                ,smsMarketingAccept_status
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', smsMarketingAccept_startDate), 'Asia/Seoul') AS smsMarketingAccept_startDate
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', smsMarketingAccept_endDate), 'Asia/Seoul') AS smsMarketingAccept_endDate
              FROM
              (
                SELECT
                  users_id
                  ,smsMarketingAccept
                  ,regexp_extract_all(smsMarketingAccept, r"'status': (\w+)") as smsMarketingAccept_status
                  ,regexp_extract_all(smsMarketingAccept, r"'startDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as smsMarketingAccept_startDate
                  ,regexp_extract_all(smsMarketingAccept, r"'endDate': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as smsMarketingAccept_endDate
                FROM users
                WHERE 1=1
              )
              ,unnest(smsMarketingAccept_status) as smsMarketingAccept_status with offset as pos1
              ,unnest(smsMarketingAccept_startDate) as smsMarketingAccept_startDate with offset as pos2
              ,unnest(smsMarketingAccept_endDate) as smsMarketingAccept_endDate with offset as pos3
              WHERE 1=1
                AND (pos1=pos2 AND pos2=pos3 and pos1=pos3)
            )

            SELECT
              A.users_id
              ,createdAt
              ,updatedAt
              ,username
              ,role
              ,lawyer
              ,birth_date
              ,sex
              ,email
              ,isEmailAccept
              ,case when B.users_id is not null then True else False end as isEmailMarketingAccept
              ,emailMarketingAccept
              ,isSMSAccept
              ,case when C.users_id is not null then True else False end as isSMSMarketingAccept
              ,smsMarketingAccept
              FROM users as A
            LEFT OUTER JOIN
            (
              SELECT
                users_id
              FROM emailMarketingAccept_unwind
              WHERE 1=1
                and emailMarketingAccept_status = 'True'
              --     and '2021-12-12' between date(emailMarketingAccept_startDate) and (emailMarketingAccept_endDate)
                and '{{ ds }}' between date(emailMarketingAccept_startDate) and date(emailMarketingAccept_endDate)
              group by 1
            ) as B on A.users_id = B.users_id
            LEFT OUTER JOIN
            (
              SELECT
                users_id
              FROM smsMarketingAccept_unwind
              WHERE 1=1
                and smsMarketingAccept_status = 'True'
              --     and '2021-12-12' between date(smsMarketingAccept_startDate) and date(smsMarketingAccept_endDate)
                and '{{ ds }}' between date(smsMarketingAccept_startDate) and date(smsMarketingAccept_endDate)
              group by 1
            ) as C on A.users_id = C.users_id
            '''
    )

    lawyers_x = BigQueryExecuteQueryOperator(
        task_id='lawyers_x',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyers_x',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            SELECT
              _id as lawyers_id
             -- ,TIMESTAMP_ADD(createdAt, INTERVAL  9 HOUR) as createdAt
              ,DATETIME(createdAt, 'Asia/Seoul') as createdAt
              ,DATETIME(updatedAt, 'Asia/Seoul') as updatedAt
              ,role
              ,slug
              ,manager
              ,company
              ,name
              ,date(DATETIME(birth, 'Asia/Seoul')) as birth_date
              ,sex
              ,regexp_extract_all(examination, r"'body': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'")[safe_offset(0)] as examination_body
              ,cast(examYear as int64) as examYear
              ,regexp_extract_all(examination, r"'year': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'")[safe_offset(0)] as examination_year
              ,regexp_extract_all(examination, r"'trainingInstitute': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'")[safe_offset(0)] as examination_trainingInstitute
              ,flag
              ,writeRate
              ,case when REGEXP_CONTAINS(writeRate, "'percentage': 100") then True else False end as isFullProfile
              ,case when REGEXP_CONTAINS(flag, "'activation': True") then True else False end as flag_activation
              ,case when REGEXP_CONTAINS(flag, "'holdWithdrawal': True") then True else False end as flag_holdWithdrawal
              ,case when REGEXP_CONTAINS(flag, "'activation': True") and REGEXP_CONTAINS(flag, "'holdWithdrawal': False") then True else False end as isActive
              ,case when _id = '5e314482c781c20602690b79' then True else False end as is_Withdrawal
            FROM `lawtalk-bigquery.raw.lawyers`
            WHERE 1=1
            --   AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '2021-12-12'
              AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            '''
    )

    lawyers_x_snapshot = BigQueryExecuteQueryOperator(
        task_id='lawyers_x_snapshot',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyers_x_snapshot_{{ ds_nodash }}',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            SELECT
              _id as lawyers_id
             -- ,TIMESTAMP_ADD(createdAt, INTERVAL  9 HOUR) as createdAt
              ,DATETIME(createdAt, 'Asia/Seoul') as createdAt
              ,DATETIME(updatedAt, 'Asia/Seoul') as updatedAt
              ,role
              ,slug
              ,manager
              ,company
              ,name
              ,date(DATETIME(birth, 'Asia/Seoul')) as birth_date
              ,sex
              ,regexp_extract_all(examination, r"'body': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'")[safe_offset(0)] as examination_body
              ,cast(examYear as int64) as examYear
              ,regexp_extract_all(examination, r"'year': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'")[safe_offset(0)] as examination_year
              ,regexp_extract_all(examination, r"'trainingInstitute': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'")[safe_offset(0)] as examination_trainingInstitute
              ,flag
              ,writeRate
              ,case when REGEXP_CONTAINS(writeRate, "'percentage': 100") then True else False end as isFullProfile
              ,case when REGEXP_CONTAINS(flag, "'activation': True") then True else False end as flag_activation
              ,case when REGEXP_CONTAINS(flag, "'holdWithdrawal': True") then True else False end as flag_holdWithdrawal
              ,case when REGEXP_CONTAINS(flag, "'activation': True") and REGEXP_CONTAINS(flag, "'holdWithdrawal': False") then True else False end as isActive
              ,case when _id = '5e314482c781c20602690b79' then True else False end as is_Withdrawal
            FROM `lawtalk-bigquery.raw.lawyers`
            WHERE 1=1
            --   AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '2021-12-12'
              AND date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            '''
    )

    adorders_history = BigQueryExecuteQueryOperator(
        task_id='adorders_history',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_history',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            SELECT
                A.adorders_id
                ,A.lawyers_id
                ,A.adorders_createdAt_date
                ,date(A.adorders_startAt_timestamp) as adorders_start_date
                ,date(A.adorders_endAt_timestamp) as adorders_end_date
                ,A.adorders_status
                ,C.adpayments_status
                ,C.adpayments_method
            FROM
            (
                select
                  _id as adorders_id
                  ,lawyer as lawyers_id
                  ,status as adorders_status
                  ,date(DATETIME(createdAt,'Asia/Seoul')) as adorders_createdAt_date
            --       ,regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as adorders_startAt
                  ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul')  as adorders_startAt_timestamp
            --       ,regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as adorders_endAt
                  ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as adorders_endAt_timestamp
                from `lawtalk-bigquery.raw.adorders`
                where date(DATETIME(createdAt,'Asia/Seoul')) <= '{{ ds }}'
            -- where date(createdAt) <= '{{ yesterday_ds }}'
            ) as A
            left outer join
            (
                SELECT
                  orders_id
                  ,_id as adpayments_id
                --   ,REGEXP_EXTRACT_ALL(orders, r"ObjectId\('(.*?)'\)")
                FROM `lawtalk-bigquery.raw.adpayments`, unnest(REGEXP_EXTRACT_ALL(orders, r"ObjectId\('(.*?)'\)")) as orders_id
                where date(DATETIME(createdAt,'Asia/Seoul')) <= '{{ ds }}'
                --     where date(createdAt) <= '2021-11-28'
            ) as B on A.adorders_id = B.orders_id
            left outer join
            (
                select
                  _id as adpayments_id
                  ,method as adpayments_method
                  ,status as adpayments_status
                from `lawtalk-bigquery.raw.adpayments`
                where date(DATETIME(createdAt,'Asia/Seoul')) <= '{{ ds }}'
                --     where date(createdAt) <= '2021-11-28'
            ) as C on B.adpayments_id = C.adpayments_id
            '''
    )

    adorders_pausehistory = BigQueryExecuteQueryOperator(
        task_id='adorders_pausehistory',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_pausehistory',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            select
              adorders_id
              ,pauseHistory_startAt
              ,pauseHistory_endAt
              ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_startAt), 'Asia/Seoul') as pauseHistory_startAt_timestamp
              ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_endAt), 'Asia/Seoul') as pauseHistory_endAt_timestamp
              ,date(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_startAt), 'Asia/Seoul')) as pauseHistory_startAt_date
              ,date(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_endAt), 'Asia/Seoul')) as pauseHistory_endAt_date
            from
            (
              SELECT
                _id as adorders_id
                ,pauseHistory
                ,regexp_extract_all(pauseHistory, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as pauseHistory_startAt
                ,regexp_extract_all(pauseHistory, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as pauseHistory_endAt
              FROM `lawtalk-bigquery.raw.adorders`
              where 1=1
                and date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
                and REGEXP_CONTAINS(pauseHistory, 'ObjectId')
            )
            ,unnest(pauseHistory_startAt) as pauseHistory_startAt with offset as pos1
            ,unnest(pauseHistory_endAt) as pauseHistory_endAt with offset as pos2
            where 1=1
              and pos1=pos2
            --  and _id = '5e7b001d73a92f0260ab5935'
            --  and _id = '5ee77c88c9070601481e9c93'
            '''
    )

    adorders_active_daily = BigQueryExecuteQueryOperator(
        task_id='adorders_active_daily',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_active_daily',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            select
              lawyer_advertisement_term_daily.* except(Date)
              ,lawyer_advertisement_term_daily.date as advertisement_active_date
            from
            (
              select
                A.*
                ,B.DATE
              from `lawtalk-bigquery.lawyer_management.adorders_history` as A
              CROSS JOIN `lawtalk-bigquery.lawyer_management.dim_date` as B
              where 1=1
                and (A.adorders_status = 'apply' and A.adpayments_status = 'paid')
                and parse_date('%F', B.date) between A.adorders_start_date and A.adorders_end_date
            ) as lawyer_advertisement_term_daily
            left outer join
            (
              select
                A.*
                ,B.Date
              from `lawtalk-bigquery.lawyer_management.adorders_pausehistory` as A
              CROSS JOIN `lawtalk-bigquery.lawyer_management.dim_date` as B
              where 1=1
                and parse_date('%F', B.date) between A.pauseHistory_startAt_date and A.pauseHistory_endAt_date
            ) as lawyer_advertisement_pause_daily
                on lawyer_advertisement_term_daily.adorders_id = lawyer_advertisement_pause_daily.adorders_id
                  and lawyer_advertisement_term_daily.DATE = lawyer_advertisement_pause_daily.Date
            where 1=1
              and lawyer_advertisement_pause_daily.adorders_id is null

            -- order by adorders_id asc, advertisement_active_date asc
            '''
    )

    betaadorders_active_daily = BigQueryExecuteQueryOperator(
        task_id='betaadorders_active_daily',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.betaadorders_active_daily',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            select
              A.*
              ,B.date as beta_advertisement_active_date
            from
            (
              select
                *
                ,date(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul'))  as betaadorders_startAt_date
                ,date(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul')) as betaadorders_endAt_date
              from `lawtalk-bigquery.raw.betaadorders`
              where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            ) as A
            CROSS JOIN `lawtalk-bigquery.lawyer_management.dim_date` as B
            where 1=1
              and A.status = 'apply'
              and parse_date('%F', B.date) between A.betaadorders_startAt_date and A.betaadorders_endAt_date
            '''
    )

    lawyers_basic = BigQueryExecuteQueryOperator(
        task_id='lawyers_basic',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyers_basic',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            select
              lawyers.lawyers_id
              ,lawyers.slug
              ,lawyers.manager as BM
              ,lawyers.createdAt
              ,lawyers.updatedAt
              ,users.users_id
              ,users.username
              ,users.role as users_role
              ,lawyers.role as lawyers_role
              ,lawyers.company
              ,lawyers.name
              ,lawyers.birth_date
              ,lawyers.sex
              ,lawyers.examination_body
              ,lawyers.examYear
              ,lawyers.examination_year
              ,lawyers.examination_trainingInstitute
              ,users.email
              ,users.isEmailAccept
              ,users.isEmailMarketingAccept
              ,users.isSMSAccept
              ,users.isSMSMarketingAccept
              ,lawyers.flag
              ,lawyers.writeRate
              ,lawyers.isFullProfile
              ,lawyers.flag_activation
              ,lawyers.flag_holdWithdrawal
              ,lawyers.isActive
              ,lawyers.is_Withdrawal
              ,case when adorders_active_daily.lawyers_id is not null then True else False end as is_adorders_active
              ,case when betaadorders_active_daily.lawyers_id is not null then True else False end as is_betaadorders_active
              ,case when adorders_active_daily.lawyers_id is not null or betaadorders_active_daily.lawyers_id is not null then True else False end as isAd
            from `lawtalk-bigquery.lawyer_management.lawyers_x` as lawyers
            left outer join `lawtalk-bigquery.lawyer_management.users_x` as users on lawyers.lawyers_id = users.lawyer
            left outer join
            (
              select
                lawyers_id
              from `lawtalk-bigquery.lawyer_management.adorders_active_daily`
              where parse_date('%F', advertisement_active_date ) = '{{ ds }}'
              group by 1
            ) as adorders_active_daily on lawyers.lawyers_id = adorders_active_daily.lawyers_id
            left outer join
            (
              select
                  lawyer as lawyers_id
              from `lawtalk-bigquery.lawyer_management.betaadorders_active_daily`
              where parse_date('%F', beta_advertisement_active_date) = '{{ ds }}'
              group by 1
            ) as betaadorders_active_daily on lawyers.lawyers_id = betaadorders_active_daily.lawyers_id
            where 1=1
              and users.role in ('lawyer','lawyer-waiting')
              and lawyers.role in ('lawyer','lawyer-waiting')
            '''
    )

    lawyers_basic_snapshot = BigQueryExecuteQueryOperator(
        task_id='lawyers_basic_snapshot',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyers_basic_snapshot_{{ ds_nodash }}',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            select
              lawyers.lawyers_id
              ,lawyers.slug
              ,lawyers.manager as BM
              ,lawyers.createdAt
              ,lawyers.updatedAt
              ,users.users_id
              ,users.username
              ,users.role as users_role
              ,lawyers.role as lawyers_role
              ,lawyers.company
              ,lawyers.name
              ,lawyers.birth_date
              ,lawyers.sex
              ,lawyers.examination_body
              ,lawyers.examYear
              ,lawyers.examination_year
              ,lawyers.examination_trainingInstitute
              ,users.email
              ,users.isEmailAccept
              ,users.isEmailMarketingAccept
              ,users.isSMSAccept
              ,users.isSMSMarketingAccept
              ,lawyers.flag
              ,lawyers.writeRate
              ,lawyers.isFullProfile
              ,lawyers.flag_activation
              ,lawyers.flag_holdWithdrawal
              ,lawyers.isActive
              ,lawyers.is_Withdrawal
              ,case when adorders_active_daily.lawyers_id is not null then True else False end as is_adorders_active
              ,case when betaadorders_active_daily.lawyers_id is not null then True else False end as is_betaadorders_active
              ,case when adorders_active_daily.lawyers_id is not null or betaadorders_active_daily.lawyers_id is not null then True else False end as isAd
            from `lawtalk-bigquery.lawyer_management.lawyers_x` as lawyers
            left outer join `lawtalk-bigquery.lawyer_management.users_x` as users on lawyers.lawyers_id = users.lawyer
            left outer join
            (
              select
                lawyers_id
              from `lawtalk-bigquery.lawyer_management.adorders_active_daily`
              where parse_date('%F', advertisement_active_date ) = '{{ ds }}'
              group by 1
            ) as adorders_active_daily on lawyers.lawyers_id = adorders_active_daily.lawyers_id
            left outer join
            (
              select
                  lawyer as lawyers_id
              from `lawtalk-bigquery.lawyer_management.betaadorders_active_daily`
              where parse_date('%F', beta_advertisement_active_date) = '{{ ds }}'
              group by 1
            ) as betaadorders_active_daily on lawyers.lawyers_id = betaadorders_active_daily.lawyers_id
            where 1=1
              and users.role in ('lawyer','lawyer-waiting')
              and lawyers.role in ('lawyer','lawyer-waiting')
            '''
    )

    lawyer_mgmt_segment = BigQueryExecuteQueryOperator(
        task_id='lawyer_mgmt_segment',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyer_mgmt_segment',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            With labeling_depth_1 as --#
            (
              select
                *
                ,case when is_Withdrawal is True then '3.탈퇴변호사'
                      when users_role = 'lawyer-waiting' then '2.회원미승인변호사'
                      else '1.회원변호사' end as label_depth_1
              from `lawtalk-bigquery.lawyer_management.lawyers_basic`
            )

            , labeling_depth_2 as --#
            (
              select
                *
                ,case when label_depth_1 in ('3.탈퇴변호사','2.회원미승인변호사') then label_depth_1
                      when  isActive is True and isFullProfile is True then '1.1.공개변호사'
                      else '1.2.비공개변호사' end label_depth_2
              from labeling_depth_1
            )

            , labeling_depth_3 as --#
            (
              select
                *
                ,case when label_depth_2 in ('3.탈퇴변호사','2.회원미승인변호사') then label_depth_2
                      when label_depth_2 = '1.1.공개변호사' and isAd is True then '1.1.1.공개_광고변호사'
                      when label_depth_2 = '1.1.공개변호사' then '1.1.2.공개_비광고변호사'
                      when label_depth_2 = '1.2.비공개변호사' and isActive is False then '1.2.1.비공개변호사_일시정지'
                      when label_depth_2 = '1.2.비공개변호사' and isFullProfile is False then '1.2.2.비공개변호사_프로필작성_미완료'
                      else 'to_check' end as label_depth_3
              from labeling_depth_2
            )


            , labeling_depth_4 as --#
            (
            select
            *
            ,case when label_depth_3 in ('3.탈퇴변호사','2.회원미승인변호사') then label_depth_3
            when label_depth_2 = '1.1.공개변호사' then label_depth_3
            when label_depth_3 = '1.2.2.비공개변호사_프로필작성_미완료' then label_depth_3
            when label_depth_3 = '1.2.1.비공개변호사_일시정지' and isFullProfile is True then '1.2.1.1.비공개변호사_일시정지_프로필작성_완료'
            when label_depth_3 = '1.2.1.비공개변호사_일시정지' then '1.2.1.2.비공개변호사_일시정지_프로필작성_미완료'
            else 'to_check' end as label_depth_4

            from labeling_depth_3
            )

            select
            *
            from labeling_depth_4
            '''
    )

    lawyer_mgmt_segment_snapshot = BigQueryExecuteQueryOperator(
        task_id='lawyer_mgmt_segment_snapshot',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyer_mgmt_segment_snapshot_{{ ds_nodash }}',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            With labeling_depth_1 as --#
            (
              select
                *
                ,case when is_Withdrawal is True then '3.탈퇴변호사'
                      when users_role = 'lawyer-waiting' then '2.회원미승인변호사'
                      else '1.회원변호사' end as label_depth_1
              from `lawtalk-bigquery.lawyer_management.lawyers_basic`
            )

            , labeling_depth_2 as --#
            (
              select
                *
                ,case when label_depth_1 in ('3.탈퇴변호사','2.회원미승인변호사') then label_depth_1
                      when  isActive is True and isFullProfile is True then '1.1.공개변호사'
                      else '1.2.비공개변호사' end label_depth_2
              from labeling_depth_1
            )

            , labeling_depth_3 as --#
            (
              select
                *
                ,case when label_depth_2 in ('3.탈퇴변호사','2.회원미승인변호사') then label_depth_2
                      when label_depth_2 = '1.1.공개변호사' and isAd is True then '1.1.1.공개_광고변호사'
                      when label_depth_2 = '1.1.공개변호사' then '1.1.2.공개_비광고변호사'
                      when label_depth_2 = '1.2.비공개변호사' and isActive is False then '1.2.1.비공개변호사_일시정지'
                      when label_depth_2 = '1.2.비공개변호사' and isFullProfile is False then '1.2.2.비공개변호사_프로필작성_미완료'
                      else 'to_check' end as label_depth_3
              from labeling_depth_2
            )


            , labeling_depth_4 as --#
            (
            select
            *
            ,case when label_depth_3 in ('3.탈퇴변호사','2.회원미승인변호사') then label_depth_3
            when label_depth_2 = '1.1.공개변호사' then label_depth_3
            when label_depth_3 = '1.2.2.비공개변호사_프로필작성_미완료' then label_depth_3
            when label_depth_3 = '1.2.1.비공개변호사_일시정지' and isFullProfile is True then '1.2.1.1.비공개변호사_일시정지_프로필작성_완료'
            when label_depth_3 = '1.2.1.비공개변호사_일시정지' then '1.2.1.2.비공개변호사_일시정지_프로필작성_미완료'
            else 'to_check' end as label_depth_4

            from labeling_depth_3
            )

            select
            *
            from labeling_depth_4
            '''
    )

    adorders_history_with_ad_contract_categroies = BigQueryExecuteQueryOperator(
        task_id='adorders_history_with_ad_contract_categroies',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_history_with_ad_contract_categroies',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            with ad_active_date_by_lawyer as
            (
              select
                slug
                ,advertisement_active_date
              from
              (
                SELECT
                  A.*
                  ,B.slug
                FROM
                (
                  select
                    lawyer_advertisement_term_daily.* except(Date)
                    ,parse_date('%F', lawyer_advertisement_term_daily.date) as advertisement_active_date
                  from
                  (
                    select
                      A.*
                      ,B.DATE
                    from `lawtalk-bigquery.lawyer_management.adorders_history` as A
                    CROSS JOIN `lawtalk-bigquery.lawyer_management.dim_date` as B
                    where 1=1
                      and (A.adorders_status = 'apply' and A.adpayments_status = 'paid')
                      and parse_date('%F', B.date) between A.adorders_start_date and A.adorders_end_date
                  ) as lawyer_advertisement_term_daily
                ) as A
                left outer join `lawtalk-bigquery.lawyer_management.lawyer_mgmt_segment` as B on A.lawyers_id = B.lawyers_id
              )
              where slug is not null
              group by 1,2
            )

            -- select
            --   *
            -- from ad_active_date_by_lawyer
            -- order by 1,2

            , dim_date as
            (
              select
                parse_date('%F', date) as date
              from `lawtalk-bigquery.lawyer_management.dim_date`
              group by 1
            )

            -- select
            --   *
            -- from dim_date

            , ad_period_with_dim_date as
            (
              select
                *
              from
              (
                select
                  slug
                  ,min(advertisement_active_date) as min_ad_date
                  ,max(advertisement_active_date) as max_ad_date
                from ad_active_date_by_lawyer
                group by 1
              ) as A
              cross join dim_date
              where 1=1
                and date >= min_ad_date
                and date <= current_date('Asia/Seoul')
            )

            -- select
            --   *
            -- from ad_period_with_dim_date
            -- order by 1,2,4

            -- , ad_contract_categroies as
            -- (
              select
                *
            --     ,lag(ad_contract_continue_status,1) over(partition by slug order by yyyymm) as ad_contract_continue_status_lastmonth
              from
              (
                select
                  *
                  , case when is_ad_date = is_ad_lastday then 'same' else 'change' end as ad_status_changed
                  , case when day_cnt_after_ad_start <= 91 then 'new'
                       else 'existing' end as is_new_customer
                  , case when day_cnt_after_ad_start = 1 then 'acquisition'
                       when is_ad_lastday is True and is_ad_date is True then 'retention'
                       when is_ad_lastday is True and is_ad_date is False then 'churning' --churning
                       when is_ad_lastday is False and is_ad_date is False then 'churned'
                       when is_ad_lastday is False and is_ad_date is True then 'winback'
                       else null end as ad_contract_continue_status
                from
                (
                  select
                    A.*
                    ,row_number() over(partition by A.slug order by date) as day_cnt_after_ad_start
                    ,case when B.slug is not null then True else False end as is_ad_date
                    ,lag(case when B.slug is not null then True else False end, 1) over(partition by A.slug order by date) as is_ad_lastday
                  from ad_period_with_dim_date as A
                  left outer join ad_active_date_by_lawyer as B on A.slug = B.slug and A.date = B.advertisement_active_date
                )
              )
            -- )
            '''
    )

    adorders_history_with_status_seg_info = BigQueryExecuteQueryOperator(
        task_id='adorders_history_with_status_seg_info',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_history_with_status_seg_info',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            with ad_contract_categorizing as
            (
              select -- best sample: 0120-서용진
                A.* --r
              --   ,max(B.month_after_ad_start)
                ,coalesce(A.day_cnt_after_ad_start - max(B.day_cnt_after_ad_start) + 1, A.day_cnt_after_ad_start) as day_cnt_by_ad_status_term
              from `lawtalk-bigquery.lawyer_management.adorders_history_with_ad_contract_categroies` as A
              left outer join
              (
                select
                  slug
                  ,date
                  ,day_cnt_after_ad_start
                from `lawtalk-bigquery.lawyer_management.adorders_history_with_ad_contract_categroies`
                where ad_status_changed = 'change'
              ) as B on A.slug = B.slug and A.date >= B.date
              group by 1,2,3,4,5,6,7,8,9,10
            )

            -- select * from ad_contract_categorizing
            -- where slug = '0120-서용진'
            -- order by 4


            -- select -- 로직 및 숫자 오류 없음
            --   ad_active_status_changed
            --   ,max(day_cnt_by_ad_status_term)
            -- from ad_contract_categorizing
            -- group by 1


            -- select -- 로직 및 숫자 오류 없음
            --   is_new_customer
            --   ,ad_contract_continue_status
            --   ,ad_status_changed
            --   ,max(day_cnt_by_ad_status_term)
            -- from ad_contract_categorizing
            -- group by 1,2,3
            -- order by 1,2,3



            select
              *
              ,lag(ad_contract_continue_status,1) over (partition by slug order by date) as ad_contract_continue_status_lastday
              ,lag(day_cnt_by_ad_status_term,1) over (partition by slug order by date) as day_cnt_by_ad_status_term_lastday
            from ad_contract_categorizing
            -- where slug = '0120-서용진'
            -- order by slug, date
            '''
    )

    adorders_item_adCategory = BigQueryExecuteQueryOperator(
        task_id='adorders_item_adCategory',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_item_adCategory',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            SELECT
              A.adorders_id
              ,A.item
              ,B.name as item_name
              ,B.price
              ,A.categories
              ,A.adCategoryId
              ,C.name as adCategory_name
            FROM
            (
              SELECT
               _id as adorders_id,
               item,
               categories,
               regexp_extract(categories, r"'adCategoryId': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as adCategoryId
              FROM `lawtalk-bigquery.raw.adorders`
              where date(DATETIME(createdAt,'Asia/Seoul')) <= '{{ ds }}'
              -- 6/15일 이후 수정된 쿼리
              -- SELECT
              --   _id as adorders_id,
              --   item,
              --   categories,
              --   adCategoryId
              -- FROM `lawtalk-bigquery.raw.adorders`
              --   , UNNEST(regexp_extract_all(categories, r"'adCategoryId': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)"))  as adCategoryId
              -- where date(DATETIME(createdAt,'Asia/Seoul')) <= '{{ ds }}'
            ) as A
            left outer join `lawtalk-bigquery.raw.adproducts` as B on A.item = B._id
            left outer join `lawtalk-bigquery.raw.adcategories` as C on A.adCategoryId = C._id
            '''
    )

    adorders_with_location_and_keyword_array = BigQueryExecuteQueryOperator(
        task_id='adorders_with_location_and_keyword_array',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_with_location_and_keyword_array',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
        #standardSQL
            with adorders_adloaction as
            (
              SELECT
                _id as adorders_id
            --     ,adLocations
                ,regexp_extract_all(adLocations, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") adLocations_id
              FROM `lawtalk-bigquery.raw.adorders`
            )

            ,adlocations_name as
            (
              select
                A._id,
                A.name as adlocations_name,
                A.adLocationGroup as adLocationGroup_id,
                B.name as adLocationGroup_id_name
              from `lawtalk-bigquery.raw.adlocations` as A
              left outer join `lawtalk-bigquery.raw.adlocationgroups` as B on A.adLocationGroup = B._id
            )

            , adlocation_info_array as
            (
              select
                adorders_id
                ,adlocation_name
              from
              (
                select
                  adorders_id
                  ,array
                  (
                    select
                      adLocationGroup_id_name
                    from unnest(adLocations_id) as adLocations_id
                    join adlocations_name on _id = adLocations_id
                  ) as adLocationGroup_id_name
                from adorders_adloaction
              ),unnest(adLocationGroup_id_name) as adlocation_name
              group by 1,2
            )


            --             select
            --               *
            --             from adlocation_info_array
            --             where adLocations_id is not null
            --             order by 1

            , adorders_keywords as
            (
              SELECT
                _id as adorders_id
            --     ,adLocations
                ,regexp_extract_all(keywords, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") keywords_id
              FROM `lawtalk-bigquery.raw.adorders`
            )

            ,adkeywords_name as #키워드광고인데 keyword_name이 안 엮이는 key값들이 있다
            (
              select
                adkeywords_id,
                adcategory_adkeyword
              from
              (
                select
                  adkeywords.*,
                  adcategories.name as adCategories_name
                  ,concat(adcategories.name,"_",adkeywords_name) as adcategory_adkeyword
                from
                (
                  SELECT
                    _id as adkeywords_id
                    ,name as adkeywords_name
                    ,regexp_extract(adCategories, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as adCategories_id
                  FROM `lawtalk-bigquery.raw.adkeywords`
                ) as adkeywords
                left outer join `lawtalk-bigquery.raw.adcategories` as adcategories on adkeywords.adCategories_id = adcategories._id
              )
                WHERE 1 = 1
                    AND adcategory_adkeyword IS NOT NULL
            )


            , adkeywords_info_array as
            (
            select
              *
              ,array
              (
                select
                  adcategory_adkeyword
                from unnest(keywords_id) as keywords_id
                join adkeywords_name on adkeywords_id = keywords_id
              ) as adkeywords_name
            from adorders_keywords
            )


            SELECT
              A.adorders_id,
              A.item,
              A.item_name,
              A.price,
              A.categories,
              A.adCategoryId,
              A.adCategory_name,
              B.* except(adorders_id),
              C.* except(adorders_id),
            FROM `lawtalk-bigquery.lawyer_management.adorders_item_adCategory` as A
            left outer join adlocation_info_array as B on A.adorders_id = B.adorders_id
            left outer join adkeywords_info_array as C on A.adorders_id = C.adorders_id
            '''
    )

    adorders_active_with_ad_kind = BigQueryExecuteQueryOperator(
        task_id='adorders_active_with_ad_kind',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adorders_active_with_ad_kind',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            with adorders_active as
            (
              SELECT
                adorders_id,
                lawyers_id,
                adorders_start_date,
                adorders_end_date,
                adorders_status,
                adpayments_status,
                adpayments_method
              FROM `lawtalk-bigquery.lawyer_management.adorders_active_daily`
              group by 1,2,3,4,5,6,7
            )

            , ad_kind_raw_1 as
            (
              SELECT
                adorders_id,
                adCategory_name as ad_name_2,
              FROM `lawtalk-bigquery.lawyer_management.adorders_with_location_and_keyword_array`
              where item_name in ('인기분야광고','일반분야광고','배너 변호사검색결과','상담순위 추천','프리미엄 메인','형량예측광고')
            )

            , ad_kind_raw_2 as
            (
              select
                adorders_id,
                adlocation_name as ad_name_2
              FROM `lawtalk-bigquery.lawyer_management.adorders_with_location_and_keyword_array`
              where item_name in ('지역광고')
            )

            , ad_kind_raw_3 as
            (
              select
                adorders_id,
                adkeywords_name as ad_name_2
              FROM `lawtalk-bigquery.lawyer_management.adorders_with_location_and_keyword_array`, unnest(adkeywords_name) as adkeywords_name
              where item_name in ('다이렉트 검색','키워드광고','프리미엄 검색')
            )

            , ad_kind_raw_total as
            (
              select * from ad_kind_raw_1
              union all select * from ad_kind_raw_2
              union all select * from ad_kind_raw_3
            )

            , adorders_active_with_ad_kind as
            (
              select
                A.adorders_id,
                A.lawyers_id,
                A.adorders_start_date,
                A.adorders_end_date,
                B.item_name as ad_name_1,
                C.ad_name_2,
                A.adorders_status,
                A.adpayments_status,
                A.adpayments_method
              from adorders_active as A
              left outer join
              (
                select
                  adorders_id
                  ,item_name
                from `lawtalk-bigquery.lawyer_management.adorders_with_location_and_keyword_array`
              ) as B on A.adorders_id = B.adorders_id
              left outer join ad_kind_raw_total as C on A.adorders_id = C.adorders_id
            )

            , betaadorders_active as
            (
              SELECT
                A._id as adorders_id
                ,A.lawyer as lawyers_id
                ,A.betaadorders_startAt_date as adorders_start_date
                ,A.betaadorders_endAt_date as adorders_end_date
                ,'플러스광고' as ad_name_1
                ,B.name as ad_name_2
                ,A.status as adorders_status
                ,'' as adpayments_status
                ,'' as adpayments_method
              FROM `lawtalk-bigquery.lawyer_management.betaadorders_active_daily` as A
              left outer join `lawtalk-bigquery.raw.adcategories` as B on A.adCategory = B._id
              group by 1,2,3,4,5,6,7,8,9
            )


            -- select * from ad_kind_raw_total #79445
            -- select * from adorders_active #49215
            -- select * from adorders_active_with_ad_kind #68287

            select * from adorders_active_with_ad_kind
            union all
            select * from betaadorders_active
            '''
    )

    questions_x = BigQueryExecuteQueryOperator(
        task_id='questions_x',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.questions_x',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            with questions as
            (
              SELECT
                _id,
                number,
                createdAt,
                editedAt,
                updatedAt,
                user,
                origin,
                slug,
                categories,
                regexp_extract_all(categories, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as categories_array,
                keywords,
                regexp_extract_all(keywords, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as keywords_array,
                -- prevCategories,
                -- etcCategories,
                -- prevKeywords,
                title,
                body,
                -- kinRaw,
                -- kinRemoved,
                -- answers,
                bestAnswer,
                -- blindedAnswers,
                -- dateOfanswer,
                -- editCount,
                -- exportable,
                -- favorites,
                -- isContents,
                -- isDelay,
                shareCount,
                viewCount,
                -- reject,
                -- rejectFlag,
              FROM `lawtalk-bigquery.raw.questions`
              where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            --     and _id in ('5a7c3d720a331c28fc999119', '563305ccc19e60483a8f06c3')
            )

            , questions_x as
            (
              select
                * except (categories_array, keywords_array),
              --     array
              --   (
              --     select
              --       name
              --     from unnest(categories_array) as categories_id
              --     join `lawtalk-bigquery.raw.adcategories` on categories_id = _id
              --   ) as adcategorie_name_array,
                ARRAY_TO_STRING
                (
                  array
                  (
                    select
                      name
                    from unnest(categories_array) as categories_id
                    join `lawtalk-bigquery.raw.adcategories` on categories_id = _id
                  ), ','
                 )as adcategorie_name,
              --   array
              --   (
              --     select
              --       name
              --     from unnest(keywords_array) as keywords_id
              --     join `lawtalk-bigquery.raw.adkeywords` on keywords_id = _id
              --   ) as keyword_name
              from questions
            )

            select
              *
            from questions_x
            '''
    )

    questions_answers = BigQueryExecuteQueryOperator(
        task_id='questions_answers',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.questions_answers',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            SELECT
              _id as questions_id
              ,answers_id
            FROM
            (
              SELECT
                _id,
                regexp_extract_all(answers, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") AS answers_id
              FROM `lawtalk-bigquery.raw.questions`
              where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            ), UNNEST(answers_id) as answers_id
            '''
    )

    answers_x = BigQueryExecuteQueryOperator(
        task_id='answers_x',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.answers_x',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            SELECT
              A.*
              ,B.origin as question_origin
              ,B.adcategorie_name
            FROM
            (
              SELECT
                _id
                ,DATETIME(createdAt, 'Asia/Seoul') as createdAt
                ,DATETIME(updatedAt, 'Asia/Seoul') as updatedAt
                ,question
                ,lawyer
                ,exportable
                ,recommended
                ,score
                ,shareCount
                ,usefulThings
                ,case when usefulThings is null or usefulThings = '[]' then False else True end as is_useful
                ,isAdopted
              FROM `lawtalk-bigquery.raw.answers`
              where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            ) as A
            left outer join
            (
              select
                _id
                ,origin
                ,adcategorie_name
              from `lawtalk-bigquery.lawyer_management.questions_x`
            ) as B on A.question = B._id
            left outer join
            (
              select
                answers_id
              from `lawyer_management.questions_answers`
            ) as c on A._id = C.answers_id
            where C.answers_id is not null
            '''
    )

    lawyer_answers_cnt_history = BigQueryExecuteQueryOperator(
        task_id='lawyer_answers_cnt_history',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyer_answers_cnt_history',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            SELECT
              lawyer

              ,struct
              (
                count(_id) as lifetime
                ,count(case when date(createdAt) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
                ,count(case when date(createdAt) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
                ,count(case when date(createdAt) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
                ,count(case when date(createdAt) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
              ) as answers_cnt

              ,struct
              (
                count(_id) as lifetime
                ,count(case when recommended = true and date(createdAt) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
                ,count(case when recommended = true and date(createdAt) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
                ,count(case when recommended = true and date(createdAt) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
                ,count(case when recommended = true and date(createdAt) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
              ) as answers_recommended_cnt

              ,struct
              (
                count(_id) as lifetime
                ,count(case when is_useful = true and date(createdAt) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
                ,count(case when is_useful = true and date(createdAt) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
                ,count(case when is_useful = true and date(createdAt) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
                ,count(case when is_useful = true and date(createdAt) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
              ) as answers_useful_cnt

            FROM `lawtalk-bigquery.lawyer_management.answers_x`
            where question_origin not in ('naver-kin')
            group by 1
            '''
    )

    callevents_x = BigQueryExecuteQueryOperator(
        task_id='callevents_x',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.callevents_x',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            -- admin상에서 callevent 잡을 때 utc를 한국 시간으로 변환하지 않고 그대로 집계하고 있음.
            SELECT
              _id
              ,type
              ,caller
              ,lawyer
              ,advice
              ,createdAt
              ,startedAt
              ,updatedAt
              ,endedAt
              -- ,DATETIME(createdAt, 'Asia/Seoul') as createdAt
              -- ,DATETIME(startedAt, 'Asia/Seoul') as startedAt
              -- ,DATETIME(updatedAt, 'Asia/Seoul') as updatedAt
              -- ,DATETIME(endedAt, 'Asia/Seoul') as endedAt
              ,duration
            FROM `lawtalk-bigquery.raw.callevents`
            --where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            where date(createdAt) <= '{{ ds }}'
            '''
    )

    lawyers_050_call_cnt_history = BigQueryExecuteQueryOperator(
        task_id='lawyers_050_call_cnt_history',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.lawyers_050_call_cnt_history',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            select
              lawyer

              ,min(date(startedAt)) as first_call_date
              ,max(date(startedAt)) as last_call_date
              ,date_diff('{{ ds }}', min(date(startedAt)), day) as days_since_first_call_date
              ,date_diff('{{ ds }}', max(date(startedAt)), day) as days_since_last_call_date

              ,struct
              (
                count(_id) as lifetime
                ,count(case when date(startedAt) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
                ,count(case when date(startedAt) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
                ,count(case when date(startedAt) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
                ,count(case when date(startedAt) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
              ) as call_cnt

            from `lawtalk-bigquery.lawyer_management.callevents_x`
            where 1=1
              and type = 'profile'
              and advice is null
            group by 1
            '''
    )

    adviceschedules_x = BigQueryExecuteQueryOperator(
      task_id='adviceschedules_x',
      use_legacy_sql = False,
      # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
      destination_dataset_table='lawtalk-bigquery.lawyer_management.adviceschedules_x',
      write_disposition = 'WRITE_TRUNCATE',
      sql='''
          SELECT
            _id,
            DATETIME(createdAt, 'Asia/Seoul') as createdAt,
            DATETIME(updatedAt, 'Asia/Seoul') as updatedAt,
            lawyer,
            number,
            parse_date('%F',dayString) as dayString,
            times,
            coalesce(array_length(split(regexp_extract(times, r"'phone': \[([0-9, ]+)\]"),',')),0) as phone_advice_slot_cnt,
            coalesce(array_length(split(regexp_extract(times, r"'visiting': \[([0-9, ]+)\]"),',')),0) as visiting_advice_slot_cnt,
            coalesce(array_length(split(regexp_extract(times, r"'video': \[([0-9, ]+)\]"),',')),0) as video_advice_slot_cnt,
            regexp_extract(times, r"'phone': \[([0-9, ]+)\]") as phone_times,
            regexp_extract(times, r"'visiting': \[([0-9, ]+)\]") as visiting_times,
            regexp_extract(times, r"'video': \[([0-9, ]+)\]") as video_phone_times,
          FROM `lawtalk-bigquery.raw.adviceschedules`
          where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
          '''
    )


    advice_x = BigQueryExecuteQueryOperator(
        task_id='advice_x',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.advice_x',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            with advice as
            (
              SELECT
                _id
                ,DATETIME(actionTime, 'Asia/Seoul') as actionTime
                ,DATETIME(createdAt, 'Asia/Seoul') as createdAt
                ,DATETIME(updatedAt, 'Asia/Seoul') as updatedAt
                ,user
                ,lawyer
                ,case when kind is null then 'phone' else kind end as kind
                ,dayString
                ,time
                ,reservedStatus
                ,status
                ,cancelInfo
                ,regexp_extract(cancelInfo, r"'adviceCancelCode\'\: ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as adviceCancelCode
                ,regexp_extract(cancelInfo, r"'reason': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as cancel_reason
                ,adCategory
                ,detailCategory
                ,survey
                ,regexp_extract(survey, r"'surveyResult': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as surveyResult_id
                ,review
                ,regexp_contains(review, r"'date': datetime.datetime\(") as is_reviewed
                ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M, %S', regexp_extract(review, r"'date': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as review_datetime
                ,regexp_extract(review, r"'rate': ([0-9]+)") as review_rate
                ,regexp_extract(review, r"'recommendation': ([a-zA-Z]+)") as review_recommendation
                --,regexp_extract(review, r"'title': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~\\]+)'") as review_title
                --,regexp_extract(review, r"'body': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~\\]+)'") as review_body
              FROM `lawtalk-bigquery.raw.advice` as A
              where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            )

            select
              advice._id
              ,advice.actionTime
              ,advice.createdAt
              ,advice.updatedAt
              ,advice.user
              ,advice.lawyer
              ,advice.kind
              ,advice.dayString
              ,advice.time
              ,time.actual_time
              ,advice.reservedStatus
              ,advice.status
              ,advice.cancelInfo
              ,advice.adviceCancelCode
              ,advicecancelcodes.description as cancel_description
              ,advice.cancel_reason
              ,advice.adCategory
              ,adcategories.name as adcategories_name
              ,advice.detailCategory
              ,advice.survey
              ,advice.surveyResult_id
              ,case when advice.surveyResult_id is not null then true else false end as lawyer_survey_done
              ,regexp_contains(surveyresults.surveyResult, r"'answerText': '사건 수임 희망함'") as wish_accept_case
              ,review
              ,is_reviewed
              ,review_datetime
              ,review_rate
              ,review_recommendation
              --,review_title
              --,review_body
            from advice
            left outer join `lawtalk-bigquery.raw.adcategories` as adcategories on advice.adCategory = adcategories._id
            left outer join `lawtalk-bigquery.lawyer_management.advice_time_matched_actual_time` as time on advice.time = time.advice_time
            left outer join `lawtalk-bigquery.raw.advicecancelcodes` as advicecancelcodes on advice.adviceCancelCode = advicecancelcodes._id
            left outer join `lawtalk-bigquery.raw.surveyresults` as surveyresults on advice.surveyResult_id = surveyresults._id
            '''
    )

    advice_x_with_transactions_page = BigQueryExecuteQueryOperator(
        task_id='advice_x_with_transactions_page',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            with advicetransactions_ as
            (
              SELECT
                _id,
                row_number() over(partition by advice order by updatedAt desc) as no_by_advice,
                number,
                DATETIME(createdAt, 'Asia/Seoul') as createdAt,
                DATETIME(updatedAt, 'Asia/Seoul') as updatedAt,
                DATETIME(requestedAt, 'Asia/Seoul') as requestedAt,
                advice,
                user,
                lawyer,
                seller,
                tid,
                status,
                method,
                originFee,
                canceled,
                coupon,
                metadata,
                regexp_extract(metadata, r"'page': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_page,
                regexp_extract(metadata, r"'source': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_source,
                regexp_extract(metadata, r"'element': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_element,
                regexp_extract(metadata, r"'position': ([0-9]+),") as metadata_position,
                regexp_extract(metadata, r"'context': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_context,
                regexp_extract(metadata, r"'contextAdditional': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_contextAdditional,
                regexp_extract(metadata, r"'extraInfo': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_extraInfo,
              FROM `lawtalk-bigquery.raw.advicetransactions`
              where 1=1
                and date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            )

            , advicetransactions_x as
            (
              select
                A.* except(no_by_advice,metadata_contextAdditional,metadata_context,metadata_extraInfo),
                coalesce(metadata_extraInfo, '') as metadata_extraInfo,
                coalesce(metadata_contextAdditional, '기타') as metadata_contextAdditional,
                coalesce(metadata_context, '기타') as metadata_context
              from advicetransactions_ as A
              where 1=1
                and no_by_advice = 1
            )

            , advicetransactions_x_with_path as
            (
              SELECT
                advicetransactions_x.*,
                case when metadata_context in ('categor','category') and REGEXP_CONTAINS(metadata_contextAdditional, r"건설/부동산|상속|성범죄|이혼|재산범죄|형사기타|기업일반|국민참여재판") then '인기분야광고'
                  when metadata_context in ('categor','category') then '일반분야광고'
                  when metadata_context = 'keyword' then '키워드광고'
                  when metadata_context = 'location' then '지역광고'
                  when metadata_context = 'plus' then '플러스광고'
                  when metadata_context = 'sentence' then '형량예측광고'
                  when metadata_context = 'sentence_a' then '형량예측광고'
                  when metadata_context = 'sentence_b' then '형량예측광고'
                  when metadata_context = 'sentence_c' then '형량예측광고'
                  when metadata_context = 'normal' then '기타'
                  when metadata_context = '기타' then '기타'
                  else '기타' end ad_name_1,
              FROM advicetransactions_x
            )

            , advicetransactions_path as
            (
              select
                advice
                ,ad_name_1
                ,metadata_contextAdditional as advicetransactions_contextAdditional
                ,metadata_extraInfo as extraInfo
              from advicetransactions_x_with_path
            )

            -- select * from advicetransactions_path

            select
              A.*,
              coalesce(B.ad_name_1,'기타') as ad_name_1,
              coalesce(B.advicetransactions_contextAdditional,'기타') as advicetransactions_contextAdditional,
              extraInfo
            from `lawtalk-bigquery.lawyer_management.advice_x` as A
            left outer join advicetransactions_path as B on A._id = B.advice
            '''
    )


    advice_x_with_transactions_page_and_matched_adorders_info = BigQueryExecuteQueryOperator(
        task_id='advice_x_with_transactions_page_and_matched_adorders_info',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page_and_matched_adorders_info',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            -- 광고가 aditem은 같지만 start end date가 충첩되는게 있을 수가 있다!!
            with x as
            (
              SELECT
                A._id as advice_id,
                A.actionTime,
                A.createdAt,
                A.updatedAt,
                A.user,
                A.lawyer,
                C.slug,
                A.kind,
                A.dayString,
                A.time,
                A.actual_time,
                A.reservedStatus,
                A.status,
                A.cancelInfo,
                A.adviceCancelCode,
                A.cancel_description,
                A.cancel_reason,
                A.adCategory,
                A.adcategories_name,
                A.detailCategory,
                A.survey,
                A.surveyResult_id,
                A.lawyer_survey_done,
                A.wish_accept_case,
                A.ad_name_1,
                A.advicetransactions_contextAdditional,
                A.extraInfo,
                A.review,
                A.is_reviewed,
                A.review_datetime,
                A.review_rate,
                A.review_recommendation,
                --A.review_title,
                --A.review_body
                case when B.lawyers_id is not null then true else false end as is_happend_by_ad_page
              FROM `lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page` as A
              left outer join
              (
                select
                  lawyers_id
                  ,adorders_start_date
                  ,adorders_end_date
                  ,ad_name_1
                  ,case when ad_name_1 in ('다이렉트 검색','키워드광고','프리미엄 검색') then REGEXP_EXTRACT(ad_name_2, r"([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\_")
                        else ad_name_2 end as ad_detail
                from `lawtalk-bigquery.lawyer_management.adorders_active_with_ad_kind`
                group by 1,2,3,4,5
              ) as B on A.lawyer=B.lawyers_id
                      and date(A.createdAt) between B.adorders_start_date and B.adorders_end_date
                      and A.ad_name_1 = B.ad_name_1
                      and regexp_contains(A.advicetransactions_contextAdditional, B.ad_detail)
              left outer join `lawtalk-bigquery.lawyer_management.lawyer_mgmt_segment` as C on A.lawyer=C.lawyers_id
              group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33
            )

            -- select
            --   count(advice_id)
            --   ,count(distinct advice_id)
            -- from x

            select
              *
            from x
            '''
    )

    post_x = BigQueryExecuteQueryOperator(
        task_id='post_x',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.post_x',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            with posts_x as
            (
              SELECT
                *,
                regexp_extract_all(categories, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as categories_arry,
                regexp_extract(case_, r"'result': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~→]+)'") as case_result,
              FROM `lawtalk-bigquery.raw.posts`
              where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{ ds }}'
            )


            select
              _id,
              DATETIME(createdAt, 'Asia/Seoul') as createdAt,
              DATETIME(createdAt, 'Asia/Seoul') as updatedAt,
              lawyer,
              slug,
              number,
              title,
              hits,
              type,
              case_,
              case_result,
              array_to_string
              (
                array
                (
                  select
                    name
                  from unnest(categories_arry) as adcategories_id
                  join `lawtalk-bigquery.raw.adcategories` on adcategories_id = _id
                ),','
              ) as adcategories_name,
              isPublished,
              isLawyerPick,
              pickRank
            from posts_x
            '''
    )


    adproduct_key_performance_category = BigQueryExecuteQueryOperator(
        task_id='adproduct_key_performance_category',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adproduct_key_performance_category',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            with raw_adcategory as
            (
              SELECT
                A.advice_id,
                A.createdAt,
                A.user,
                A.lawyer,
                coalesce(A.slug,"") as slug,
                A.kind,
                A.dayString,
                A.actual_time,
                A.reservedStatus,
                A.status,
                A.ad_name_1,
                A.advicetransactions_contextAdditional,
                B.*
              FROM `lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page_and_matched_adorders_info` as A
              left outer join `lawtalk-bigquery.lawyer_management.dim_ad_period_split` as B on date(A.createdAt) between parse_date('%F', B.start_date) and parse_date('%F', B.end_date)
              where 1=1
                and regexp_contains(A.ad_name_1, r"분야광고")
                and A.advicetransactions_contextAdditional != '기타'
                and A.status = 'complete'
            )

            , calulating_an_advice_divided_by_context_cnt as
            (
              select
                * except (context)
                ,1/count(advicetransactions_contextAdditional_split) over(partition by advice_id) as an_advice_divided_by_context_cnt
              from
              (
                select
                  *
                  ,split(advicetransactions_contextAdditional, ',') as context
                from raw_adcategory
              ), unnest(context) as advicetransactions_contextAdditional_split
            )

            , stat as
            (
              select
                start_date,
                category,
                advice_cnt,
                sum(advice_cnt) over(partition by start_date order by advice_cnt desc) as advice_cnt_accumulated_desc_by_startdate,
                sum(advice_cnt) over(partition by start_date) as advice_cnt_total_by_startdate,
                sum(advice_cnt) over(partition by start_date order by advice_cnt desc)/sum(advice_cnt) over(partition by start_date) as advice_cnt_accumulated_ratio_by_startdate,
                advice_active_client_cnt,
                advice_active_lawyer_cnt,
                advice_cnt_per_active_lawyer,
         --     min(advice_cnt) over(partition by start_date) as min_advice_cnt_by_category_in_startdate,
         --     max(advice_cnt) over(partition by start_date) as max_advice_cnt_by_category_in_startdate,
                coalesce(SAFE_DIVIDE((advice_cnt - min(advice_cnt) over(partition by start_date)),(max(advice_cnt) over(partition by start_date) - min(advice_cnt) over(partition by start_date))),1) as advice_cnt_normalize,
                coalesce(SAFE_DIVIDE((advice_active_client_cnt - min(advice_active_client_cnt) over(partition by start_date)),(max(advice_active_client_cnt) over(partition by start_date) - min(advice_active_client_cnt) over(partition by start_date))),1) as advice_active_client_cnt_normalize,
                coalesce(SAFE_DIVIDE((advice_active_lawyer_cnt - min(advice_active_lawyer_cnt) over(partition by start_date)),(max(advice_active_lawyer_cnt) over(partition by start_date) - min(advice_active_lawyer_cnt) over(partition by start_date))),1) as advice_active_lawyer_cnt_normalize,
                coalesce(SAFE_DIVIDE((advice_cnt_per_active_lawyer - min(advice_cnt_per_active_lawyer) over(partition by start_date)),(max(advice_cnt_per_active_lawyer) over(partition by start_date) - min(advice_cnt_per_active_lawyer) over(partition by start_date))),1) as advice_cnt_per_active_lawyer_normalize,
              from
              (
                select
                  start_date,
                  advicetransactions_contextAdditional_split as category,
                  sum(an_advice_divided_by_context_cnt) as advice_cnt,
                  count(distinct user) as advice_active_client_cnt,
                  count(distinct slug) as advice_active_lawyer_cnt,
                  sum(an_advice_divided_by_context_cnt)/count(distinct slug) as advice_cnt_per_active_lawyer
                from calulating_an_advice_divided_by_context_cnt
                group by 1,2
              )
            )

            , stat_gini as
            (
              select
                start_date,
                category,
                1-(sum(advice_cnt_ratio_accum*(1/(lawyer_cnt_total-1)))/0.5) as gini_coefficient
              from
              (
                select
                  *,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc) as lawyer_cnt_accum,
                  count(lawyer) over(partition by start_date, category) as lawyer_cnt_total,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc)/count(lawyer) over(partition by start_date,  category) as lawyer_cnt_ratio_accum,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc) as advice_cnt_accum,
                  sum(advice_cnt) over(partition by start_date, category) as advice_cnt_total,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc)/  sum(advice_cnt) over(partition by start_date,  category) as advice_cnt_ratio_accum
                from
                (
                  select
                    start_date,
                    advicetransactions_contextAdditional_split as category,
                    slug as lawyer,
                    sum(an_advice_divided_by_context_cnt) as advice_cnt
                  from calulating_an_advice_divided_by_context_cnt
                  group by 1,2,3
                )
              )
              where lawyer_cnt_total-lawyer_cnt_accum > 0
              group by 1,2
            )


            select
              A.*,
              B.gini_coefficient
            from stat as A
            left outer join stat_gini as B on A.start_date = B.start_date and A.category = B.category
            order by 1 desc, 3 desc
            '''
    )

    adproduct_key_performance_area = BigQueryExecuteQueryOperator(
        task_id='adproduct_key_performance_area',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adproduct_key_performance_area',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            with raw_area as
            (
              SELECT
                A.advice_id,
                A.createdAt,
                A.user,
                A.lawyer,
                coalesce(A.slug,"") as slug,
                A.kind,
                A.dayString,
                A.actual_time,
                A.reservedStatus,
                A.status,
                A.ad_name_1,
                A.advicetransactions_contextAdditional,
                B.*
              FROM `lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page_and_matched_adorders_info` as A
              left outer join `lawtalk-bigquery.lawyer_management.dim_ad_period_split` as B on date(A.createdAt) between parse_date('%F', B.start_date) and parse_date('%F', B.end_date)
              where 1=1
                and regexp_contains(A.ad_name_1, r"지역광고")
                and A.advicetransactions_contextAdditional != '기타'
                and A.status = 'complete'
            )

            , calulating_an_advice_divided_by_context_cnt as
            (
              select
                * except (context)
                ,1/count(advicetransactions_contextAdditional_split) over(partition by advice_id) as an_advice_divided_by_context_cnt
              from
              (
                select
                  *
                  ,split(advicetransactions_contextAdditional, ',') as context
                from raw_area
              ), unnest(context) as advicetransactions_contextAdditional_split
            )

            , stat as
            (
              select
                start_date,
                category,
                advice_cnt,
                sum(advice_cnt) over(partition by start_date order by advice_cnt desc) as advice_cnt_accumulated_desc_by_startdate,
                sum(advice_cnt) over(partition by start_date) as advice_cnt_total_by_startdate,
                sum(advice_cnt) over(partition by start_date order by advice_cnt desc)/sum(advice_cnt) over(partition by start_date) as advice_cnt_accumulated_ratio_by_startdate,
                advice_active_client_cnt,
                advice_active_lawyer_cnt,
                advice_cnt_per_active_lawyer,
         --     min(advice_cnt) over(partition by start_date) as min_advice_cnt_by_category_in_startdate,
         --     max(advice_cnt) over(partition by start_date) as max_advice_cnt_by_category_in_startdate,
                coalesce(SAFE_DIVIDE((advice_cnt - min(advice_cnt) over(partition by start_date)),(max(advice_cnt) over(partition by start_date) - min(advice_cnt) over(partition by start_date))),1) as advice_cnt_normalize,
                coalesce(SAFE_DIVIDE((advice_active_client_cnt - min(advice_active_client_cnt) over(partition by start_date)),(max(advice_active_client_cnt) over(partition by start_date) - min(advice_active_client_cnt) over(partition by start_date))),1) as advice_active_client_cnt_normalize,
                coalesce(SAFE_DIVIDE((advice_active_lawyer_cnt - min(advice_active_lawyer_cnt) over(partition by start_date)),(max(advice_active_lawyer_cnt) over(partition by start_date) - min(advice_active_lawyer_cnt) over(partition by start_date))),1) as advice_active_lawyer_cnt_normalize,
                coalesce(SAFE_DIVIDE((advice_cnt_per_active_lawyer - min(advice_cnt_per_active_lawyer) over(partition by start_date)),(max(advice_cnt_per_active_lawyer) over(partition by start_date) - min(advice_cnt_per_active_lawyer) over(partition by start_date))),1) as advice_cnt_per_active_lawyer_normalize,
              from
              (
                select
                  start_date,
                  advicetransactions_contextAdditional_split as category,
                  sum(an_advice_divided_by_context_cnt) as advice_cnt,
                  count(distinct user) as advice_active_client_cnt,
                  count(distinct slug) as advice_active_lawyer_cnt,
                  sum(an_advice_divided_by_context_cnt)/count(distinct slug) as advice_cnt_per_active_lawyer
                from calulating_an_advice_divided_by_context_cnt
                group by 1,2
              )
            )

            , stat_gini as
            (
              select
                start_date,
                category,
                1-(sum(advice_cnt_ratio_accum*(1/(lawyer_cnt_total-1)))/0.5) as gini_coefficient
              from
              (
                select
                  *,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc) as lawyer_cnt_accum,
                  count(lawyer) over(partition by start_date, category) as lawyer_cnt_total,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc)/count(lawyer) over(partition by start_date,  category) as lawyer_cnt_ratio_accum,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc) as advice_cnt_accum,
                  sum(advice_cnt) over(partition by start_date, category) as advice_cnt_total,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc)/  sum(advice_cnt) over(partition by start_date,  category) as advice_cnt_ratio_accum
                from
                (
                  select
                    start_date,
                    advicetransactions_contextAdditional_split as category,
                    slug as lawyer,
                    sum(an_advice_divided_by_context_cnt) as advice_cnt
                  from calulating_an_advice_divided_by_context_cnt
                  group by 1,2,3
                )
              )
              where lawyer_cnt_total-lawyer_cnt_accum > 0
              group by 1,2
            )


            select
              A.*,
              B.gini_coefficient
            from stat as A
            left outer join stat_gini as B on A.start_date = B.start_date and A.category = B.category
            order by 1 desc, 3 desc
            '''
    )

    adproduct_key_performance_union = BigQueryExecuteQueryOperator(
        task_id='adproduct_key_performance_union',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adproduct_key_performance_union',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            SELECT
              '분야광고' as adproduct,
              *
            FROM `lawtalk-bigquery.lawyer_management.adproduct_key_performance_category`
            union all
            select
              '지역광고' as adproduct,
              *
            FROM `lawtalk-bigquery.lawyer_management.adproduct_key_performance_area`
            '''
    )

    adproduct_lorenzcurve_category = BigQueryExecuteQueryOperator(
        task_id='adproduct_lorenzcurve_category',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adproduct_lorenzcurve_category',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            with raw_adcategory as
            (
              SELECT
                A.advice_id,
                A.createdAt,
                A.user,
                A.lawyer,
                coalesce(A.slug,"") as slug,
                A.kind,
                A.dayString,
                A.actual_time,
                A.reservedStatus,
                A.status,
                A.ad_name_1,
                A.advicetransactions_contextAdditional,
                B.*
              FROM `lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page_and_matched_adorders_info` as A
              left outer join `lawtalk-bigquery.lawyer_management.dim_ad_period_split` as B on date(A.createdAt) between parse_date('%F', B.start_date) and parse_date('%F', B.end_date)
              where 1=1
                and regexp_contains(A.ad_name_1, r"분야광고")
                and A.advicetransactions_contextAdditional != '기타'
                and A.status = 'complete'
            )

            , calulating_an_advice_divided_by_context_cnt as
            (
              select
                * except (context)
                ,1/count(advicetransactions_contextAdditional_split) over(partition by advice_id) as an_advice_divided_by_context_cnt
              from
              (
                select
                  *
                  ,split(advicetransactions_contextAdditional, ',') as context
                from raw_adcategory
              ), unnest(context) as advicetransactions_contextAdditional_split
            )



            select
              *,
              row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc) as lawyer_cnt_accum,
              count(lawyer) over(partition by start_date, category) as lawyer_cnt_total,
              row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc)/count(lawyer) over(partition by start_date,  category) as lawyer_cnt_ratio_accum,
              sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc) as advice_cnt_accum,
              sum(advice_cnt) over(partition by start_date, category) as advice_cnt_total,
              advice_cnt/sum(advice_cnt) over(partition by start_date, category) as advice_cnt_ratio,
              sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc)/  sum(advice_cnt) over(partition by start_date,  category) as advice_cnt_ratio_accum
            from
            (
              select
                start_date,
                advicetransactions_contextAdditional_split as category,
                slug as lawyer,
                sum(an_advice_divided_by_context_cnt) as advice_cnt
              from calulating_an_advice_divided_by_context_cnt
              group by 1,2,3
            )
            '''
    )

    adproduct_lorenzcurve_area = BigQueryExecuteQueryOperator(
        task_id='adproduct_lorenzcurve_area',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adproduct_lorenzcurve_area',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            with raw_area as
            (
              SELECT
                A.advice_id,
                A.createdAt,
                A.user,
                A.lawyer,
                coalesce(A.slug,"") as slug,
                A.kind,
                A.dayString,
                A.actual_time,
                A.reservedStatus,
                A.status,
                A.ad_name_1,
                A.advicetransactions_contextAdditional,
                B.*
              FROM `lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page_and_matched_adorders_info` as A
              left outer join `lawtalk-bigquery.lawyer_management.dim_ad_period_split` as B on date(A.createdAt) between parse_date('%F', B.start_date) and parse_date('%F', B.end_date)
              where 1=1
                and regexp_contains(A.ad_name_1, r"지역광고")
                and A.advicetransactions_contextAdditional != '기타'
                and A.status = 'complete'
            )

            , calulating_an_advice_divided_by_context_cnt as
            (
              select
                * except (context)
                ,1/count(advicetransactions_contextAdditional_split) over(partition by advice_id) as an_advice_divided_by_context_cnt
              from
              (
                select
                  *
                  ,split(advicetransactions_contextAdditional, ',') as context
                from raw_area
              ), unnest(context) as advicetransactions_contextAdditional_split
            )

            select
              *,
              row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc) as lawyer_cnt_accum,
              count(lawyer) over(partition by start_date, category) as lawyer_cnt_total,
              row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc)/count(lawyer) over(partition by start_date,  category) as lawyer_cnt_ratio_accum,
              sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc) as advice_cnt_accum,
              sum(advice_cnt) over(partition by start_date, category) as advice_cnt_total,
              advice_cnt/sum(advice_cnt) over(partition by start_date, category) as advice_cnt_ratio,
              sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc)/  sum(advice_cnt) over(partition by start_date,  category) as advice_cnt_ratio_accum
            from
            (
              select
                start_date,
                advicetransactions_contextAdditional_split as category,
                slug as lawyer,
                sum(an_advice_divided_by_context_cnt) as advice_cnt
              from calulating_an_advice_divided_by_context_cnt
              group by 1,2,3
            )
            '''
    )

    adproduct_lorenzcurve_union = BigQueryExecuteQueryOperator(
        task_id='adproduct_lorenzcurve_union',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.adproduct_lorenzcurve_union',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            SELECT
              '분야광고' as adproduct,
              *
            FROM `lawtalk-bigquery.lawyer_management.adproduct_lorenzcurve_category`
            union all
            select
              '지역광고' as adproduct,
              *
            FROM `lawtalk-bigquery.lawyer_management.adproduct_lorenzcurve_area`
            '''
    )


    category_with_matched_lawyer_profile_main_field = BigQueryExecuteQueryOperator(
        task_id='category_with_matched_lawyer_profile_main_field',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.category_with_matched_lawyer_profile_main_field',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            #standardSQL
            select
              _id as adcategories_id,
              category_name,
              lawyers_id
            from
            (
              select
                _id,
                name as category_name
              from `lawtalk-bigquery.raw.adcategories`
            ) as A
            left outer join
            (
              select
                lawyers_id,
                mainFields_category_id
              from
              (
                SELECT
                  _id as lawyers_id,
                  mainFields,
                  regexp_extract_all(mainFields, r"'category': ObjectId\(\'([a-zA-Z0-9]+)\'\)\}") as mainFields_category_id_array
                FROM `lawtalk-bigquery.raw.lawyers`
                where 1=1
              ), unnest(mainFields_category_id_array) as mainFields_category_id
            ) as B on A._id = mainFields_category_id
            '''
    )

    gini_raw_by_field_based_on_advertiser = BigQueryExecuteQueryOperator(
        task_id='gini_raw_by_field_based_on_advertiser',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.gini_raw_by_field_based_on_advertiser',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            with dim_categories as
            (
              SELECT
                'adcategories' as product
                ,name as field
              FROM `lawtalk-bigquery.raw.adcategories`
            --   UNION ALL
            --   SELECT
            --     'adlocationgroups' as product
            --     ,name as field
            --   FROM `lawtalk-bigquery.raw.adlocationgroups`
            --   UNION ALL
            --   SELECT
            --     '기타' as product
            --     ,'기타' as field
            )

            , dim_categories_with_ad_period as
            (
              select
                *
              from dim_categories
              cross join `lawtalk-bigquery.lawyer_management.dim_ad_period_split`
            )

            -- select
            --   *
            -- from dim_categories_with_ad_period

            , advicetransactions_cnt as
            (
              select
                ad_start_date,
                ad_end_date,
                advicetransactions_contextAdditional,
                lawyer,
                sum(an_advice_divided_by_context_cnt) as advicetransactions_cnt
              from
              (
                select
                  * except (context)
                  ,1/count(advicetransactions_contextAdditional_split) over(partition by advice_id) as an_advice_divided_by_context_cnt
                from
                (
                  select
                    *
                    ,split(advicetransactions_contextAdditional, ',') as context
                  from
                  (
                    SELECT
                      advice_id,
                      createdAt,
                      user,
                      lawyer,
                      coalesce(slug,"") as slug,
                      kind,
                      dayString,
                      actual_time,
                      reservedStatus,
                      status,
                      ad_name_1,
                      advicetransactions_contextAdditional,
                      B.start_date as ad_start_date,
                      B.end_date as ad_end_date
                    FROM `lawtalk-bigquery.lawyer_management.advice_x_with_transactions_page_and_matched_adorders_info` as A
                    left outer join `lawtalk-bigquery.lawyer_management.dim_ad_period_split` as B on parse_date('%F', A.dayString) between parse_date('%F', B.start_date) and parse_date('%F', B.end_date)
                    where status in ('complete','reservation')
                  )
                ), unnest(context) as advicetransactions_contextAdditional_split
              )
              group by 1,2,3,4
            )

            , adorders_active_with_ad_kind as
            (
                select
                  *
                from `lawtalk-bigquery.lawyer_management.adorders_active_with_ad_kind` as adorders_active_with_ad_kind
                left outer join `lawtalk-bigquery.raw.lawyers` as lawyers on adorders_active_with_ad_kind.lawyers_id = lawyers._id
                where 1=1
                  and ad_name_1 in ('인기분야광고','일반분야광고') --,'지역광고')
                  and regexp_contains(slug, r"탈퇴") is False
            )

            , lawyer_advice_slot_cnt as
            (
              SELECT
                A.lawyer,
                B.start_date,
                B.end_date,
                sum(A.phone_advice_slot_cnt) as phone_advice_slot_cnt,
                sum(A.visiting_advice_slot_cnt)as visiting_advice_slot_cnt,
                sum(A.video_advice_slot_cnt) as video_advice_slot_cnt,
                sum(A.phone_advice_slot_cnt) + sum(A.visiting_advice_slot_cnt) + sum(A.video_advice_slot_cnt) as all_advice_slot_cnt
              FROM `lawtalk-bigquery.lawyer_management.adviceschedules_x` as A
              left outer join `lawtalk-bigquery.lawyer_management.dim_ad_period_split` as B on A.dayString between parse_date('%F', B.start_date) and parse_date('%F', B.end_date)
              group by 1,2,3
              order by 1,2,3
            )

            , merge_table as
            (
              select
                dim_categories_with_ad_period.*
                ,adorders_active_with_ad_kind.lawyers_id
                ,adorders_active_with_ad_kind.slug
                ,coalesce(advicetransactions_cnt.advicetransactions_cnt,0) as advicetransactions_cnt
                ,coalesce(lawyer_advice_slot_cnt.phone_advice_slot_cnt, 0) as phone_advice_slot_cnt
                ,coalesce(lawyer_advice_slot_cnt.visiting_advice_slot_cnt, 0) as visiting_advice_slot_cnt
                ,coalesce(lawyer_advice_slot_cnt.video_advice_slot_cnt, 0) as video_advice_slot_cnt
                ,coalesce(lawyer_advice_slot_cnt.all_advice_slot_cnt, 0) as all_advice_slot_cnt
            from dim_categories_with_ad_period
              left outer join adorders_active_with_ad_kind on parse_date('%F', dim_categories_with_ad_period.start_date) >= adorders_active_with_ad_kind.adorders_start_date
                  and parse_date('%F', dim_categories_with_ad_period.end_date) <= adorders_active_with_ad_kind.adorders_end_date
                  and dim_categories_with_ad_period.field = adorders_active_with_ad_kind.ad_name_2
              left outer join lawyer_advice_slot_cnt
                on adorders_active_with_ad_kind.lawyers_id = lawyer_advice_slot_cnt.lawyer
                  and dim_categories_with_ad_period.start_date = lawyer_advice_slot_cnt.start_date
                  and dim_categories_with_ad_period.end_date = lawyer_advice_slot_cnt.end_date
              left outer join advicetransactions_cnt
                on dim_categories_with_ad_period.start_date = advicetransactions_cnt.ad_start_date
                  and dim_categories_with_ad_period.end_date = advicetransactions_cnt.ad_end_date
                  and dim_categories_with_ad_period.field = advicetransactions_cnt.advicetransactions_contextAdditional
                  and adorders_active_with_ad_kind.lawyers_id = advicetransactions_cnt.lawyer
              where 1=1
            )

            -- select #75개 맞음
            --   field
            -- from merge_table
            -- group by 1

            -- select
            --   *
            -- from merge_table
            -- where 1=1
            --   and start_date = '2022-04-01'
            --   and field = '성범죄'
            -- order by start_date, product, field, advicetransactions_cnt desc

            , stat as
            (
              select
                *
                ,count(slug) over(partition by field, start_date) as advertiser_cnt_by_field
                ,sum(advicetransactions_cnt) over(partition by field, start_date) as advicetransactions_cnt_by_field
                ,count(case when all_advice_slot_cnt > 0 then slug else null end) over(partition by field, start_date) as advertiser_opened_slot_cnt_by_field
                ,coalesce(sum(case when all_advice_slot_cnt > 0 then advicetransactions_cnt else null end) over(partition by field, start_date),0) as advicetransactions_cnt_by_field_based_on_advertiser_opened_slot
              from merge_table
            --   where all_advice_slot_cnt > 0
              order by start_date, product, field, advicetransactions_cnt desc
            )


            select
              *
            from stat
            where 1=1
            --   and start_date = '2021-09-01'
            --   and field = '상속'
            --   and all_advice_slot_cnt = 0 and advicetransactions_cnt > 0
            '''
    )

    gini_by_field_based_on_advertiser = BigQueryExecuteQueryOperator(
        task_id='gini_by_field_based_on_advertiser',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.gini_by_field_based_on_advertiser',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            with dim_categories as
            (
              SELECT
                'adcategories' as product
                ,name as field
              FROM `lawtalk-bigquery.raw.adcategories`
            --   UNION ALL
            --   SELECT
            --     'adlocationgroups' as product
            --     ,name as field
            --   FROM `lawtalk-bigquery.raw.adlocationgroups`
            --   UNION ALL
            --   SELECT
            --     '기타' as product
            --     ,'기타' as field
            )

            , dim_categories_with_ad_period as
            (
              select
                *
              from dim_categories
              cross join `lawtalk-bigquery.lawyer_management.dim_ad_period_split`
            )


            , stat_advertiser_advicetransaction_cnt_by_field as
            (
              select
                field,
                start_date,
                count(slug) as advertiser_cnt,
                sum(advicetransactions_cnt) as advicetransactions_cnt
              from `lawtalk-bigquery.lawyer_management.gini_raw_by_field_based_on_advertiser`
              group by 1,2
            )

            , stat_gini as
            (
              select
                start_date,
                category,
                1-(sum(advice_cnt_ratio_accum*(1/(lawyer_cnt_total-1)))/0.5) as gini_coefficient
              from
              (
                select
                  *,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc) as lawyer_cnt_accum,
                  count(lawyer) over(partition by start_date, category) as lawyer_cnt_total,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc)/count(lawyer) over(partition by start_date,  category) as lawyer_cnt_ratio_accum,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc) as advice_cnt_accum,
                  sum(advice_cnt) over(partition by start_date, category) as advice_cnt_total,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc)/  sum(advice_cnt) over(partition by start_date,  category) as advice_cnt_ratio_accum
                from
                (
                  select
                    start_date,
                    field as category,
                    slug as lawyer,
                    advicetransactions_cnt as advice_cnt
                  from `lawtalk-bigquery.lawyer_management.gini_raw_by_field_based_on_advertiser`
                  where advicetransactions_cnt_by_field > 0
                )
              )
              where lawyer_cnt_total-lawyer_cnt_accum > 0
              group by 1,2
            )


            -- select
            --   *
            -- from stat_gini


            select
              dim_categories_with_ad_period.*
              ,stat_gini.gini_coefficient
              ,coalesce(stat_advertiser_advicetransaction_cnt_by_field.advertiser_cnt,0) as advertiser_cnt
              ,coalesce(stat_advertiser_advicetransaction_cnt_by_field.advicetransactions_cnt,0) as advicetransactions_cnt
            from
            (
              select
                product,
                field,
                start_date,
                end_date
              from dim_categories_with_ad_period
            ) as dim_categories_with_ad_period
            left outer join stat_gini on dim_categories_with_ad_period.field = stat_gini.category and dim_categories_with_ad_period.start_date = stat_gini.start_date
            left outer join stat_advertiser_advicetransaction_cnt_by_field on dim_categories_with_ad_period.field = stat_advertiser_advicetransaction_cnt_by_field.field and dim_categories_with_ad_period.start_date = stat_advertiser_advicetransaction_cnt_by_field.start_date
            where 1=1
            -- and dim_categories_with_ad_period.start_date = '2022-02-01'
            -- order by field, start_date
            order by gini_coefficient desc
            '''
    )


    gini_by_field_based_on_advertiser_opened_slot = BigQueryExecuteQueryOperator(
        task_id='gini_by_field_based_on_advertiser_opened_slot',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.gini_by_field_based_on_advertiser_opened_slot',
        write_disposition = 'WRITE_TRUNCATE',
        sql='''
            with dim_categories as
            (
              SELECT
                'adcategories' as product
                ,name as field
              FROM `lawtalk-bigquery.raw.adcategories`
            --   UNION ALL
            --   SELECT
            --     'adlocationgroups' as product
            --     ,name as field
            --   FROM `lawtalk-bigquery.raw.adlocationgroups`
            --   UNION ALL
            --   SELECT
            --     '기타' as product
            --     ,'기타' as field
            )

            , dim_categories_with_ad_period as
            (
              select
                *
              from dim_categories
              cross join `lawtalk-bigquery.lawyer_management.dim_ad_period_split`
            )


            , stat_advertiser_advicetransaction_cnt_by_field as
            (
              select
                field,
                start_date,
                count(slug) as advertiser_cnt,
                sum(advicetransactions_cnt) as advicetransactions_cnt
              from `lawtalk-bigquery.lawyer_management.gini_raw_by_field_based_on_advertiser`
              where all_advice_slot_cnt > 0
              group by 1,2
            )

            , stat_gini as
            (
              select
                start_date,
                category,
                1-(sum(advice_cnt_ratio_accum*(1/(lawyer_cnt_total-1)))/0.5) as gini_coefficient
              from
              (
                select
                  *,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc) as lawyer_cnt_accum,
                  count(lawyer) over(partition by start_date, category) as lawyer_cnt_total,
                  row_number() over(partition by start_date, category order by advice_cnt asc, lawyer asc)/count(lawyer) over(partition by start_date,  category) as lawyer_cnt_ratio_accum,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc) as advice_cnt_accum,
                  sum(advice_cnt) over(partition by start_date, category) as advice_cnt_total,
                  sum(advice_cnt) over(partition by start_date, category order by advice_cnt asc, lawyer asc)/  sum(advice_cnt) over(partition by start_date,  category) as advice_cnt_ratio_accum
                from
                (
                  select
                    start_date,
                    field as category,
                    slug as lawyer,
                    advicetransactions_cnt as advice_cnt
                  from `lawtalk-bigquery.lawyer_management.gini_raw_by_field_based_on_advertiser`
                  where all_advice_slot_cnt > 0 and advicetransactions_cnt_by_field_based_on_advertiser_opened_slot > 0
                )
              )
              where lawyer_cnt_total-lawyer_cnt_accum > 0
              group by 1,2
            )


            -- select
            --   *
            -- from stat_gini


            select
              dim_categories_with_ad_period.*
              ,stat_gini.gini_coefficient
              ,coalesce(stat_advertiser_advicetransaction_cnt_by_field.advertiser_cnt,0) as advertiser_cnt
              ,coalesce(stat_advertiser_advicetransaction_cnt_by_field.advicetransactions_cnt,0) as advicetransactions_cnt
            from
            (
              select
                product,
                field,
                start_date,
                end_date
              from dim_categories_with_ad_period
            ) as dim_categories_with_ad_period
            left outer join stat_gini on dim_categories_with_ad_period.field = stat_gini.category and dim_categories_with_ad_period.start_date = stat_gini.start_date
            left outer join stat_advertiser_advicetransaction_cnt_by_field on dim_categories_with_ad_period.field = stat_advertiser_advicetransaction_cnt_by_field.field and dim_categories_with_ad_period.start_date = stat_advertiser_advicetransaction_cnt_by_field.start_date
            where 1=1
            -- and dim_categories_with_ad_period.start_date = '2022-02-01'
            -- order by field, start_date
            order by gini_coefficient desc
            '''
    )



    # advice_cnt_history_by_status = BigQueryExecuteQueryOperator(
    #     task_id='advice_cnt_history_by_status',
    #     use_legacy_sql = False,
    #     # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
    #     destination_dataset_table='lawtalk-bigquery.lawyer_management.advice_cnt_history_by_status',
    #     write_disposition = 'WRITE_TRUNCATE',
    #     sql='''
    #         #standardSQL
    #         SELECT
    #           lawyer

    #           ,min(case when status in ('complete') then parse_date('%F', dayString) else null end) as first_complete_advice_date
    #           ,max(case when status in ('complete') then parse_date('%F', dayString) else null end) as last_complete_advice_date
    #           ,date_diff('{{ ds }}', min(case when status in ('complete') then parse_date('%F', dayString) else null end), day) as days_since_first_complete_advice_date
    #           ,date_diff('{{ ds }}', max(case when status in ('complete') then parse_date('%F', dayString) else null end), day) as days_since_last_complete_advice_date


    #           ,struct
    #           (
    #             count(_id) as lifetime
    #             ,count(case when parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
    #             ,count(case when parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
    #             ,count(case when parse_date('%F', dayString)  between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
    #             ,count(case when parse_date('%F', dayString) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
    #           ) as advice_cnt_total


    #           ,struct
    #           (
    #             count(case when status = 'complete' then _id else null end) as lifetime
    #             ,count(case when status = 'complete' and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
    #             ,count(case when status = 'complete' and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
    #             ,count(case when status = 'complete' and parse_date('%F', dayString) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
    #             ,count(case when status = 'complete' and parse_date('%F', dayString) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
    #           ) as advice_cnt_complete


    #           ,struct
    #           (
    #           count(case when status = 'cancel' then _id else null end) as lifetime
    #           ,count(case when status = 'cancel' and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
    #           ,count(case when status = 'cancel' and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
    #           ,count(case when status = 'cancel' and parse_date('%F', dayString) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
    #           ,count(case when status = 'cancel' and parse_date('%F', dayString) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
    #           ) as advice_cnt_cancel

    #           ,struct
    #           (
    #           count(case when status in ('reserved','reservation') then _id else null end) as lifetime
    #           ,count(case when status in ('reserved','reservation') and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
    #           ,count(case when status in ('reserved','reservation') and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
    #           ,count(case when status in ('reserved','reservation') and parse_date('%F', dayString) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
    #           ,count(case when status in ('reserved','reservation') and parse_date('%F', dayString) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
    #           ) as advice_cnt_reservation_reserved

    #           ,struct
    #           (
    #           count(case when lawyer_survey_done is true then _id else null end) as lifetime
    #           ,count(case when lawyer_survey_done is true and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
    #           ,count(case when lawyer_survey_done is true and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
    #           ,count(case when lawyer_survey_done is true and parse_date('%F', dayString) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
    #           ,count(case when lawyer_survey_done is true and parse_date('%F', dayString) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
    #           ) as advice_cnt_survey_done

    #           ,struct
    #           (
    #           count(case when wish_accept_case is true then _id else null end) as lifetime
    #           ,count(case when wish_accept_case is true and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 182 day) then _id else null end) as last_6m
    #           ,count(case when wish_accept_case is true and parse_date('%F', dayString) >= date_sub('{{ ds }}', interval 30 day) then _id else null end) as last_30d
    #           ,count(case when wish_accept_case is true and parse_date('%F', dayString) between date_trunc(date_sub('{{ ds }}', interval 1 month), month) and date_sub(date_trunc('{{ ds }}', month), interval 1 day) then _id else null end) as last_month
    #           ,count(case when wish_accept_case is true and parse_date('%F', dayString) >= date_trunc('{{ ds }}', month) then _id else null end) as mtd
    #           ) as advice_cnt_wish_accept_case

    #         FROM `lawyer_management.advice_x_with_transactions_page`
    #         group by 1
    #         '''
    # )


    # end = dummy.DummyOperator(task_id='end')

######################################################################################################
    start
    #start>>check_bm_activities
    start>>users_x_snapshot>>users_x
    start>>lawyers_x_snapshot>>lawyers_x
    start>>adorders_history>>adorders_pausehistory>>adorders_active_daily
    start>>adorders_item_adCategory>>adorders_with_location_and_keyword_array
    start>>betaadorders_active_daily
    start>>[questions_x,questions_answers]>>answers_x>>lawyer_answers_cnt_history
    start>>callevents_x>>lawyers_050_call_cnt_history
    start>>adviceschedules_x
    start>>advice_x>>advice_x_with_transactions_page
    start>>post_x
    start>>category_with_matched_lawyer_profile_main_field


    [lawyers_x,users_x,adorders_active_daily,betaadorders_active_daily]>>lawyers_basic>>lawyers_basic_snapshot>>lawyer_mgmt_segment>>lawyer_mgmt_segment_snapshot
    [adorders_history,lawyer_mgmt_segment]>>adorders_history_with_ad_contract_categroies>>adorders_history_with_status_seg_info
    [adorders_active_daily,adorders_with_location_and_keyword_array,betaadorders_active_daily]>>adorders_active_with_ad_kind
    [adorders_active_with_ad_kind,advice_x_with_transactions_page,lawyer_mgmt_segment]>>advice_x_with_transactions_page_and_matched_adorders_info
    advice_x_with_transactions_page_and_matched_adorders_info>>[adproduct_key_performance_category,adproduct_key_performance_area]>>adproduct_key_performance_union
    advice_x_with_transactions_page_and_matched_adorders_info>>[adproduct_lorenzcurve_category,adproduct_lorenzcurve_area]>>adproduct_lorenzcurve_union
    [advice_x_with_transactions_page_and_matched_adorders_info,adorders_active_with_ad_kind]>>gini_raw_by_field_based_on_advertiser>>[gini_by_field_based_on_advertiser,gini_by_field_based_on_advertiser_opened_slot]


    # [check_bm_activities,users_x,lawyers_x,adorders_active_daily,adorders_item_adCategory,betaadorders_active_daily,lawyers_advice_cnt_history_by_status,lawyers_050_call_cnt_history,lawyer_answers_cnt_history,stat_advicetransactions_cnt_by_context_daily,stat_advicetransactions_cnt_by_context_and_lawyer_daily,lawyer_mgmt_segment]>>end
