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
    'owner': 'DAEGUN KIM',
    'email': ['dg.kim@lawcompany.co.kr'],
    'email_on_retry': True,
    'email_on_failure': True,
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=3),
    'max_active_runs' : 1,
    'max_active_tasks' : 1,
    'start_date': '2022-02-07',
}


with models.DAG(
        dag_id = 'test_lawyer_management_ga4_220207_02_DGK',
        description = 'Data Workflow for Data Mart of lawyer_management on GA4',
        schedule_interval=datetime.timedelta(days=1),
        # schedule_interval= '30 1 * * *', #한국시간기준 매일 아침 10시 30분에 스타트
        default_args=default_args) as dag:
    
    start = dummy.DummyOperator(task_id='start')

    check_events_table_lastday = BigQueryTableExistenceSensor(
        #빅쿼리 예약된 쿼리로 만들어진 테이블
        task_id = 'check_events_table_lastday',
        project_id = 'lawtalk-bigquery',
        dataset_id= 'analytics_265523655',
        table_id = 'events_{{ ds_nodash }}',
    )

    stat_daily_listviewclick_and_profileview_cnt_by_lawyer = BigQueryExecuteQueryOperator(
        task_id='stat_daily_listviewclick_and_profileview_cnt_by_lawyer',
        use_legacy_sql = False,
        # destination_dataset_table='lawtalk-bigquery.tmp_daegunkim.lawyers_adorders_history_{{ yesterday_ds_nodash }}',
        destination_dataset_table='lawtalk-bigquery.lawyer_management.stat_daily_listviewclick_and_profileview_cnt_by_lawyer',
        write_disposition = 'WRITE_APPEND',
        sql='''
            #standardSQL
            WITH impressions AS (
              SELECT 
                user_pseudo_id,
                event_name, 
                DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_date, 
                DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_datetime, 
                udfs.param_value_by_key('lawyer_id', event_params).string_value AS lawyer_slug,
              FROM `lawtalk-bigquery.analytics_265523655.events_*` 
              WHERE _table_suffix = '{{ ds_nodash }}'
              AND event_name = 'tag.search.프로필노출' #목록노출
            ),

            clicks_ad_list AS (
              SELECT 
                user_pseudo_id,
                event_name, 
                DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_date, 
                DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_datetime, 
                udfs.param_value_by_key('lawyer_id', event_params).string_value AS lawyer_slug,
              FROM `lawtalk-bigquery.analytics_265523655.events_*` 
              WHERE _table_suffix = '{{ ds_nodash }}'
              AND event_name = 'tag.search.프로필클릭' #목록클릭
            )

            ,view_profile AS (
              SELECT 
                user_pseudo_id,
                event_name, 
                DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_date, 
                DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_datetime, 
                udfs.param_value_by_key('lawyer_id', event_params).string_value AS lawyer_slug,
              FROM `lawtalk-bigquery.analytics_265523655.events_*` 
              WHERE _table_suffix = '{{ ds_nodash }}'
              AND event_name = 'tag.프로필페이지진입(page_view)' #프로필페이지진입
            )

            ,raw as
            (
              SELECT * 
              FROM impressions
              UNION ALL
              SELECT * 
              FROM clicks_ad_list
              UNION ALL
              SELECT * 
              FROM view_profile
            )

            ,stat as
            (
                select
                  event_date,
                  lawyer_slug,
                  case when event_name='tag.search.프로필노출' then '목록노출'
                       when event_name='tag.search.프로필클릭' then '목록클릭'
                       when event_name='tag.프로필페이지진입(page_view)' then '프로필조회'
                       else null end as event_name,
                  COUNT(user_pseudo_id) AS event_cnt,
                  COUNT(DISTINCT user_pseudo_id) AS event_user_cnt
                from raw
                group by 1,2,3
                order by 1,2,3
            )

            #pivoting
            select
                event_date,
                lawyer_slug,
                sum(case when event_name = '목록노출' then event_cnt else 0 end) as list_view_cnt,
                sum(case when event_name = '목록클릭' then event_cnt else 0 end) as list_click_cnt,
                sum(case when event_name = '프로필조회' then event_cnt else 0 end) as profile_view_cnt,
                sum(case when event_name = '목록노출' then event_user_cnt else 0 end) as list_view_user_cnt,
                sum(case when event_name = '목록클릭' then event_user_cnt else 0 end) as list_click_user_cnt,
                sum(case when event_name = '프로필조회' then event_user_cnt else 0 end) as profile_view_user_cnt
            from stat
            group by 1,2
            order by 1,2
            '''
    )



    # end = dummy.DummyOperator(task_id='end')
  
######################################################################################################
    start 
    start>>check_events_table_lastday>>stat_daily_listviewclick_and_profileview_cnt_by_lawyer


