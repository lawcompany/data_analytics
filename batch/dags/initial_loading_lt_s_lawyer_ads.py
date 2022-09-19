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
    dag_id="initial_loading_lt_s_lawyer_ads",
    description ="initial_loading_lt_s_lawyer_ads",
    start_date = datetime(2022, 6, 25, tzinfo = KST),
    #schedule_interval = '0 5 * * *',
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

    ########################################################
    #dataset: mart
    #table_name: lt_s_lawyer_ads
    #description: [로톡] 일자별 변호사별 광고 이용 현황(기준일자에 사용중인 광고에 대해 리스트업)
    #table_type: snapshot
    #reprocessing date range: b_date 기준 1일치 처리 (해당일자 시점의 스냅샷 형태로 하루치만 처리하면 됨)
    ########################################################

    delete_lt_s_lawyer_ads = BigQueryOperator(
        task_id = 'delete_lt_s_lawyer_ads',
        use_legacy_sql = False,
        sql = "delete from `lawtalk-bigquery.mart.lt_s_lawyer_ads` where b_date = date('{{next_ds}}')"
    )

    insert_lt_s_lawyer_ads = BigQueryExecuteQueryOperator(
        task_id='insert_lt_s_lawyer_ads',
        use_legacy_sql = False,
        destination_dataset_table='lawtalk-bigquery.mart.lt_s_lawyer_ads',
        write_disposition = 'WRITE_APPEND',
        sql='''
            with pause_lawyer as
            ( -- 광고 일시정지 중인 변호사의 광고 리스트 발췌
                select distinct lawyer_id as lawyer_id
                     , order_id
                  from `lawtalk-bigquery.mart.lt_r_lawyer_ad_sales` a
                     , unnest(pause_start_dt) as pause_start_dt with offset as pos1
                     , unnest(pause_end_dt) as pause_end_dt with offset as pos2
                 where pos1 = pos2
                   and date('{{next_ds}}') between date(pause_start_dt) and date(pause_end_dt)
            )
            select date('{{next_ds}}') as b_date
                 , a.lawyer_id
                 , a.slug
                 , a.lawyer_name
                 , a.manager
                 , a.kind
                 , case when a.kind='location' then a.location_group_id else a.category_id end as ad_id
                 , case when a.kind='location' then a.location_group_name else a.category_name end as ad_name
                 , case when c._id is not null then 1 else 0 end as is_free
              from `lawtalk-bigquery.mart.lt_r_lawyer_ad_sales` a
              left join pause_lawyer b
                on a.lawyer_id = b.lawyer_id
               and a.order_id = b.order_id
              left join
                 (
                    select distinct _id, campaign
                      from `lawtalk-bigquery.raw.adcoupons`
                     where campaign = '분야프로모션_분야한개무료광고_자동생성'
                 ) c
                on a.coupon_id = c._id
             where (a.kind = 'plus' or a.pay_status = 'paid')
               and a.order_status = 'apply'
               and date('{{next_ds}}') between date(a.ad_start_dt) and date(a.ad_end_dt)
               and b.lawyer_id is null
            '''
    )

start >> delete_lt_s_lawyer_ads >> insert_lt_s_lawyer_ads
