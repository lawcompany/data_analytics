from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")


with DAG(
    dag_id="f_lnc_kpi_w",
    description ="lawncompany kpi for shareholders",
    start_date = datetime(2022, 8, 3, tzinfo = KST),
    schedule_interval = '0 9 * * 1', #UTC기준??
    tags=["jungarui","KPI"],
    default_args={
        "owner": "jungarui"#,
        #"retries": 3,  # Task가 실패한 경우, 3번 재시도
        #"retry_delay": timedelta(minutes=3),  # 재시도하는 시간 간격은 3분
    }
) as dag:


    start = DummyOperator(
        task_id="start"
    )


    summary_kpi = BigQueryExecuteQueryOperator(
        task_id = 'summary_kpi',
        destination_dataset_table = 'lawtalk-bigquery.for_shareholder.f_lnc_kpi_w${{ ds_nodash }}',
        use_legacy_sql = False,
        write_disposition = 'WRITE_TRUNCATE',
        sql = """
        with all_lawyer as
        ##변호사 명단
        (
            select *
            FROM
            (
                select a.lawyer
                     , b.name
                     , b.manager
                     , b.role
                     , b.flag
                     , b.writeRate
                     , lower(regexp_extract(b.flag, r"'activation': (\w+)")) as act_char
                     , case when lower(regexp_extract(b.flag, r"'activation': (\w+)")) = 'true' then 1 else 0 end as is_act
                     , lower(regexp_extract(b.flag, r"'holdWithdrawal': (\w+)")) as hold_char
                     , case when lower(regexp_extract(b.flag, r"'holdWithdrawal': (\w+)")) = 'true' then 1 else 0 end as is_hold
                     , safe_cast(regexp_extract(b.writeRate, r"'percentage': (\w+)") as int64) as full_num
                     , case when safe_cast(regexp_extract(b.writeRate, r"'percentage': (\w+)") as int64) >= 100 then 1 else 0 end as is_full
                FROM
                (
                select lawyer
                from `raw.users`
                where lawyer is not null
                and role = 'lawyer'
                and date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{next_ds}}'
                ) a
                inner join `raw.lawyers` b
                on a.lawyer = b._id
                and b.role = 'lawyer'
            ) a
        )
        , open_lawyer As (
        ## 공개변호사 명단
            select *
            from all_lawyer a
            where a.is_act = 1 and a.is_hold = 0 and a.is_full = 1
        )
        ,adorders_pausehistory_tmp AS (
        ## adorders에 휴면 기간만 발췌해오는 중간테이블(일단 array로 만듦)
            select
              _id as adorders_id
              ,pauseHistory
              ,regexp_extract_all(pauseHistory, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as pauseHistory_startAt
              ,regexp_extract_all(pauseHistory, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as pauseHistory_endAt
            from `lawtalk-bigquery.raw.adorders`
            where date(DATETIME(createdAt, 'Asia/Seoul')) <= '{{next_ds}}'
              and REGEXP_CONTAINS(pauseHistory, 'ObjectId')
        )
        , adorders_pausehistory AS (
        ## adorders_pausehistory_tmp를 통해 row형태로 가공
            select
              adorders_id
              ,DATE(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_startAt), 'Asia/Seoul')) as pauseHistory_startAt_date
              ,DATE(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_endAt), 'Asia/Seoul')) as pauseHistory_endAt_date
            from adorders_pausehistory_tmp
              ,unnest(pauseHistory_startAt) as pauseHistory_startAt with offset as pos1
              ,unnest(pauseHistory_endAt) as pauseHistory_endAt with offset as pos2
            where pos1=pos2
        )
        , adoders_arrange AS
        ## adorders 가공(start_date,end_date 파싱 및 휴면기간 매핑)
        (
            select a.lawyer
                 , a._id
                 , b.adorders_id as pause_lawyer_id
                 , a.status
                 , a.term
                 , DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(a.term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as start_date
                 , DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(a.term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as end_date
                 , b.pauseHistory_startAt_date
                 , b.pauseHistory_endAt_date
            from `raw.adorders` a
            left join adorders_pausehistory b
            on a._id = b.adorders_id
        )
        , adpayments_arrange AS
        ## adpayments 가공(order_id파싱)
        (
            select distinct b as paid_lawyer_orders
                 , coupon
                 , status
              from
              (
                  select regexp_extract_all(orders, "ObjectId\\\\('(.*?)'") as paid_lawyer_orders
                     , coupon
                     , status
                  from `raw.adpayments`
              ) a,
              unnest(paid_lawyer_orders) as b
        )
        , betaadorders_arrange AS
        ## 플러스광고 테이블인 betaadorders 가공
        (
            select lawyer
                 , DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as start_date
                 , DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as end_date
            from `raw.betaadorders`
            where status = 'apply'
        )
        , freecoupons AS
        ## 분야 광고 1개 무료 쿠폰 id만 발췌
        (
            select distinct _id, campaign
            from `raw.adcoupons`
            where campaign = '분야프로모션_분야한개무료광고_자동생성'
        )
        , ad_lawyer AS
        (
        ## 광고주 변호사
            select distinct a.lawyer
            from
            (
                select distinct a.lawyer as lawyer
                from
                (
                    select *
                    from adoders_arrange
                    where pause_lawyer_id is null
                    or date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day) not between pauseHistory_startAt_date and pauseHistory_endAt_date
                ) a
                inner join adpayments_arrange b
                on a._id = b.paid_lawyer_orders
                and a.status = 'apply'
                and b.status = 'paid'
                and date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day) between date(a.start_date) and date(a.end_date)
                union all
                select distinct a.lawyer
                from betaadorders_arrange a
                where date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day) between date(a.start_date) and date(a.end_date)
            ) a
        )
        , conversion AS
        (
           select num
           from unnest(generate_array(1,3)) as num
        )
        ## 1,2. 변호사회원수&광고주수
        select date('{{next_ds}}','Asia/Seoul') as batch_date
             , b.b_week
             , b.week_start_date
             , b.week_end_date
             , '1. 로톡' as service
             , case when a.num = 1 then '1) 로톡 변호사 회원 수' else '2) 로톡 변호사 광고주 수' end as cat
             , sum(case when a.num = 1 then summit_lawyer+withdraw_lawyer_cnt else ads_lawyer end) as f_value
        from conversion a
        cross join
        (
            SELECT d.b_week
                 , d.week_start_date
                 , d.week_end_date
                 , count(distinct a.lawyer) as summit_lawyer
                 , count(distinct c.lawyer) as ads_lawyer
                 , sum(distinct e.withdraw_lawyer_cnt) as withdraw_lawyer_cnt
            from all_lawyer a
            left join open_lawyer b
            on a.lawyer = b.lawyer
            left join ad_lawyer c
            on b.lawyer = c.lawyer
            inner join `common.d_calendar` d
            on date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day) = d.full_date
            left join `for_shareholder.f_lt_lawyer_withdraw` e
            on d.full_date = e.b_date
            group by 1,2,3
        ) b
        where a.num <= 2
        group by 1,2,3,4,5,6
        union all
        ## 3. 유료상담예약건수
        select date('{{next_ds}}','Asia/Seoul') as batch_date
             , x.b_week
             , x.week_start_date
             , x.week_end_date
             , '1. 로톡' as service
             , '3) 유료 상담예약 건수' as cat
             , count(distinct a._id) as f_value
        from `common.d_calendar` x
        inner join `raw.advicetransactions` a
        on a.status in ('paid','canceled')
        and x.full_date between date_sub(date('{{next_ds}}','Asia/Seoul'),interval 7 day) and date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day)
        and FORMAT_TIMESTAMP('%Y%m%d', createdAT, 'Asia/Seoul') = x.b_date
        inner join `raw.advice` b
        on a.advice = b._id
        group by 1,2,3,4,5,6
        union all
        ## 4. 050 콜수
        select date('{{next_ds}}','Asia/Seoul') as batch_date
             , x.b_week
             , x.week_start_date
             , x.week_end_date
             , '1. 로톡' as service
             , '4) 050 전화 상담 연결 수' as cat
             , count(distinct a._id) as f_value
        from `common.d_calendar` x
        inner join `raw.callevents` a
        on x.full_date between date_sub(date('{{next_ds}}','Asia/Seoul'),interval 7 day) and date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day)
        and FORMAT_TIMESTAMP('%Y%m%d', startedAT, 'Asia/Seoul') = x.b_date
        and a.type = 'profile'
        group by 1,2,3,4,5,6
        union all
        ## 5. 로톡비즈 변호사 회원 수
        select date('{{next_ds}}','Asia/Seoul') as batch_date
             , x.b_week
             , x.week_start_date
             , x.week_end_date
             , '4. 로톡비즈' as service
             , '1) 변호사 회원 수' as cat
             , count(distinct a.user_name) as f_value
        from `common.d_calendar` x
        inner join `for_shareholder.s_biz_lawyer` a
        on x.full_date = date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day)
        and x.full_date = a.b_date
        and FORMAT_TIMESTAMP('%Y%m%d', createdAt, 'Asia/Seoul') <= x.b_date
        group by 1,2,3,4,5,6
        union all
        ## 6. 빅케이스 회원 수
        select date('{{next_ds}}','Asia/Seoul') as batch_date
             , b.b_week
             , b.week_start_date
             , b.week_end_date
             , '2. 빅케이스' as service
             , case when a.num = 1 then '1) 변호사 회원 수' when a.num = 2 then '2) 법률전문직 회원 수' else '3) 기타 일반 회원 수' end as cat
             , sum(case when a.num = 1 then lawyer when a.num = 2 then legal_profession else etc_lawyer end) as f_value
        from conversion a
        cross join
        (
            select date('{{next_ds}}','Asia/Seoul') as batch_date
                 , x.b_week
                 , x.week_start_date
                 , x.week_end_date
                 , sum(lawyer) as lawyer
                 , sum(legal_profession) as legal_profession
                 , sum(etc_lawyer) as etc_lawyer
            from `common.d_calendar` x
            inner join
            (
                select date('{{next_ds}}','Asia/Seoul') as batch_date
                     , sum(lawyer) as lawyer
                     , sum(legal_profession) as legal_profession
                     , group_all_user_cnt-sum(lawyer)-sum(legal_profession) as etc_lawyer
                from
                (
                    select distinct b_date
                         , all_user_cnt
                         , lawyer
                         , legal_profession
                         , last_value(all_user_cnt) over(order by b_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as group_all_user_cnt
                     from `for_shareholder.f_bc_lawyer`
                    where b_date < date('{{next_ds}}','Asia/Seoul')
                ) a
                group by date('{{next_ds}}','Asia/Seoul')
                       , group_all_user_cnt
            ) a
            on x.full_date = date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day)
            group by 1,2,3,4
        ) b
        where a.num <= 3
        group by 1,2,3,4,5,6
        union all
        ## 7. 모든변호사 변호사 회원 수
        select date('{{next_ds}}','Asia/Seoul') as batch_date
             , x.b_week
             , x.week_start_date
             , x.week_end_date
             , '3. 모든변호사' as service
             , '1) 변호사 회원 수' as cat
             , a.lawyer_cnt as f_value
        from `common.d_calendar` x
        inner join `for_shareholder.f_mb_monica_lawyer` a
        on x.full_date = date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day)-- 어제날짜
        and x.full_date = a.b_date
        group by 1,2,3,4,5,6,7
        union all
        ## 8. 온라인상담 답변글 수
        select date('{{next_ds}}','Asia/Seoul') as batch_date
             , x.b_week
             , x.week_start_date
             , x.week_end_date
             , '1. 로톡' as service
             , '5) 온라인 상담 답변 수' as cat
             , count(distinct a._id) as f_value
        from `common.d_calendar` x
        inner join `raw.answers` a
        on x.full_date between date_sub(date('{{next_ds}}','Asia/Seoul'),interval 7 day) and date_sub(date('{{next_ds}}','Asia/Seoul'),interval 1 day)
        and FORMAT_TIMESTAMP('%Y%m%d', createdAT, 'Asia/Seoul') = x.b_date
        group by 1,2,3,4,5,6
        """
    )

start >> summary_kpi
