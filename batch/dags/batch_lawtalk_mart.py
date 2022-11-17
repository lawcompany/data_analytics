from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from datetime import timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="batch_lawtalk_mart",
    description ="lawtalk data mart",
    start_date = datetime(2022, 9, 11, tzinfo = KST),
    schedule_interval = '0 5 * * *',
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
    #table_name: lt_s_lawyer_info
    #description: [로톡] 변호사 정보
    #table_type: s단 일자별 스냅샷
    #reprocessing date range: b_date 기준 1일치 처리 (해당일자 시점의 스냅샷 형태로 하루치만 처리하면 됨)
    ########################################################

    with TaskGroup(
        group_id="lt_s_lawyer_info"
    ) as lt_s_lawyer_info:

        delete_lt_s_lawyer_info = BigQueryOperator(
            task_id = 'delete_lt_s_lawyer_info',
            use_legacy_sql = False,
            sql = "delete from `lawtalk-bigquery.mart.lt_s_lawyer_info` where b_date = date('{{next_ds}}')"
        )

        insert_lt_s_lawyer_info = BigQueryExecuteQueryOperator(
            task_id='insert_lt_s_lawyer_info',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.lt_s_lawyer_info',
            write_disposition = 'WRITE_APPEND',
            sql='''
                with all_lawyer as
                (
                    select distinct a.*
                        , b.adLocationGroup as address_location_id
                        , c.name as address_location_name
                    from
                    (
                        select b._id as lawyer_id
                            , b.name as lawyer_name
                            , case when b.slug = '5e314482c781c20602690b79' and b._id = '5e314482c781c20602690b79' then '탈퇴한 변호사'
                                    when b.slug = '5e314482c781c20602690b79' and b._id = '616d0c91b78909e152c36e71' then '미활동 변호사'
                                    when b.slug like '%탈퇴한%' then concat(b.slug,'(탈퇴보류)')
                                    else slug
                            end as slug
                            , b.manager
                            , b.role as lawyer_role
                            , a.role as user_role
                            , case when b.role = 'lawyer' and a.role = 'lawyer' then 1 else 0 end as is_approved
                            , case when b.role = 'lawyer-waiting' and a.role = 'lawyer-waiting' then 1 else 0 end as is_waiting
                            , b.flag
                            , b.writeRate
                            , lower(regexp_extract(b.flag, r"'activation': (\w+)")) as act_char
                            , case when lower(regexp_extract(b.flag, r"'activation': (\w+)")) = 'true' then 1 else 0 end as is_act
                            , lower(regexp_extract(b.flag, r"'holdWithdrawal': (\w+)")) as hold_char
                            , case when lower(regexp_extract(b.flag, r"'holdWithdrawal': (\w+)")) = 'true' then 1 else 0 end as is_hold
                            , safe_cast(regexp_extract(b.writeRate, r"'percentage': (\w+)") as int64) as full_num
                            , case when safe_cast(regexp_extract(b.writeRate, r"'percentage': (\w+)") as int64) >= 100 then 1 else 0 end as is_full
                            , b.address
                            , regexp_extract(b.address, r"\'level2\': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as location2
                            , regexp_extract(b.address, r"\'level1\': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as location1
                            , safe_cast(regexp_extract(b.adviceFee, r"'phone': (\w+)") as numeric) as counsel_phone_fee
                            , safe_cast(regexp_extract(b.adviceFee, r"'video': (\w+)") as numeric) as counsel_video_fee
                            , safe_cast(regexp_extract(b.adviceFee, r"'visiting': (\w+)") as numeric) as counsel_visiting_fee
                            , date(birth,'Asia/Seoul') as birth
                            , company
                            , sex
                            , regexp_extract(b.examination, r"'body': '(.*?)'") as exam
                            , regexp_extract(b.examination, r"'year': '(.*?)'") as exam_round
                            , regexp_extract(b.examination, r"'trainingInstitute': '(.*?)'") as exam_generation
                            , safe_cast(examYear as int64) as exam_year
                            , datetime(b.createdAt,'Asia/Seoul') as crt_dt
                        from `lawtalk-bigquery.raw.lawyers` b
                        left join
                            (
                                select lawyer, role
                                from `lawtalk-bigquery.raw.users`
                                where lawyer is not null
                                and role in ('lawyer','lawyer-waiting')
                            ) a
                        on b._id = a.lawyer
                        and b.role = a.role
                        ) a
                    left join `lawtalk-bigquery.raw.adlocations` b
                    on a.location2 = b.address
                    or a.location1 = b.address
                    left join `lawtalk-bigquery.raw.adlocationgroups` c
                    on b.adLocationGroup = c._id
                )
                , ads_lawyer_info as
                (
                select a.lawyer_id
                    , a.lawyer_name
                    , count(distinct(case when a.kind = 'category' then a.ad_id end)) as category_ad_cnt
                    , count(distinct(case when a.kind = 'location' then a.ad_id end)) as location_ad_cnt
                    , count(distinct(case when a.kind = 'plus' then a.ad_id end)) as plus_ad_cnt
                    , count(distinct(case when a.is_free=1 then a.ad_id end)) as free_category_ad_cnt
                    from `lawtalk-bigquery.mart.lt_s_lawyer_ads` a
                where b_date = date('{{next_ds}}') -- 20221002 added by LJA
                group by a.lawyer_id
                        , a.lawyer_name
                )
                , yesterday_lawyer_info as
                (
                    select *
                    from `lawtalk-bigquery.mart.lt_s_lawyer_info`
                    where b_date = date('{{next_ds}}') - 1
                )
                , acc_review_cnt as
                (
                    select lawyer_id
                        , count(distinct counsel_id) as acc_review_cnt
                    from `lawtalk-bigquery.mart.lt_r_user_pay_counsel`
                    where user_review_dt is not null
                    group by lawyer_id
                )
                , acc_qna_cnt as
                (
                    select lawyer as lawyer_id
                        , count(distinct _id) as acc_qna_cnt
                    from `lawtalk-bigquery.raw.answers`
                    group by lawyer
                )
                , acc_post_cnt as
                (
                    select lawyer as lawyer_id
                        , count(distinct _id) as acc_post_cnt
                    from `lawtalk-bigquery.raw.posts`
                    group by lawyer
                )
                , acc_legaltip_cnt as
                (
                    select lawyer as lawyer_id
                        , count(distinct _id) as acc_legaltip_cnt
                    from `lawtalk-bigquery.raw.videos`
                    group by lawyer
                )
                , acc_counsel_cnt as
                (
                    select lawyer_id
                        , count(distinct counsel_id) as acc_counsel_cnt
                    from `lawtalk-bigquery.mart.lt_r_user_pay_counsel`
                    where pay_status = 'paid'
                    group by lawyer_id
                )
                , acc_050call_cnt as
                (
                    select lawyer as lawyer_id
                        , count(distinct _id) as acc_050call_cnt
                    from `lawtalk-bigquery.raw.callevents`
                    where duration >= 60
                    and type = 'profile' -- 20220930 added
                    group by lawyer
                )
                select date('{{next_ds}}') as b_date
                    , case when a.lawyer_id is not null then a.lawyer_id else b.lawyer_id end as lawyer_id
                    , case when a.lawyer_id is not null then a.slug else b.slug end as slug
                    , case when a.lawyer_id is not null then a.lawyer_name else b.lawyer_name end as lawyer_name
                    , case when a.lawyer_id is not null then a.manager else b.manager end as manager
                    , case when a.lawyer_id is not null then a.is_approved else b.is_approved end as is_approved
                    , case when a.lawyer_id is not null then a.is_opened else b.is_opened end as is_opened
                    , case when a.lawyer_id is not null then a.is_ad else b.is_ad end as is_ad
                    , case when a.lawyer_id is not null then a.paid_kind else b.paid_kind end as paid_kind
                    , case when a.lawyer_id is not null then a.is_paused else b.is_paused end as is_paused
                    , case when a.lawyer_id is not null then a.is_fully_profile else b.is_fully_profile end as is_fully_profile
                    , case when b.lawyer_id is not null and a.lawyer_id is null then 1 else 0 end as is_resting -- 전일자엔 변호사 정보가 있는데 당일자엔 없다면 휴면처리가 되어 센티널로 넘어갔다고 볼 수 있음
                    , case when a.lawyer_id is not null then a.is_category_ad else b.is_category_ad end as is_category_ad
                    , case when a.lawyer_id is not null then a.is_location_ad else b.is_location_ad end as is_location_ad
                    , case when a.lawyer_id is not null then a.is_plus_ad else b.is_plus_ad end as is_plus_ad
                    , case when a.lawyer_id is not null then a.category_ad_cnt else b.category_ad_cnt end as category_ad_cnt
                    , case when a.lawyer_id is not null then a.location_ad_cnt else b.location_ad_cnt end as location_ad_cnt
                    , case when a.lawyer_id is not null then a.plus_ad_cnt else b.plus_ad_cnt end as plus_ad_cnt
                    , case when a.lawyer_id is not null then a.address_location_id else b.address_location_id end as address_location_id
                    , case when a.lawyer_id is not null then a.address_location_name else b.address_location_name end as address_location_name
                    , case when a.lawyer_id is not null then a.counsel_phone_fee else b.counsel_phone_fee end as counsel_phone_fee
                    , case when a.lawyer_id is not null then a.counsel_video_fee else b.counsel_video_fee end as counsel_video_fee
                    , case when a.lawyer_id is not null then a.counsel_visiting_fee else b.counsel_visiting_fee end as counsel_visiting_fee
                    , case when a.lawyer_id is not null then a.birth else b.birth end as birth
                    , case when a.lawyer_id is not null then a.company else b.company end as company
                    , case when a.lawyer_id is not null then a.sex else b.sex end as sex
                    , case when a.lawyer_id is not null then a.exam else b.exam end as exam
                    , case when a.lawyer_id is not null then a.exam_round else b.exam_round end as exam_round
                    , case when a.lawyer_id is not null then a.exam_generation else b.exam_generation end as exam_generation
                    , case when a.lawyer_id is not null then a.exam_year else b.exam_year end as exam_year
                    , case when a.lawyer_id is not null then a.acc_review_cnt else b.acc_review_cnt end as acc_review_cnt
                    , case when a.lawyer_id is not null then a.acc_qna_cnt else b.acc_qna_cnt end as acc_qna_cnt
                    , case when a.lawyer_id is not null then a.acc_post_cnt else b.acc_post_cnt end as acc_post_cnt
                    , case when a.lawyer_id is not null then a.acc_legaltip_cnt else b.acc_legaltip_cnt end as acc_legaltip_cnt
                    , case when a.lawyer_id is not null then a.acc_counsel_cnt else b.acc_counsel_cnt end as acc_counsel_cnt
                    , case when a.lawyer_id is not null then a.acc_050call_cnt else b.acc_050call_cnt end as acc_050call_cnt
                    , case when a.lawyer_id is not null then a.crt_dt else b.crt_dt end as crt_dt
                from
                (
                    select a.lawyer_id
                        , a.slug
                        , a.lawyer_name
                        , a.manager
                        , case when a.is_approved=1 then 1 -- 회원 승인
                                when a.is_waiting=1 then 0 -- 회원 미승인
                                else null -- 비정상 데이터
                        end as is_approved
                        , case when a.is_approved=1 and a.is_act = 1 and a.is_hold = 0 and a.is_full = 1 then 1
                                when a.is_approved=0 then null -- 비정상 데이터
                                else 0
                        end as is_opened -- 공개/비공개 여부
                        , case when a.is_approved=1 and b.lawyer_id is not null then 1
                                when a.is_approved=0 then null -- 비정상 데이터
                                else 0
                        end as is_ad -- 광고/비광고 여부
                        , case when a.is_approved=1 and b.category_ad_cnt+b.location_ad_cnt=1 and b.free_category_ad_cnt=1 and b.plus_ad_cnt=0 then 'free'
                                when a.is_approved=1 and b.lawyer_id is not null then 'paid'
                        end as paid_kind
                        , case when a.is_approved=1 and (a.is_act = 0 or a.is_hold = 1) then 1
                                when a.is_approved=0 then null -- 비정상 데이터
                                else 0
                        end as is_paused -- 일시정지 여부
                        , case when a.is_approved=1 and a.is_full = 1 then 1
                                when a.is_approved=0 then null -- 비정상 데이터
                                else 0
                        end as is_fully_profile -- 프로필 100% 작성 여부
                        , case when a.is_approved=1 and b.category_ad_cnt>0 then 1
                                when a.is_approved=0 then null -- 비정상 데이터
                                else 0
                        end as is_category_ad -- 분야광고주 여부
                        , case when a.is_approved=1 and b.location_ad_cnt>0 then 1
                                when a.is_approved=0 then null -- 비정상 데이터
                                else 0
                        end as is_location_ad -- 지역광고주 여부
                        , case when a.is_approved=1 and b.plus_ad_cnt>0 then 1
                                when a.is_approved=0 then null -- 비정상 데이터
                                else 0
                        end as is_plus_ad -- 플러스광고주 여부
                        , coalesce(b.category_ad_cnt,0) as category_ad_cnt
                        , coalesce(b.location_ad_cnt,0) as location_ad_cnt
                        , coalesce(b.plus_ad_cnt,0) as plus_ad_cnt
                        , a.address_location_id
                        , a.address_location_name
                        , a.counsel_phone_fee
                        , a.counsel_video_fee
                        , a.counsel_visiting_fee
                        , a.birth
                        , a.company
                        , a.sex
                        , a.exam
                        , a.exam_round
                        , a.exam_generation
                        , a.exam_year
                        , coalesce(c.acc_review_cnt,0) as acc_review_cnt
                        , coalesce(d.acc_qna_cnt,0) as acc_qna_cnt
                        , coalesce(e.acc_post_cnt,0) as acc_post_cnt
                        , coalesce(f.acc_legaltip_cnt,0) as acc_legaltip_cnt
                        , coalesce(g.acc_counsel_cnt,0) as acc_counsel_cnt
                        , coalesce(h.acc_050call_cnt,0) as acc_050call_cnt
                        , a.crt_dt
                    from all_lawyer a
                    left join ads_lawyer_info b
                        on a.lawyer_id = b.lawyer_id
                    left join acc_review_cnt c
                        on a.lawyer_id = c.lawyer_id
                    left join acc_qna_cnt d
                        on a.lawyer_id = d.lawyer_id
                    left join acc_post_cnt e
                        on a.lawyer_id = e.lawyer_id
                    left join acc_legaltip_cnt f
                        on a.lawyer_id = f.lawyer_id
                    left join acc_counsel_cnt g
                        on a.lawyer_id = g.lawyer_id
                    left join acc_050call_cnt h
                        on a.lawyer_id = h.lawyer_id
                ) a
                full join yesterday_lawyer_info b -- 휴면 여부 확인을 위해 전일자 데이터 발췌
                    on a.lawyer_id = b.lawyer_id
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
                -- group by 하는 이유 : 탈퇴한변호사(lawyer_id =5e2169cb6f333d01bee4924a)데이터가 계속 중복되는 이슈 발생
                '''
        )

        delete_lt_s_lawyer_info >> insert_lt_s_lawyer_info


    ########################################################
    #dataset: mart
    #table_name: lt_r_lawyer_ad_sales
    #description: [[로톡] 변호사 광고 구매 로그(adorders(주문)와 adpayments(결제), betaadorders(플러스광고) 테이블을 조인하여 정리한 테이블)
    #table_type: raw data
    #reprocessing date range: b_date기준 16일치 재처리(D-16 ~ D-1) : 광고기간이 1~15일 / 16~말일이고 그 사이 데이터 변경이 있을 수 있어 16일치 재처리(말일이 31일 경우 최대 16일이므로..)
    ########################################################

    with TaskGroup(
        group_id="lt_r_lawyer_ad_sales"
    ) as lt_r_lawyer_ad_sales:

        #집계데이터 tmp테이블에 먼저 insert
        insert_tmp_lt_r_lawyer_ad_sales = BigQueryExecuteQueryOperator(
            task_id='insert_tmp_lt_r_lawyer_ad_sales',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.tmp_lt_r_lawyer_ad_sales',
            write_disposition = 'WRITE_TRUNCATE',
            sql='''
                with lawyer as
                ( -- 변호사 정보
                    select _id as lawyer_id
                         , name as lawyer_name
                         , manager
                         , case when slug = '5e314482c781c20602690b79' and _id = '5e314482c781c20602690b79' then '탈퇴한 변호사'
                                when slug = '5e314482c781c20602690b79' and _id = '616d0c91b78909e152c36e71' then '미활동 변호사'
                                when slug like '%탈퇴한%' then concat(slug,'(탈퇴보류)')
                                else slug
                           end as slug
                      from `lawtalk-bigquery.raw.lawyers`
                     where role = 'lawyer'
                )
                , adorders_base as
                ( -- adorders에서 필요한 정보만 간추려 가져온다.
                    select _id as order_id
                         , lawyer as lawyer_id
                         , regexp_extract_all(adLocations, r"'locationId': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as location_id
                         , regexp_extract_all(categories, r"'adCategoryId': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as category_id
                         , coupon as coupon_id
                         , safe_cast(price as numeric) as price
                         , status as order_status
                         , datetime(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as ad_start_dt
                         , datetime(parse_timestamp('%Y, %m, %e, %H, %M, %S', regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as ad_end_dt
                         , regexp_extract_all(pauseHistory, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") pause_start_dt
                         , regexp_extract_all(pauseHistory, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") pause_end_dt
                         , case when regexp_extract(autoExtendTerm,r"'status': \'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'")='on' then 1 else 0 end as is_auto_extend
                         , datetime(createdAt,'Asia/Seoul') as order_crt_dt
                         , datetime(updatedAt,'Asia/Seoul') as order_upd_dt
                      from `lawtalk-bigquery.raw.adorders`
                     where ((date(createdAt,'Asia/Seoul') >= date('2022-06-16') -- 분야개편 이후
                       and date(createdAt,'Asia/Seoul') between date('{{next_ds}}')-15 and date('{{next_ds}}'))
                        or date(updatedAt,'Asia/Seoul') = date('{{next_ds}}')) -- 20221017 added by jungarui 무료광고분야의 분야를 광고기간중엔 언제든지 바꿀 수 있다고 함.
                )
                , adorders_pause as
                ( -- pause_start_dt와 end_date를 KST datetime으로 형변환하여 다시 array로 만든다.
                    select order_id
                         , array_agg(pause_start_dt) as pause_start_dt
                         , array_agg(pause_end_dt) as pause_end_dt
                      from
                      (
                          select order_id
                               , datetime(parse_timestamp('%Y, %m, %e, %H, %M', pause_start_dt), 'Asia/Seoul') as pause_start_dt
                               , datetime(parse_timestamp('%Y, %m, %e, %H, %M, %S', pause_end_dt), 'Asia/Seoul') as pause_end_dt
                            from adorders_base
                               , unnest(pause_start_dt) as pause_start_dt with offset as pos1
                               , unnest(pause_end_dt) as pause_end_dt with offset as pos2
                           where pos1 = pos2
                      ) a
                     group by order_id
                )
                , adorders_location as
                ( -- 지역광고 주문건에 대해 지역정보를 가져온다.
                    select a.order_id
                         , array_agg(a.location_id) as location_id
                         , array_agg(b.name) as location_name
                         , b.adLocationGroup as location_group_id
                         , c.name as location_group_name
                         , c.adLocationCategory as location_category_id
                         , d.name as location_category_name
                      from
                      (
                          select order_id
                               , location_id
                            from adorders_base,
                                 unnest(location_id) as location_id
                      ) a
                      left join `lawtalk-bigquery.raw.adlocations` b
                        on a.location_id = b._id
                      left join `lawtalk-bigquery.raw.adlocationgroups` c
                        on b.adLocationGroup = c._id
                      left join `lawtalk-bigquery.raw.adlocationcategories` d
                        on c.adLocationCategory = d._id
                      group by a.order_id
                             , b.adLocationGroup
                             , c.name
                             , c.adLocationCategory
                             , d.name
                )
                , adorders_category as
                ( -- 분야광고 주문건에 대해 분야정보를 가져온다.
                    select a.order_id
                         , a.category_id
                         , b.name as category_name
                      from
                      (
                          select order_id
                               , category_id
                            from adorders_base,
                                 unnest(category_id) as category_id
                      ) a
                      left join `lawtalk-bigquery.raw.adcategories` b
                        on a.category_id = b._id
                )
                , adpayments_base as
                ( -- adpayments에서 필요한 정보만 간추려 가져온다.
                    select _id as pay_id
                         , status as pay_status
                         , safe_cast(fixedFee as numeric) as tot_fixed_fee
                         , safe_cast(originFee as numeric) as tot_origin_fee
                         , adSubscription as subscription_id
                         , coupon as coupon_id
                         , method as pay_method
                         , datetime(requestedAt,'Asia/Seoul')  as pay_req_dt
                         , datetime(createdAt,'Asia/Seoul') as pay_crt_dt
                         , datetime(updatedAt,'Asia/Seoul') as pay_upd_dt
                         , datetime(parse_timestamp('%Y, %m, %e, %H, %M, %S', regexp_extract(canceled, r"'at': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as pay_canc_dt
                         , order_id
                      from `lawtalk-bigquery.raw.adpayments`
                         , unnest(regexp_extract_all(orders, r"ObjectId\('(.*?)'\)")) as order_id
                )
                , betaadorders_base as
                ( -- betaadorders에서 필요한 정보만 간추려 가져온다.
                    select date(createdAt,'Asia/Seoul') as b_date
                         , _id as order_id
                         , lawyer as lawyer_id
                         , adCategory as category_id
                         , category as category_name
                         , safe_cast(price as numeric) as price
                         , status as order_status
                         , datetime(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as ad_start_dt
                         , datetime(parse_timestamp('%Y, %m, %e, %H, %M, %S', regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as ad_end_dt
                         , datetime(createdAt,'Asia/Seoul') as order_crt_dt
                         , datetime(updatedAt,'Asia/Seoul') as order_upd_dt
                      from `lawtalk-bigquery.raw.betaadorders` a
                     where date(createdAt,'Asia/Seoul') >= date('2022-06-16') -- 분야개편 이후
                       and date(createdAt,'Asia/Seoul') between date('{{next_ds}}')-15 and date('{{next_ds}}')
                )
                -- 각 정보들 join하여 최종 데이터 구성
                select date(a.order_crt_dt) as b_date
                     , b.pay_id
                     , a.order_id
                     , a.lawyer_id
                     , c.slug
                     , c.lawyer_name
                     , c.manager
                     , case when array_length(a.location_id) > 0 then 'location'
                            when array_length(a.category_id) > 0 then 'category'
                            else 'N/A'
                       end as kind
                     , d.location_id
                     , d.location_name
                     , d.location_group_id
                     , d.location_group_name
                     , d.location_category_id
                     , d.location_category_name
                     , e.category_id
                     , e.category_name
                     , b.pay_status
                     , b.tot_fixed_fee
                     , b.tot_origin_fee
                     , b.subscription_id
                     , coalesce(a.coupon_id,b.coupon_id) as coupon_id
                     , b.pay_method
                     , b.pay_req_dt
                     , b.pay_crt_dt
                     , b.pay_upd_dt
                     , b.pay_canc_dt
                     , a.price
                     , a.order_status
                     , a.ad_start_dt
                     , a.ad_end_dt
                     , f.pause_start_dt
                     , f.pause_end_dt
                     , a.is_auto_extend
                     , a.order_crt_dt
                     , a.order_upd_dt
                  from adorders_base a
                  left join adpayments_base b
                    on a.order_id = b.order_id
                  left join lawyer c
                    on a.lawyer_id = c.lawyer_id
                  left join adorders_location d
                    on a.order_id = d.order_id
                  left join adorders_category e
                    on a.order_id = e.order_id
                  left join adorders_pause f
                    on a.order_id = f.order_id
                 union all
                -- 플러스광고 따로 insert
                select a.b_date
                     , null as pay_id
                     , a.order_id
                     , a.lawyer_id
                     , c.slug
                     , c.lawyer_name
                     , c.manager
                     , 'plus' as kind
                     , null as location_id
                     , null as location_name
                     , null as location_group_id
                     , null as location_group_name
                     , null as location_category_id
                     , null as location_category_name
                     , a.category_id
                     , a.category_name
                     , null as pay_status
                     , null as tot_fixed_fee
                     , null as tot_origin_fee
                     , null as subscription_id
                     , null as coupon_id
                     , null as pay_method
                     , null as pay_req_dt
                     , null as pay_crt_dt
                     , null as pay_upd_dt
                     , null as pay_canc_dt
                     , a.price
                     , a.order_status
                     , a.ad_start_dt
                     , a.ad_end_dt
                     , null as pause_start_dt
                     , null as pause_end_dt
                     , null as is_auto_extend
                     , a.order_crt_dt
                     , a.order_upd_dt
                  from betaadorders_base a
                  left join lawyer c
                    on a.lawyer_id = c.lawyer_id
                '''
        )

        #upsert를 위해 앞에서 집계한 tmp테이블을 가지고 delete
        delete_lt_r_lawyer_ad_sales = BigQueryOperator(
            task_id = 'delete_lt_r_lawyer_ad_sales',
            use_legacy_sql = False,
            sql = '''
                  delete from `lawtalk-bigquery.mart.lt_r_lawyer_ad_sales`
                        where b_date between date('{{next_ds}}')-15 and date('{{next_ds}}')
                           or order_id in (select distinct order_id from `lawtalk-bigquery.mart.tmp_lt_r_lawyer_ad_sales`)
                  '''
        )

        #원테이블에 tmp데이터 전체 insert
        insert_lt_r_lawyer_ad_sales = BigQueryExecuteQueryOperator(
            task_id='insert_lt_r_lawyer_ad_sales',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.lt_r_lawyer_ad_sales',
            write_disposition = 'WRITE_APPEND',
            sql='''
                select *
                  from `lawtalk-bigquery.mart.tmp_lt_r_lawyer_ad_sales`
                '''
        )

        insert_tmp_lt_r_lawyer_ad_sales >> delete_lt_r_lawyer_ad_sales >> insert_lt_r_lawyer_ad_sales


    ########################################################
    #dataset: mart
    #table_name: lt_s_lawyer_ads
    #description: [로톡] 일자별 변호사별 광고 이용 현황(기준일자에 사용중인 광고에 대해 리스트업)
    #table_type: snapshot
    #reprocessing date range: b_date 기준 1일치 처리 (해당일자 시점의 스냅샷 형태로 하루치만 처리하면 됨)
    ########################################################

    with TaskGroup(
        group_id="lt_s_lawyer_ads"
        ) as lt_s_lawyer_ads:

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
                     , sum(case when c._id is not null then 1 else 0 end) as is_free
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
                 group by 1,2,3,4,5,6,7,8 -- 20221012 by jungarui 무료광고 체크 시 중복건 발생 확인하여 수정
                '''
        )

        delete_lt_s_lawyer_ads >> insert_lt_s_lawyer_ads


    ########################################################
    #dataset: mart
    #table_name: lt_r_user_pay_counsel
    #description: [로톡] 의뢰인 유료 상담로그(advice와 advicetransactions를 조인하여 필요정보만 발췌하여 정리한 테이블)
    #table_type: raw data
    #reprocessing date range: b_date기준 12일치 재처리(D-12 ~ D-1) : 상담예약 시 해당일자를 포함하여 D+7까지로 상담일자를 설정할 수 있고 상담일로부터 D+5까지 상담결과지 작성이 가능하여 D+12까지 데이터 변경될 가능성 있음
    #modified history :
    # 키워드-분야,지역 매핑 정확도 개선 로직 추가 (지라이슈:DATAANAL-115) 2022-11-17 by LJA
    ########################################################

    with TaskGroup(
        group_id="lt_r_user_pay_counsel"
        ) as lt_r_user_pay_counsel:

        delete_lt_r_user_pay_counsel = BigQueryOperator(
            task_id = 'delete_lt_r_user_pay_counsel',
            use_legacy_sql = False,
            sql = "delete from `lawtalk-bigquery.mart.lt_r_user_pay_counsel` where b_date between date('{{next_ds}}')-11 and date('{{next_ds}}')"
        )

        insert_lt_r_user_pay_counsel = BigQueryExecuteQueryOperator(
            task_id='insert_lt_r_user_pay_counsel',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.lt_r_user_pay_counsel',
            write_disposition = 'WRITE_APPEND',
            sql='''
                with lawyer as
                ( -- 변호사 정보
                    select _id as lawyer_id
                         , name as lawyer_name
                         , manager
                         , case when slug = '5e314482c781c20602690b79' and _id = '5e314482c781c20602690b79' then '탈퇴한 변호사'
                                when slug = '5e314482c781c20602690b79' and _id = '616d0c91b78909e152c36e71' then '미활동 변호사'
                                when slug like '%탈퇴한%' then concat(slug,'(탈퇴보류)')
                                else slug
                           end as slug
                      from `lawtalk-bigquery.raw.lawyers`
                     where role = 'lawyer'
                )
                , user as
                ( -- 유저 정보
                    select _id as user_id
                         , email as user_email
                         , username as user_nickname
                         , username as user_name
                      from `lawtalk-bigquery.raw.users`
                )
                , advice_base as
                ( -- advice 컬렉션에서 필요정보만 발췌
                    select a._id as counsel_id
                         , a.user as user_id
                         , a.email as user_email
                         , a.name as user_name
                         , lawyer as lawyer_id
                         , case when a.kind is null then 'phone' else a.kind end as kind
                         , a.body
                         , a.status as counsel_status
                         , datetime(a.createdAt,'Asia/Seoul') as counsel_crt_dt
                         , datetime(date_add(datetime(a.daystring), interval cast(a.time * 30 as int) minute)) as counsel_exc_dt
                         , datetime(a.updatedAt,'Asia/Seoul') as counsel_upd_dt
                         , a.adCategory as category_id
                         , b.name as category_name
                         , regexp_extract(a.survey, r"'surveyUser': '(.*?)'") as user_review_id
                         , regexp_extract(a.survey, r"'surveyResult': '(.*?)'") as lawyer_survey_id
                         , datetime(parse_timestamp('%Y, %m, %e, %H, %M, %S', regexp_extract(a.review, r"'date': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul')as user_review_dt
                         , regexp_extract(a.review, r"'rate': (\d+)") as user_review_rate
                         , regexp_extract(a.review, r"'title': '(.*?)'") as user_review_title
                         , regexp_extract(a.review, r"'body': '(.*?)'") as user_review_body
                         , regexp_extract(cancelInfo, r"'adviceCancelCode': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)") as counsel_cancel_code
                      from `lawtalk-bigquery.raw.advice` a
                      left join `lawtalk-bigquery.raw.adcategories` b
                        on a.adCategory = b._id
                     where date(a.createdAt,'Asia/Seoul') between date('{{next_ds}}')-11 and date('{{next_ds}}')
                )
                , advicetransactions_base as
                ( -- advicetransactions 컬렉션에서 필요정보만 발췌
                  -- 키워드 입력이 아니라 분야선택이나 지역선택해서 들어왔을 때 extra_info에만 입력되고 있어서, extra_info가 분야명이나 지역명 그 자체라면 context_additional에도 동일 값 기록되도록 보정해 줌
                    select a.pay_id
                         , a.counsel_id
                         , a.user_id
                         , a.lawyer_id
                         , a.pay_status
                         , a.pay_method
                         , a.price
                         , a.origin_fee
                         , a.good_name
                         , a.coupon_id
                         , a.device
                         , case when a.context is null then (case when b.name is not null then 'category'
                                                                  when c.name is not null then 'location'
                                                             end)
                                else a.context
                           end as context
                         , a.extra_info
                         , coalesce(a.context_additional, b.name, c.name, d.cat, e.large_cat, 'N/A') as context_additional
                         , a.adid
                         , a.pay_req_dt
                         , a.pay_crt_dt
                         , a.pay_upd_dt
                         , a.pay_canc_dt
                      from
                          (
                            select _id as pay_id
                                 , advice as counsel_id
                                 , user as user_id
                                 , lawyer as lawyer_id
                                 , status as pay_status
                                 , method as pay_method
                                 , safe_cast(regexp_extract(paid, r"'price': (\d+)") as numeric) as price
                                 , safe_cast(originFee as numeric) as origin_fee
                                 , regexp_extract(raw, r"'goodname': '(.*?)'") as good_name
                                 , coupon as coupon_id
                                 , regexp_extract(metadata, r"'ua': '(.*?)'") as device
                                 , regexp_extract(metadata, r"'context': '(.*?)'") as context
                                 , regexp_extract(metadata, r"'extraInfo': '(.*?)'") as extra_info
                                 , regexp_extract(metadata, r"'contextAdditional': '(.*?)'") as context_additional
                                 , regexp_extract(metadata, r"'adid': '(.*?)'") as adid
                                 , datetime(requestedAt,'Asia/Seoul') as pay_req_dt
                                 , datetime(createdAt,'Asia/Seoul') as pay_crt_dt
                                 , datetime(updatedAt,'Asia/Seoul') as pay_upd_dt
                                 , datetime(parse_timestamp('%Y, %m, %e, %H, %M, %S', regexp_extract(canceled, r"'at': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul')as pay_canc_dt
                              from `lawtalk-bigquery.raw.advicetransactions` a
                             where date(a.createdAt,'Asia/Seoul') between date('{{next_ds}}')-11 and date('{{next_ds}}')
                          ) a
                      left join `lawtalk-bigquery.raw.adcategories` b
                        on coalesce(a.context_additional,a.extra_info) = b.name
                      left join `lawtalk-bigquery.raw.adlocationgroups` c
                        on coalesce(a.context_additional,a.extra_info) = c.name
                      left join -- 키워드-분야 매핑 코드테이블에서 추가 보정 2022-11-17 by LJA
                           (
                             select array_to_string(array_agg(b.name),',') as cat, a.name as keyword
                               from
                                   (
                                     select json_value(cat) cat
                                          , name
                                       from `lt_src.adkeywords` a
                                          , unnest(json_query_array(adcategories)) cat
                                   ) a
                               inner join `lt_src.adcategories` b
                                  on a.cat = b._id
                             group by a.name
                           ) d
                        on replace(a.extra_info,' ','') = d.keyword
                      left join -- 키워드가 지역명을 포함하고 있을 때 추가 보정 2022-11-17 by LJA
                           (
                             select a.name cat, b.name large_cat
                               from `lt_src.adlocations` a
                               inner join `lt_src.adlocationgroups` b
                                  on a.adLocationGroup = b._id
                           ) e
                        on replace(a.extra_info,' ','') = e.cat
                )
                select coalesce(date(a.counsel_crt_dt),date(pay_crt_dt)) as b_date
                     , coalesce(a.counsel_id,b.counsel_id) as counsel_id
                     , b.pay_id
                     , coalesce(b.user_id,a.user_id) as user_id
                     , coalesce(a.user_email,d.user_email) as user_email
                     , coalesce(a.user_name,d.user_name) as user_name
                     , d.user_nickname
                     , coalesce(b.lawyer_id,a.lawyer_id) as lawyer_id
                     , c.slug
                     , c.lawyer_name
                     , c.manager
                     , a.kind
                     , b.pay_status
                     , b.pay_method
                     , b.price
                     , b.origin_fee
                     , b.good_name
                     , b.coupon_id
                     , b.device
                     , b.context
                     , b.context_additional
                     , b.extra_info
                     , b.adid
                     , b.pay_req_dt
                     , b.pay_crt_dt
                     , b.pay_upd_dt
                     , b.pay_canc_dt
                     , a.body
                     , a.counsel_status
                     , a.counsel_cancel_code
                     , e.description as counsel_cancel_reason
                     , a.counsel_crt_dt
                     , a.counsel_exc_dt
                     , a.counsel_upd_dt
                     , case when a.counsel_id is not null and a.lawyer_survey_id is null then 'N/A' else a.category_id end as category_id
                     , case when a.counsel_id is not null and a.lawyer_survey_id is null then 'N/A' else a.category_name end as category_name
                     , a.lawyer_survey_id
                     , a.user_review_id
                     , a.user_review_dt
                     , a.user_review_rate
                     , a.user_review_title
                     , a.user_review_body
                  from advice_base a
                  full join advicetransactions_base b
                    on a.counsel_id = b.counsel_id
                  left join lawyer c
                    on coalesce(b.lawyer_id,a.lawyer_id,'') = c.lawyer_id
                  left join user d
                    on coalesce(b.user_id,a.user_id,'') = d.user_id
                  left join `lawtalk-bigquery.raw.advicecancelcodes` e
                    on a.counsel_cancel_code = e._id
                '''
        )

        delete_lt_r_user_pay_counsel >> insert_lt_r_user_pay_counsel


    ########################################################
    #dataset: mart
    #table_name: lt_w_lawyer_counsel
    #description: [로톡] 일자별 변호사별 유료상담 현황
    #table_type: w단 일자별 집계
    #reprocessing date range: b_date기준 5일치 재처리(D-6 ~ D-1) : lt_r_user_pay_counsel.counsel_exc_dt기준으로 D+5까지 counsel_status가 업데이트 될 수 있으므로 5일치 재처리
    #modified history :
    #    20220926 - b_date 기준을 상담실행일자(counsel_exc_dt)로 변경하고 재처리 기간을 5일치로 변경(변경된 기준으로 데이터 재적재 완료) by LJA
    #    20221117 - cat_mapping_standard=complex 값을 추가하여 의뢰인 입력 키워드 기반의 분야가 N/A면 상담결과지의 분야로 매핑하도록 기준 추가 by LJA
    ########################################################

    with TaskGroup(
        group_id="lt_w_lawyer_counsel"
        ) as lt_w_lawyer_counsel:

        delete_lt_w_lawyer_counsel = BigQueryOperator(
            task_id = 'delete_lt_w_lawyer_counsel',
            use_legacy_sql = False,
            sql = "delete from `lawtalk-bigquery.mart.lt_w_lawyer_counsel` where b_date between date('{{next_ds}}')-5 and date('{{next_ds}}')"
        )

        insert_lt_w_lawyer_counsel = BigQueryExecuteQueryOperator(
            task_id='insert_lt_w_lawyer_counsel',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.lt_w_lawyer_counsel',
            write_disposition = 'WRITE_APPEND',
            sql='''
                -- 유저가 입력한 키워드 기준의 분야 (키워드에 다중 카테고리가 매칭될 수 있음 그럴 경우 1/n 분배)
                select a.b_date
                     , a.lawyer_id
                     , a.slug
                     , a.lawyer_name
                     , a.manager
                     , 'keyword' as cat_mapping_standard
                     , coalesce(b._id, c._id, 'N/A') as category_id
                     , a.cat as category_name
                     , sum(case when a.kind='phone' then a.counsel_cnt end) as phone_cnt
                     , sum(case when a.kind='phone' then a.counsel_price end) as phone_price
                     , sum(case when a.kind='video' then a.counsel_cnt end) as video_cnt
                     , sum(case when a.kind='video' then a.counsel_price end) as video_price
                     , sum(case when a.kind='visiting' then a.counsel_cnt end) as visiting_cnt
                     , sum(case when a.kind='visiting' then a.counsel_price end) as visiting_price
                  from
                      (
                        select date(a.counsel_exc_dt) as b_date
                             , a.lawyer_id
                             , a.slug
                             , a.lawyer_name
                             , a.manager
                             , cat
                             , a.counsel_id
                             , a.kind
                             , safe_cast(1/array_length(split(a.context_additional, ',')) as numeric) as counsel_cnt
                             , safe_cast(a.origin_fee * (1/array_length(split(a.context_additional, ','))) as numeric) as counsel_price
                          from `lawtalk-bigquery.mart.lt_r_user_pay_counsel` a
                             , unnest(split(a.context_additional, ',')) as cat
                         where a.b_date >='2022-06-16'
                           and a.b_date between date('{{next_ds}}')-11 and date('{{next_ds}}')
                           and date(a.counsel_exc_dt) between date('{{next_ds}}')-5 and date('{{next_ds}}')
                           and a.counsel_status = 'complete'
                      ) a
                  left join `lawtalk-bigquery.raw.adcategories` b
                    on a.cat = b.name
                  left join `lawtalk-bigquery.raw.adlocationgroups` c
                    on a.cat = c.name
                 group by a.b_date
                        , a.lawyer_id
                        , a.slug
                        , a.lawyer_name
                        , a.manager
                        , coalesce(b._id, c._id, 'N/A')
                        , a.cat
                union all
                -- 변호사가 입력한 분야
                select date(a.counsel_exc_dt) as b_date
                     , a.lawyer_id
                     , a.slug
                     , a.lawyer_name
                     , a.manager
                     , 'category' as cat_mapping_standard
                     , a.category_id
                     , a.category_name
                     , sum(case when a.kind='phone' then 1 end) as phone_cnt
                     , sum(case when a.kind='phone' then a.origin_fee end) as phone_price
                     , sum(case when a.kind='video' then 1 end) as video_cnt
                     , sum(case when a.kind='video' then a.origin_fee end) as video_price
                     , sum(case when a.kind='visiting' then 1 end) as visiting_cnt
                     , sum(case when a.kind='visiting' then a.origin_fee end) as visiting_price
                  from `lawtalk-bigquery.mart.lt_r_user_pay_counsel` a
                 where a.b_date >='2022-06-16'
                   and a.b_date between date('{{next_ds}}')-11 and date('{{next_ds}}')
                   and date(a.counsel_exc_dt) between date('{{next_ds}}')-5 and date('{{next_ds}}')
                   and a.counsel_status = 'complete'
                 group by date(a.counsel_exc_dt)
                        , a.lawyer_id
                        , a.slug
                        , a.lawyer_name
                        , a.manager
                        , a.category_id
                        , a.category_name
                union all
                -- 분야기준 혼합 : 의뢰인 입력 키워드 기반의 분야가 N/A면 상담결과지의 분야로 매핑
                select a.b_date
                     , a.lawyer_id
                     , a.slug
                     , a.lawyer_name
                     , a.manager
                     , 'complex' as cat_mapping_standard
                     , coalesce(b._id, c._id, 'N/A') as category_id
                     , a.cat as category_name
                     , sum(case when a.kind='phone' then a.counsel_cnt end) as phone_cnt
                     , sum(case when a.kind='phone' then a.counsel_price end) as phone_price
                     , sum(case when a.kind='video' then a.counsel_cnt end) as video_cnt
                     , sum(case when a.kind='video' then a.counsel_price end) as video_price
                     , sum(case when a.kind='visiting' then a.counsel_cnt end) as visiting_cnt
                     , sum(case when a.kind='visiting' then a.counsel_price end) as visiting_price
                  from
                      (
                        select date(a.counsel_exc_dt) as b_date
                             , a.lawyer_id
                             , a.slug
                             , a.lawyer_name
                             , a.manager
                             , cat
                             , a.counsel_id
                             , a.kind
                             , safe_cast(1/array_length(split((case when a.context_additional='N/A' then a.category_name else a.context_additional end), ',')) as numeric) as counsel_cnt
                             , safe_cast(a.origin_fee * (1/array_length(split((case when a.context_additional='N/A' then a.category_name else a.context_additional end), ','))) as numeric) as counsel_price
                          from `lawtalk-bigquery.mart.lt_r_user_pay_counsel` a
                             , unnest(split((case when a.context_additional='N/A' then a.category_name else a.context_additional end), ',')) as cat
                         where a.b_date >='2022-06-16'
                           and a.b_date between date('{{next_ds}}')-11 and date('{{next_ds}}')
                           and date(a.counsel_exc_dt) between date('{{next_ds}}')-5 and date('{{next_ds}}')
                           and a.counsel_status = 'complete'
                      ) a
                  left join `lawtalk-bigquery.raw.adcategories` b
                    on a.cat = b.name
                  left join `lawtalk-bigquery.raw.adlocationgroups` c
                    on a.cat = c.name
                 group by a.b_date
                        , a.lawyer_id
                        , a.slug
                        , a.lawyer_name
                        , a.manager
                        , coalesce(b._id, c._id, 'N/A')
                        , a.cat
                '''
        )

        delete_lt_w_lawyer_counsel >> insert_lt_w_lawyer_counsel

    ########################################################
    #dataset: mart
    #table_name: lt_w_user_counsel
    #description: [로톡] 일자별 유저별 카테고리별 유료상담 현황
    #table_type: w단 일자별 집계
    #reprocessing date range: b_date기준 5일치 재처리(D-6 ~ D-1) : lt_r_user_pay_counsel.counsel_exc_dt기준으로 D+5까지 counsel_status가 업데이트 될 수 있으므로 5일치 재처리
    #modified history :
    #    20221117 - cat_mapping_standard=complex 값을 추가하여 의뢰인 입력 키워드 기반의 분야가 N/A면 상담결과지의 분야로 매핑하도록 기준 추가 by LJA
    ########################################################

    with TaskGroup(
        group_id="lt_w_user_counsel"
        ) as lt_w_user_counsel:

        delete_lt_w_user_counsel = BigQueryOperator(
            task_id = 'delete_lt_w_user_counsel',
            use_legacy_sql = False,
            sql = "delete from `lawtalk-bigquery.mart.lt_w_user_counsel` where b_date between date('{{next_ds}}')-5 and date('{{next_ds}}')"
        )

        insert_lt_w_user_counsel = BigQueryExecuteQueryOperator(
            task_id='insert_lt_w_user_counsel',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.lt_w_user_counsel',
            write_disposition = 'WRITE_APPEND',
            sql='''
                -- 유저가 입력한 키워드 기준의 분야 (키워드에 다중 카테고리가 매칭될 수 있음 그럴 경우 1/n 분배)
                select a.b_date
                     , a.user_id
                     , a.user_email
                     , a.user_name
                     , a.user_nickname
                     , 'keyword' as cat_mapping_standard
                     , coalesce(b._id, c._id, 'N/A') as category_id
                     , a.cat as category_name
                     , a.lawyer_id
                     , a.slug
                     , a.lawyer_name
                     , a.manager
                     , sum(case when a.kind='phone' then a.counsel_cnt end) as phone_cnt
                     , sum(case when a.kind='phone' then a.counsel_price end) as phone_price
                     , sum(case when a.kind='video' then a.counsel_cnt end) as video_cnt
                     , sum(case when a.kind='video' then a.counsel_price end) as video_price
                     , sum(case when a.kind='visiting' then a.counsel_cnt end) as visiting_cnt
                     , sum(case when a.kind='visiting' then a.counsel_price end) as visiting_price
                  from
                      (
                        select date(a.counsel_exc_dt) as b_date
                             , a.user_id
                             , a.user_email
                             , a.user_name
                             , a.user_nickname
                             , a.lawyer_id
                             , a.slug
                             , a.lawyer_name
                             , a.manager
                             , cat
                             , a.counsel_id
                             , a.kind
                             , safe_cast(1/array_length(split(a.context_additional, ',')) as numeric) as counsel_cnt
                             , safe_cast(a.origin_fee * (1/array_length(split(a.context_additional, ','))) as numeric) as counsel_price
                          from `lawtalk-bigquery.mart.lt_r_user_pay_counsel` a
                             , unnest(split(a.context_additional, ',')) as cat
                         where a.b_date >='2022-06-16'
                           and a.b_date between date('{{next_ds}}')-11 and date('{{next_ds}}')
                           and date(a.counsel_exc_dt) between date('{{next_ds}}')-5 and date('{{next_ds}}')
                           and a.counsel_status = 'complete'
                      ) a
                  left join `lawtalk-bigquery.raw.adcategories` b
                    on a.cat = b.name
                  left join `lawtalk-bigquery.raw.adlocationgroups` c
                    on a.cat = c.name
                 group by a.b_date
                        , a.user_id
                        , a.user_email
                        , a.user_name
                        , a.user_nickname
                        , a.lawyer_id
                        , a.slug
                        , a.lawyer_name
                        , a.manager
                        , coalesce(b._id, c._id, 'N/A')
                        , a.cat
                union all
                -- 변호사가 입력한 분야
                select date(a.counsel_exc_dt) as b_date
                     , a.user_id
                     , a.user_email
                     , a.user_name
                     , a.user_nickname
                     , 'category' as cat_mapping_standard
                     , a.category_id
                     , a.category_name
                     , a.lawyer_id
                     , a.slug
                     , a.lawyer_name
                     , a.manager
                     , sum(case when a.kind='phone' then 1 end) as phone_cnt
                     , sum(case when a.kind='phone' then a.origin_fee end) as phone_price
                     , sum(case when a.kind='video' then 1 end) as video_cnt
                     , sum(case when a.kind='video' then a.origin_fee end) as video_price
                     , sum(case when a.kind='visiting' then 1 end) as visiting_cnt
                     , sum(case when a.kind='visiting' then a.origin_fee end) as visiting_price
                  from `lawtalk-bigquery.mart.lt_r_user_pay_counsel` a
                 where a.b_date >='2022-06-16'
                   and a.b_date between date('{{next_ds}}')-11 and date('{{next_ds}}')
                   and date(a.counsel_exc_dt) between date('{{next_ds}}')-5 and date('{{next_ds}}')
                   and a.counsel_status = 'complete'
                 group by date(a.counsel_exc_dt)
                        , a.user_id
                        , a.user_email
                        , a.user_name
                        , a.user_nickname
                        , a.lawyer_id
                        , a.slug
                        , a.lawyer_name
                        , a.manager
                        , a.category_id
                        , a.category_name
                 union all
                 -- 분야기준 혼합 : 의뢰인 입력 키워드 기반의 분야가 N/A면 상담결과지의 분야로 매핑
                 select a.b_date
                      , a.user_id
                      , a.user_email
                      , a.user_name
                      , a.user_nickname
                      , 'complex' as cat_mapping_standard
                      , coalesce(b._id, c._id, 'N/A') as category_id
                      , a.cat as category_name
                      , a.lawyer_id
                      , a.slug
                      , a.lawyer_name
                      , a.manager
                      , sum(case when a.kind='phone' then a.counsel_cnt end) as phone_cnt
                      , sum(case when a.kind='phone' then a.counsel_price end) as phone_price
                      , sum(case when a.kind='video' then a.counsel_cnt end) as video_cnt
                      , sum(case when a.kind='video' then a.counsel_price end) as video_price
                      , sum(case when a.kind='visiting' then a.counsel_cnt end) as visiting_cnt
                      , sum(case when a.kind='visiting' then a.counsel_price end) as visiting_price
                   from
                       (
                         select date(a.counsel_exc_dt) as b_date
                              , a.user_id
                              , a.user_email
                              , a.user_name
                              , a.user_nickname
                              , a.lawyer_id
                              , a.slug
                              , a.lawyer_name
                              , a.manager
                              , cat
                              , a.counsel_id
                              , a.kind
                              , safe_cast(1/array_length(split((case when a.context_additional='N/A' then a.category_name else a.context_additional end), ',')) as numeric) as counsel_cnt
                              , safe_cast(a.origin_fee * (1/array_length(split((case when a.context_additional='N/A' then a.category_name else a.context_additional end), ','))) as numeric) as counsel_price
                           from `lawtalk-bigquery.mart.lt_r_user_pay_counsel` a
                              , unnest(split((case when a.context_additional='N/A' then a.category_name else a.context_additional end), ',')) as cat
                          where a.b_date >='2022-06-16'
                            and a.b_date between date('{{next_ds}}')-11 and date('{{next_ds}}')
                            and date(a.counsel_exc_dt) between date('{{next_ds}}')-5 and date('{{next_ds}}')
                            and a.counsel_status = 'complete'
                       ) a
                   left join `lawtalk-bigquery.raw.adcategories` b
                     on a.cat = b.name
                   left join `lawtalk-bigquery.raw.adlocationgroups` c
                     on a.cat = c.name
                  group by a.b_date
                         , a.user_id
                         , a.user_email
                         , a.user_name
                         , a.user_nickname
                         , a.lawyer_id
                         , a.slug
                         , a.lawyer_name
                         , a.manager
                         , coalesce(b._id, c._id, 'N/A')
                         , a.cat
                '''
        )

        delete_lt_w_user_counsel >> insert_lt_w_user_counsel


    ########################################################
    #dataset: mart
    #table_name: lt_r_lawyer_slot
    #description: [로톡] 변호사의 상담 슬롯 오픈과 유료 상담 여부
    #table_type: raw data
    #reprocessing date range: b_date기준 12일치 재처리(D-12 ~ D-1) : 슬롯 오픈 시 해당일자를 포함하여 D+7까지로 상담일자를 설정할 수 있고 상담일로부터 D+5까지 상담결과지 및 후기 작성이 가능하여 D+12까지 데이터 변경될 가능성 있음
    ########################################################

    with TaskGroup(
    group_id="lt_r_lawyer_slot"
    ) as lt_r_lawyer_slot:

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
                    b_date
                    ,lawyer_id
                    ,slug
                    ,lawyer_name
                    ,manager
                FROM `lawtalk-bigquery.mart.lt_s_lawyer_info`
                WHERE b_date BETWEEN date('{{next_ds}}') - 11 AND date('{{next_ds}}')
                )

                , BASE AS (
                SELECT
                lawyer
                ,dayString
                ,times
                ,DATETIME(TIMESTAMP(createdAt),'Asia/Seoul') slot_crt_dt
                ,ROW_NUMBER() OVER (PARTITION BY lawyer, daystring ORDER BY DATETIME(TIMESTAMP(createdAt),'Asia/Seoul') DESC) rn
                FROM `lawtalk-bigquery.lt_src.adviceschedules`
                WHERE DATE(daystring) BETWEEN date('{{next_ds}}') -5 and date('{{next_ds}}') +7
                QUALIFY rn = 1 
                )

                SELECT
                DATE(slot_opened_dt) as b_date
                ,t_slot.lawyer lawyer_id
                ,slug
                ,lawyer_name name
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
                ,t_advice.counsel_id
                ,counsel_crt_dt
                ,counsel_status
                FROM (
                SELECT
                lawyer
                ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(phone_time_slot AS INT) * 30 MINUTE) slot_opened_dt
                ,'phone' as kind
                ,slot_crt_dt
                FROM BASE, UNNEST(JSON_VALUE_ARRAY(times,'$.phone')) phone_time_slot

                UNION ALL

                SELECT
                    lawyer
                    ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(video_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
                    ,'video' as kind
                    ,slot_crt_dt
                FROM BASE, UNNEST(JSON_VALUE_ARRAY(times,'$.video')) video_time_slot

                UNION ALL

                SELECT
                    lawyer
                    ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(visiting_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
                    ,'visiting' as kind
                    ,slot_crt_dt
                FROM BASE, UNNEST(JSON_VALUE_ARRAY(times,'$.visiting')) visiting_time_slot

                ) t_slot LEFT JOIN (SELECT
                                        counsel_exc_dt
                                        ,counsel_crt_dt
                                        ,kind
                                        ,lawyer_id
                                        ,counsel_status
                                        ,counsel_id
                                    FROM `lawtalk-bigquery.mart.lt_r_user_pay_counsel`
                                    WHERE DATE(counsel_exc_dt) BETWEEN date('{{next_ds}}') -5 and date('{{next_ds}}')
                                    AND counsel_status != 'reserved') t_advice
                                ON t_slot.lawyer = t_advice.lawyer_id
                                AND t_slot.slot_opened_dt = t_advice.counsel_exc_dt
                                AND t_slot.kind = t_advice.kind
                        LEFT JOIN t_lawyer ON t_slot.lawyer = t_lawyer.lawyer_id
                                            AND DATE(t_slot.slot_crt_dt) = t_lawyer.b_date
                    '''
        )

        delete_lt_r_lawyer_slot >> insert_lt_r_lawyer_slot


    ########################################################
    #dataset: mart
    #table_name: lt_w_lawyer_slot
    #description: [로톡] 일자별 변호사별 슬롯 오픈 및 예약 현황
    #table_type: w단 일자별 집계
    #reprocessing date range: b_date기준 12일치 재처리(D-12 ~ D-1) : 슬롯 오픈 시 해당일자를 포함하여 D+7까지로 상담일자를 설정할 수 있고 상담일로부터 D+5까지 상담결과지 및 후기 작성이 가능하여 D+12까지 데이터 변경될 가능성 있음
    ########################################################

    with TaskGroup(
    group_id="lt_w_lawyer_slot"
    ) as lt_w_lawyer_slot:

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

        delete_lt_w_lawyer_slot >> insert_lt_w_lawyer_slot


    ########################################################
    #dataset: mart
    #table_name: lt_s_user_info
    #description: [로톡] 유저 정보 (의뢰인, 변호사, 변호사 승인대기)
    #table_type: s단 일자별 스냅샷
    #reprocessing date range: b_date 기준 1일치 처리 (해당일자 시점의 스냅샷 형태로 하루치만 처리하면 됨)
    ########################################################

    with TaskGroup(
    group_id="lt_s_user_info"
    ) as lt_s_user_info:

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
                    lawyer_id
                    ,slug
                    ,lawyer_name
                    ,manager
                    FROM `lawtalk-bigquery.mart.lt_s_lawyer_info`
                    WHERE b_date = date('{{next_ds}}')
                )
                -- , BASE AS (
                SELECT
                    date('{{next_ds}}') as b_date
                    ,CASE WHEN role = 'secession' THEN 'user' ELSE role END role
                    ,_id user_id
                    ,username user_nickname
                    ,CASE WHEN _id = '620a0996ee8c9876d5f62d6a' OR slug = '탈퇴한 변호사' OR role = 'secession' THEN '탈퇴'
                            WHEN _id = '620a0a07ee8c9876d5f671d8' OR slug = '미활동 변호사' THEN '미활동'
                            WHEN slug LIKE '%(탈퇴보류)' THEN '탈퇴 보류'
                            WHEN role = 'lawyer_waiting' THEN '승인 대기'
                            ELSE '활동'
                    END user_status
                    ,email user_email
                    ,CASE isNonMember WHEN TRUE THEN 1 ELSE 0 END is_non_member
                    ,CAST(EXTRACT(year FROM birth) as numeric) birth_year
                    ,CAST(EXTRACT(year FROM date('{{next_ds}}')) - EXTRACT(year FROM birth) + 1 as numeric) korean_age
                    ,sex
                    ,SAFE_CAST(countryCode as numeric) country_code
                    ,DATETIME(TIMESTAMP(createdAt),'Asia/Seoul') crt_dt
                    ,DATETIME(TIMESTAMP(updatedAt),'Asia/Seoul') upd_dt
                    ,CASE isEmailAccept WHEN True THEN 1 ELSE 0 END is_email_accept
                    ,CASE JSON_VALUE(emailMarketingAccept[ARRAY_LENGTH(JSON_EXTRACT_ARRAY(emailMarketingAccept)) - 1], '$.status') WHEN 'true' THEN 1 ELSE 0 END is_email_marketing_accept
                    -- ,JSON_VALUE(emailMarketingAccept[ARRAY_LENGTH(JSON_EXTRACT_ARRAY(emailMarketingAccept)) - 1], '$.startDate') email_marketing_accept_start_date
                    ,DATETIME(TIMESTAMP(JSON_VALUE(emailMarketingAccept[ARRAY_LENGTH(JSON_EXTRACT_ARRAY(emailMarketingAccept)) - 1], '$.endDate')),'Asia/Seoul') email_marketing_accept_end_date
                    ,CASE isSMSAccept WHEN True THEN 1 ELSE 0 END is_sms_accept
                    ,CASE JSON_VALUE(smsMarketingAccept[ARRAY_LENGTH(JSON_EXTRACT_ARRAY(smsMarketingAccept)) - 1], '$.status') WHEN 'true' THEN 1 ELSE 0 END is_sms_marketing_accept
                    -- ,JSON_VALUE(smsMarketingAccept[ARRAY_LENGTH(JSON_EXTRACT_ARRAY(smsMarketingAccept)) - 1], '$.startDate') sms_marketing_accept_start_date
                    ,DATETIME(TIMESTAMP(JSON_VALUE(smsMarketingAccept[ARRAY_LENGTH(JSON_EXTRACT_ARRAY(smsMarketingAccept)) - 1], '$.endDate')),'Asia/Seoul') sms_marketing_accept_end_date
                    ,provider
                    ,referrer
                    ,referrerOther referrer_other
                    ,recommender recommender_id
                    ,CAST(null AS string) recommender_name
                    ,CASE reviewCouponCheck WHEN TRUE THEN 1
                                            WHEN FALSE THEN 0
                                            ELSE null
                    END is_review_coupon
                    ,JSON_VALUE(utm,'$.utm_source') utm_source
                    ,JSON_VALUE(utm,'$.utm_medium') utm_medium
                    ,JSON_VALUE(utm,'$.utm_campaign') utm_campaign
                    ,JSON_VALUE(utm,'$.utm_content') utm_content
                    ,JSON_VALUE(utm,'$.utm_term') utm_term
                    ,lawyer_id
                FROM `lt_src.users` LEFT JOIN t_lawyer ON `lt_src.users`.lawyer = t_lawyer.lawyer_id
                '''
        )
        delete_lt_s_user_info >> insert_lt_s_user_info


    ########################################################
    #dataset: mart
    #table_name: lt_s_qna
    #description: [로톡] 상담 사례 질문과 답변
    #table_type: s단 일자별 스냅샷
    #reprocessing date range: b_date 기준 1일치 처리 (해당일자 시점의 스냅샷 형태로 하루치만 처리하면 됨)
    ########################################################

    with TaskGroup(
    group_id="lt_s_qna"
    ) as lt_s_qna:

        delete_lt_s_qna = BigQueryOperator(
            task_id = 'delete_lt_s_qna',
            use_legacy_sql = False,
            sql = "delete from `lawtalk-bigquery.mart.lt_s_qna` where b_date = date('{{next_ds}}')"
        )

        insert_lt_s_qna = BigQueryExecuteQueryOperator(
            task_id='insert_lt_s_qna',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.lt_s_qna',
            write_disposition = 'WRITE_APPEND',
            sql='''
                WITH t_question_cat AS (
                SELECT
                    question_id
                    ,ARRAY_AGG(category_id) category_id
                    ,ARRAY_AGG(name) category_name
                FROM (
                SELECT
                    `lt_src.questions`._id question_id
                    ,JSON_VALUE(category_id) category_id
                    ,name
                FROM `lt_src.questions`, UNNEST(JSON_QUERY_ARRAY(categories)) category_id
                                        LEFT JOIN `lt_src.adcategories` ON JSON_VALUE(category_id) = `lt_src.adcategories`._id
                )
                WHERE category_id IS NOT NULL
                GROUP BY 1
                )

                , t_favorites_user AS (
                SELECT
                    DISTINCT _id question_id
                    ,MAX(favorites_offset) OVER (PARTITION BY _id) + 1 favorites_user_cnt
                FROM `lt_src.questions`, UNNEST(JSON_VALUE_ARRAY(favorites)) favorites WITH OFFSET favorites_offset
                )

                , t_users AS (
                SELECT
                    user_id
                    ,user_nickname
                    ,user_status
                    ,user_email
                FROM `lawtalk-bigquery.mart.lt_s_user_info` WHERE b_date = "{{next_ds}}"
                )

                , t_question_users AS (
                SELECT
                    DATE("{{next_ds}}") as b_date
                    ,_id question_id
                    ,number question_number
                    ,title question_title
                    ,DATETIME(TIMESTAMP(`lt_src.questions`.createdAt),'Asia/Seoul') question_crt_dt
                    ,DATETIME(TIMESTAMP(`lt_src.questions`.updatedAt),'Asia/Seoul') question_upd_dt
                    ,CAST(null AS int) is_kin_question
                    ,viewCount acc_view_cnt
                    ,user user_id
                    ,user_nickname
                    ,user_status
                    ,user_email
                    ,isDirectPublished
                    ,expectPublishedAt
                FROM `lt_src.questions` LEFT JOIN t_users ON `lt_src.questions`.user = t_users.user_id
                )

                , t_question_base AS (
                SELECT
                    b_date
                    ,question_id
                    ,question_number
                    ,question_title
                    ,question_crt_dt
                    ,question_upd_dt
                    ,category_id
                    ,category_name
                    ,is_kin_question
                    ,IFNULL(favorites_user_cnt,0) favorites_user_cnt
                    ,acc_view_cnt
                    ,user_id
                    ,user_nickname
                    ,user_status
                    ,user_email
                    ,isDirectPublished
                    ,expectPublishedAt
                FROM t_question_users LEFT JOIN t_question_cat USING (question_id)
                                    LEFT JOIN t_favorites_user USING (question_id)
                )

                , t_answer_base AS (
                SELECT
                    _id answer_id
                    ,question question_id
                    ,number answer_number
                    ,DATETIME(TIMESTAMP(createdAt),'Asia/Seoul') answer_crt_dt
                    ,DATETIME(TIMESTAMP(updatedAt),'Asia/Seoul') answer_upd_dt
                    ,CAST(null AS int) is_kin_answer
                    ,CASE exportable WHEN TRUE THEN 1 ELSE 0 END is_kin_answer_exportable
                    ,CASE isAdopted WHEN TRUE THEN 1 ELSE 0 END is_adopted
                    ,CASE JSON_VALUE(blindInfo, '$.blindStatus') WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE 0 END is_blind_answer
                    ,lawyer lawyer_id
                    ,linfo.slug
                    ,lawyer_name
                    ,manager
                FROM `lt_src.answers` LEFT JOIN (SELECT
                                                lawyer_id
                                                ,slug
                                                ,lawyer_name
                                                ,manager
                                                FROM `mart.lt_s_lawyer_info`
                                                WHERE b_date = "{{next_ds}}") linfo
                                        ON `lt_src.answers`.lawyer = linfo.lawyer_id
                )

                SELECT
                    b_date
                    ,question_id
                    ,CAST(question_number as numeric) question_number
                    ,question_title
                    ,question_crt_dt
                    ,question_upd_dt
                    ,category_id
                    ,category_name
                    ,is_kin_question
                    ,CAST(favorites_user_cnt as numeric) favorites_user_cnt
                    ,CAST(acc_view_cnt as numeric) acc_view_cnt
                    ,user_id
                    ,user_nickname
                    ,user_status
                    ,user_email
                    ,CASE WHEN answer_id IS NULL THEN 0 ELSE 1 END is_answered
                    ,MIN(answer_crt_dt) OVER (PARTITION BY question_id) first_answer_crt_dt
                    ,MAX(answer_crt_dt) OVER (PARTITION BY question_id) recent_answer_crt_dt
                    ,answer_id
                    ,CAST(answer_number as numeric) answer_number
                    ,answer_crt_dt
                    ,answer_upd_dt
                    ,is_kin_answer
                    ,is_kin_answer_exportable
                    ,is_adopted
                    ,is_blind_answer
                    ,lawyer_id
                    ,slug lawyer_slug
                    ,lawyer_name
                    ,manager
                    ,CASE isDirectPublished WHEN TRUE THEN 1
                                            WHEN FALSE THEN 0
                    END is_direct_published
                    ,DATETIME(TIMESTAMP(expectPublishedAt),'Asia/Seoul') expect_published_dt
                FROM t_question_base LEFT JOIN t_answer_base USING (question_id)
                '''
        )

        delete_lt_s_qna >> insert_lt_s_qna


start >> lt_r_lawyer_ad_sales >> lt_s_lawyer_ads >> lt_s_lawyer_info >> lt_s_user_info >> lt_s_qna
start >> lt_r_user_pay_counsel >> lt_w_lawyer_counsel >> lt_s_lawyer_info
lt_s_lawyer_info >> lt_r_lawyer_slot >> lt_w_lawyer_slot
lt_r_user_pay_counsel >> lt_w_user_counsel
