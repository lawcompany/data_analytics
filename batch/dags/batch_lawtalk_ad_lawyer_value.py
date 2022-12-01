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
    dag_id="batch_lawtalk_ad_lawyer_value",
    description ="lawtalk value of ad lawyer",
    start_date = datetime(2022, 11, 26, tzinfo = KST),
    schedule_interval = '0 7 * * *',
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
    #table_name: lt_s_ad_lawyer_value
    #description: [로톡] 광고변호사 활동성 및 만족도, 가치
    #table_type: s단 일자별 스냅샷
    #reprocessing date range: b_date 기준 1일치 처리 (해당일자 시점의 스냅샷 형태로 하루치만 처리하면 됨)
    ########################################################

    with TaskGroup(
        group_id="lt_s_ad_lawyer_value"
    ) as lt_s_ad_lawyer_value:

        delete_lt_s_ad_lawyer_value = BigQueryOperator(
            task_id = 'delete_lt_s_ad_lawyer_value',
            use_legacy_sql = False,
            sql = "delete from `lawtalk-bigquery.mart.lt_s_ad_lawyer_value` where b_date = date('{{next_ds}}') and date('{{next_ds}}') = date(current_timestamp,'Asia/Seoul')-1"
        )

        insert_lt_s_ad_lawyer_value = BigQueryExecuteQueryOperator(
            task_id='insert_lt_s_ad_lawyer_value',
            use_legacy_sql = False,
            destination_dataset_table='lawtalk-bigquery.mart.lt_s_ad_lawyer_value',
            write_disposition = 'WRITE_APPEND',
            sql='''
                with main_info as
                (
                  select lawyer_id
                      , lawyer_name
                      , slug
                      , manager
                      , paid_kind
                      , category_ad_cnt
                      , location_ad_cnt
                      , plus_ad_cnt
                      , category_ad_cnt + location_ad_cnt + plus_ad_cnt as ad_cnt
                  from `mart.lt_s_lawyer_info`
                  where b_date = date('{{next_ds}}')
                    and is_resting = 0
                    and is_approved = 1
                    and is_opened = 1
                    and is_ad = 1
                )
                , profile as
                (
                  select slug
                       , sum(case when date_cat='3months' then imp_cart_cnt end) _3months_imp_cart_cnt
                       , sum(case when date_cat='3months' then click_card_cnt end) _3months_click_card_cnt
                       , sum(case when date_cat='3months' then click_qna_cnt end) _3months_click_qna_cnt
                       , sum(case when date_cat='3months' then click_posts_cnt end) _3months_click_posts_cnt
                       , sum(case when date_cat='3months' then click_video_cnt end) _3months_click_video_cnt
                       , sum(case when date_cat='3months' then tot_view_cnt end) _3months_tot_view_cnt
                       , sum(case when date_cat='ads' then imp_cart_cnt end) ads_imp_cart_cnt
                       , sum(case when date_cat='ads' then click_card_cnt end) ads_click_card_cnt
                       , sum(case when date_cat='ads' then click_qna_cnt end) ads_click_qna_cnt
                       , sum(case when date_cat='ads' then click_posts_cnt end) ads_click_posts_cnt
                       , sum(case when date_cat='ads' then click_video_cnt end) ads_click_video_cnt
                       , sum(case when date_cat='ads' then tot_view_cnt end) ads_tot_view_cnt
                       , sum(case when date_cat='bef_ads' then imp_cart_cnt end) bef_ads_imp_cart_cnt
                       , sum(case when date_cat='bef_ads' then click_card_cnt end) bef_ads_click_card_cnt
                       , sum(case when date_cat='bef_ads' then click_qna_cnt end) bef_ads_click_qna_cnt
                       , sum(case when date_cat='bef_ads' then click_posts_cnt end) bef_ads_click_posts_cnt
                       , sum(case when date_cat='bef_ads' then click_video_cnt end) bef_ads_click_video_cnt
                       , sum(case when date_cat='bef_ads' then tot_view_cnt end) bef_ads_tot_view_cnt
                    from
                    (
                      select '3months' as date_cat
                          , slug
                          , sum(coalesce(case when type = 'imp_card' then cnt end,0)) as imp_cart_cnt
                          , sum(coalesce(case when type = 'click_card' then cnt end,0)) as click_card_cnt
                          , sum(coalesce(case when type = 'click_qna' then cnt end,0)) as click_qna_cnt
                          , sum(coalesce(case when type = 'click_posts' then cnt end,0)) as click_posts_cnt
                          , sum(coalesce(case when type = 'click_video' then cnt end,0)) as click_video_cnt
                          , sum(coalesce(case when type = 'tot_view' then cnt end,0)) as tot_view_cnt
                        from `mart.lt_r_lawyer_amplitude_performance`
                      where b_date between date_sub(date('{{next_ds}}')+1, interval 3 month) and (date('{{next_ds}}'))
                      group by 1,2
                      union all
                      select 'ads' as date_cat
                          , slug
                          , sum(coalesce(case when type = 'imp_card' then cnt end,0)) as imp_cart_cnt
                          , sum(coalesce(case when type = 'click_card' then cnt end,0)) as click_card_cnt
                          , sum(coalesce(case when type = 'click_qna' then cnt end,0)) as click_qna_cnt
                          , sum(coalesce(case when type = 'click_posts' then cnt end,0)) as click_posts_cnt
                          , sum(coalesce(case when type = 'click_video' then cnt end,0)) as click_video_cnt
                          , sum(coalesce(case when type = 'tot_view' then cnt end,0)) as tot_view_cnt
                        from `mart.lt_r_lawyer_amplitude_performance`
                      where b_date BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                                                  else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                            end)
                                    AND (date('{{next_ds}}'))
                      group by 1,2
                      union all
                      select 'bef_ads' as date_cat
                          , slug
                          , sum(coalesce(case when type = 'imp_card' then cnt end,0)) as imp_cart_cnt
                          , sum(coalesce(case when type = 'click_card' then cnt end,0)) as click_card_cnt
                          , sum(coalesce(case when type = 'click_qna' then cnt end,0)) as click_qna_cnt
                          , sum(coalesce(case when type = 'click_posts' then cnt end,0)) as click_posts_cnt
                          , sum(coalesce(case when type = 'click_video' then cnt end,0)) as click_video_cnt
                          , sum(coalesce(case when type = 'tot_view' then cnt end,0)) as tot_view_cnt
                        from `mart.lt_r_lawyer_amplitude_performance`
                      where b_date BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                                 else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                                            end)
                                    AND (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-15')
                                              else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')-1
                                         end)
                      group by 1,2
                    ) a
                 group by 1
                )
                , posts as
                (
                  select a.lawyer_id
                       , sum(case when date_cat='3months' then posts_cnt end) _3months_posts_cnt
                       , sum(case when date_cat='ads' then posts_cnt end) ads_posts_cnt
                       , sum(case when date_cat='bef_ads' then posts_cnt end) bef_ads_posts_cnt
                    from
                    (
                        select '3months' as date_cat
                            , lawyer as lawyer_id
                            , count(distinct _id) as posts_cnt
                          from `raw.posts`
                        where date(createdAt,'Asia/Seoul') between date_sub(date('{{next_ds}}')+1, interval 3 month) and (date('{{next_ds}}'))
                        group by 1,2
                        union all
                        select 'ads' as date_cat
                            , lawyer as lawyer_id
                            , count(distinct _id) as posts_cnt
                          from `raw.posts`
                        where date(createdAt,'Asia/Seoul')
                              BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                                            else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                        end)
                              AND (date('{{next_ds}}'))
                        group by 1,2
                        union all
                        select 'bef_ads' as date_cat
                            , lawyer as lawyer_id
                            , count(distinct _id) as posts_cnt
                          from `raw.posts`
                        where date(createdAt,'Asia/Seoul')
                              BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                            else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                                      end)
                              AND (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-15')
                                        else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')-1
                                    end)
                        group by 1,2
                    ) a
                   group by 1
                )
                , qna as
                (
                  select a.lawyer_id
                       , sum(case when date_cat='3months' then qna_cnt end) _3months_qna_cnt
                       , sum(case when date_cat='ads' then qna_cnt end) ads_qna_cnt
                       , sum(case when date_cat='bef_ads' then qna_cnt end) bef_ads_qna_cnt
                    from
                    (
                      select '3months' as date_cat
                          , lawyer as lawyer_id
                          , count(distinct _id) as qna_cnt
                        from `raw.answers`
                      where date(createdAt,'Asia/Seoul') between date_sub(date('{{next_ds}}')+1, interval 3 month) and (date('{{next_ds}}'))
                      group by 1,2
                      union all
                      select 'ads' as date_cat
                          , lawyer as lawyer_id
                          , count(distinct _id) as qna_cnt
                        from `raw.answers`
                      where date(createdAt,'Asia/Seoul')
                            BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                                          else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                      end)
                            AND (date('{{next_ds}}'))
                      group by 1,2
                      union all
                      select 'bef_ads' as date_cat
                          , lawyer as lawyer_id
                          , count(distinct _id) as qna_cnt
                        from `raw.answers`
                      where date(createdAt,'Asia/Seoul')
                            BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                          else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                                    end)
                            AND (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-15')
                                      else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')-1
                                  end)
                      group by 1,2
                    ) a
                   group by 1
                )
                , video as
                (
                  select a.lawyer_id
                       , sum(case when date_cat='3months' then video_cnt end) _3months_video_cnt
                       , sum(case when date_cat='ads' then video_cnt end) ads_video_cnt
                       , sum(case when date_cat='bef_ads' then video_cnt end) bef_ads_video_cnt
                    from
                    (
                      select '3months' as date_cat
                          , lawyer as lawyer_id
                          , count(distinct _id) as video_cnt
                        from `raw.videos`
                      where date(createdAt,'Asia/Seoul') between date_sub(date('{{next_ds}}')+1, interval 3 month) and (date('{{next_ds}}'))
                      group by 1,2
                      union all
                      select 'ads' as date_cat
                          , lawyer as lawyer_id
                          , count(distinct _id) as video_cnt
                        from `raw.videos`
                      where date(createdAt,'Asia/Seoul')
                            BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                                          else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                      end)
                            AND (date('{{next_ds}}'))
                      group by 1,2
                      union all
                      select 'bef_ads' as date_cat
                          , lawyer as lawyer_id
                          , count(distinct _id) as video_cnt
                        from `raw.videos`
                      where date(createdAt,'Asia/Seoul')
                            BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                          else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                                    end)
                            AND (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-15')
                                      else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')-1
                                  end)
                      group by 1,2
                    ) a
                   group by 1
                )
                , r_counsel as
                (
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
                            where a.b_date >= date_sub(date('{{next_ds}}'), interval 4 month)
                              and date(a.counsel_exc_dt) BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 2 month))||'-01')
                                                                       else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 3 month))||'-16')
                                                                  end)
                                              AND (date('{{next_ds}}'))
                              and a.counsel_status in ('reservation','complete')
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
                )
                , counsel as
                (
                  select a.lawyer_id
                       , sum(case when date_cat='3months' then counsel_cnt end) _3months_counsel_cnt
                       , sum(case when date_cat='3months' then counsel_price end) _3months_counsel_price
                       , sum(case when date_cat='3months' then predict_price end) _3months_predict_price
                       , sum(case when date_cat='ads' then counsel_cnt end) ads_counsel_cnt
                       , sum(case when date_cat='ads' then counsel_price end) ads_counsel_price
                       , sum(case when date_cat='ads' then predict_price end) ads_predict_price
                       , sum(case when date_cat='bef_ads' then counsel_cnt end) bef_ads_counsel_cnt
                       , sum(case when date_cat='bef_ads' then counsel_price end) bef_ads_counsel_price
                       , sum(case when date_cat='bef_ads' then predict_price end) bef_ads_predict_price
                    from
                    (
                        select date_cat
                            , lawyer_id
                            , slug
                            , lawyer_name
                            , manager
                            , round(sum(counsel_cnt)) as counsel_cnt
                            , round(sum(counsel_price)) as counsel_price
                            , sum(counsel_cnt*coalesce(b.conv_rate,0)*coalesce(b.avg_price,0)) as predict_price
                          from (
                                select '3months' as date_cat
                                    , lawyer_id
                                    , slug
                                    , lawyer_name
                                    , manager
                                    , category_id
                                    , category_name
                                    , sum(coalesce(phone_cnt,0)) + sum(coalesce(video_cnt,0)) + sum(coalesce(visiting_cnt,0)) as counsel_cnt
                                    , sum(coalesce(phone_price,0)) + sum(coalesce(video_price,0)) + sum(coalesce(visiting_price,0)) as counsel_price
                                  from r_counsel
                                where b_date BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 2 month))||'-01')
                                                          else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 3 month))||'-16')
                                                      end)
                                              AND (date('{{next_ds}}'))
                                  and cat_mapping_standard = 'keyword'
                                group by 1,2,3,4,5,6,7
                                union all
                                select 'ads' as date_cat
                                    , lawyer_id
                                    , slug
                                    , lawyer_name
                                    , manager
                                    , category_id
                                    , category_name
                                    , sum(coalesce(phone_cnt,0)) + sum(coalesce(video_cnt,0)) + sum(coalesce(visiting_cnt,0)) as counsel_cnt
                                    , sum(coalesce(phone_price,0)) + sum(coalesce(video_price,0)) + sum(coalesce(visiting_price,0)) as counsel_price
                                  from r_counsel
                                where b_date BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                                                            else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                                      end)
                                              AND (date('{{next_ds}}'))
                                  and cat_mapping_standard = 'keyword'
                                group by 1,2,3,4,5,6,7
                                union all
                                select 'bef_ads' as date_cat
                                    , lawyer_id
                                    , slug
                                    , lawyer_name
                                    , manager
                                    , category_id
                                    , category_name
                                    , sum(coalesce(phone_cnt,0)) + sum(coalesce(video_cnt,0)) + sum(coalesce(visiting_cnt,0)) as counsel_cnt
                                    , sum(coalesce(phone_price,0)) + sum(coalesce(video_price,0)) + sum(coalesce(visiting_price,0)) as counsel_price
                                  from r_counsel
                                where b_date
                                      BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                                    else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                                              end)
                                      AND (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-15')
                                                else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')-1
                                            end)
                                  and cat_mapping_standard = 'keyword'
                                group by 1,2,3,4,5,6,7
                              ) a
                          left join `mart.lt_s_category_predictive_value` b
                            on a.category_name = b.category
                        group by 1,2,3,4,5
                    ) a
                   group by 1
                )
                , ad_date as
                (
                  select 1 as num
                      , (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 2 month))||'-01')
                              else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 3 month))||'-16')
                          end) as b_date
                  union all
                  select 2 as num
                      , (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 2 month))||'-16')
                              else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 2 month))||'-01')
                          end) as b_date
                  union all
                  select 3 as num
                      , (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-01')
                              else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 2 month))||'-16')
                          end) as b_date
                  union all
                  select 4 as num
                      , (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                              else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-01')
                          end) as b_date
                  union all
                  select 5 as num
                      , (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                              else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                          end) as b_date
                  union all
                  select 6 as num
                      , (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                              else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                          end) as b_date
                )
                , ad_sale as
                (
                 select a.lawyer_id
                      , sum(case when date_cat='3months' then tot_fee end) _3months_tot_fee
                      , sum(case when date_cat='3months' then ad_round_cnt end) _3months_ad_round_cnt
                      , sum(case when date_cat='ads' then tot_fee end) ads_tot_fee
                      , sum(case when date_cat='ads' then ad_round_cnt end) ads_ad_round_cnt
                      , sum(case when date_cat='bef_ads' then tot_fee end) bef_ads_tot_fee
                      , sum(case when date_cat='bef_ads' then ad_round_cnt end) bef_ads_ad_round_cnt
                    from
                    (
                      select '3months' as date_cat
                          , a.lawyer_id
                          , a.lawyer_name
                          , sum(a.tot_fixed_fee) as tot_fee
                          , max(a.ad_round_cnt) as ad_round_cnt
                        from (
                                select a.lawyer_id
                                    , a.lawyer_name
                                    , a._id
                                    , a.tot_fixed_fee
                                    , max(a.ad_round_cnt) as ad_round_cnt
                                  from (
                                        select a.lawyer_id
                                            , a.lawyer_name
                                            , coalesce(a.pay_id,a.order_id) as _id
                                            , a.tot_fixed_fee
                                            , count(distinct num) over(partition by a.lawyer_id) as ad_round_cnt
                                            , a.ad_start_dt
                                            , a.ad_end_dt
                                            , c.num
                                          from `mart.lt_r_lawyer_ad_sales` a
                                          inner join (select min(b_date) as min_date, max(b_date) as max_date from ad_date) b
                                            on b.min_date <=date(a.ad_end_dt)
                                          and b.max_date >=date(a.ad_start_dt)
                                          left join ad_date c
                                            on c.b_date between date(a.ad_start_dt) and date(a.ad_end_dt)
                                          where (a.kind = 'plus' or a.pay_status = 'paid')
                                      ) a
                                  group by 1,2,3,4
                              ) a
                        group by 1,2,3
                        union all
                        select 'ads' as date_cat
                              , a.lawyer_id
                              , a.lawyer_name
                              , sum(tot_fixed_fee) as tot_fee
                              , 1 as ad_round_cnt
                          from (
                                  select a.lawyer_id
                                        , a.lawyer_name
                                        , coalesce(a.pay_id,a.order_id) as _id
                                        , a.tot_fixed_fee
                                    from `mart.lt_r_lawyer_ad_sales` a
                                    where (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                                                  else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                            end)  between date(a.ad_start_dt) and date(a.ad_end_dt)
                                      and (a.kind = 'plus' or a.pay_status = 'paid')
                                    group by 1,2,3,4
                                ) a
                          group by 1,2,3,5
                          union all
                        select 'bef_ads' as date_cat
                              , a.lawyer_id
                              , a.lawyer_name
                              , sum(tot_fixed_fee) as tot_fee
                              , 1 as ad_round_cnt
                          from (
                                  select a.lawyer_id
                                        , a.lawyer_name
                                        , coalesce(a.pay_id,a.order_id) as _id
                                        , a.tot_fixed_fee
                                    from `mart.lt_r_lawyer_ad_sales` a
                                    where (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                                else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                                           end)  between date(a.ad_start_dt) and date(a.ad_end_dt)
                                      and (a.kind = 'plus' or a.pay_status = 'paid')
                                    group by 1,2,3,4
                                ) a
                          group by 1,2,3,5
                    ) a
                   group by 1
                )
                , slot_base AS (
                    SELECT
                        lawyer
                        ,daystring
                        ,NULLIF(phone_times,'') phone_times
                        ,NULLIF(video_times,'') video_times
                        ,NULLIF(visiting_times,'') visiting_times
                        ,DATETIME(createdAt,'Asia/Seoul') slot_crt_dt
                        ,ROW_NUMBER() OVER (PARTITION BY lawyer, daystring ORDER BY DATETIME(createdAt,'Asia/Seoul') DESC) rn
                    FROM `lawtalk-bigquery.raw.adviceschedules`
                    , UNNEST(REGEXP_EXTRACT_ALL(times,r"'phone': \[(.*?)\]")) phone_times
                    , UNNEST(REGEXP_EXTRACT_ALL(times,r"'video': \[(.*?)\]")) video_times
                    , UNNEST(REGEXP_EXTRACT_ALL(times,r"'visiting': \[(.*?)\]")) visiting_times
                    WHERE DATE(daystring) between date_sub(date('{{next_ds}}')+1, interval 3 month) and (date('{{next_ds}}'))
                    QUALIFY rn = 1
                )
                , slot_summary as (
                SELECT
                    DATE(slot_opened_dt) as b_date
                    ,t_slot.lawyer lawyer_id
                    ,slot_crt_dt
                    ,slot_opened_dt
                    ,EXTRACT(DATE FROM slot_opened_dt) slot_opened_date
                    ,FORMAT_DATETIME('%R', slot_opened_dt) slot_opened_time
                    ,case when FORMAT_DATETIME('%R', slot_opened_dt) between '08:00' and '12:59' then '오전'
                          when FORMAT_DATETIME('%R', slot_opened_dt) between '13:00' and '18:59' then '오후'
                          when FORMAT_DATETIME('%R', slot_opened_dt) between '19:00' and '23:59' then '저녁'
                     end as slot_time_category
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
                    ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(phone_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
                    ,'phone' as kind
                    ,slot_crt_dt
                FROM slot_base, UNNEST(SPLIT(phone_times,', ')) phone_time_slot
                UNION ALL
                SELECT
                    lawyer
                    ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(video_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
                    ,'video' as kind
                    ,slot_crt_dt
                FROM slot_base, UNNEST(SPLIT(video_times,', ')) video_time_slot
                UNION ALL
                SELECT
                    lawyer
                    ,DATE_ADD(DATETIME(dayString), INTERVAL CAST(REPLACE(visiting_time_slot,' ','') AS INT) * 30 MINUTE) slot_opened_dt
                    ,'visiting' as kind
                    ,slot_crt_dt
                FROM slot_base, UNNEST(SPLIT(visiting_times,', ')) visiting_time_slot
                ) t_slot LEFT JOIN (SELECT
                                        counsel_exc_dt
                                        ,counsel_crt_dt
                                        ,kind
                                        ,lawyer_id
                                        ,counsel_status
                                        ,counsel_id
                                    FROM `lawtalk-bigquery.mart.lt_r_user_pay_counsel`
                                    WHERE DATE(counsel_exc_dt) between date_sub(date('{{next_ds}}')+1, interval 3 month) and (date('{{next_ds}}'))
                                    AND counsel_status != 'reserved') t_advice
                                ON t_slot.lawyer = t_advice.lawyer_id
                                AND t_slot.slot_opened_dt = t_advice.counsel_exc_dt
                                AND t_slot.kind = t_advice.kind
                )
                , slot as
                (
                  select a.lawyer_id
                       , sum(case when date_cat='3months' and slot_time_category='오전' then slot_cnt end) _3months_slot_cnt_morning
                       , sum(case when date_cat='3months' and slot_time_category='오전' then reserve_cnt end) _3months_reserve_cnt_morning
                       , sum(case when date_cat='ads' and slot_time_category='오전' then slot_cnt end) ads_slot_cnt_morning
                       , sum(case when date_cat='ads' and slot_time_category='오전' then reserve_cnt end) ads_reserve_cnt_morning
                       , sum(case when date_cat='bef_ads' and slot_time_category='오전' then slot_cnt end) bef_ads_slot_cnt_morning
                       , sum(case when date_cat='bef_ads' and slot_time_category='오전' then reserve_cnt end) bef_ads_reserve_cnt_morning
                       , sum(case when date_cat='3months' and slot_time_category='오후' then slot_cnt end) _3months_slot_cnt_afternoon
                       , sum(case when date_cat='3months' and slot_time_category='오후' then reserve_cnt end) _3months_reserve_cnt_afternoon
                       , sum(case when date_cat='ads' and slot_time_category='오후' then slot_cnt end) ads_slot_cnt_afternoon
                       , sum(case when date_cat='ads' and slot_time_category='오후' then reserve_cnt end) ads_reserve_cnt_afternoon
                       , sum(case when date_cat='bef_ads' and slot_time_category='오후' then slot_cnt end) bef_ads_slot_cnt_afternoon
                       , sum(case when date_cat='bef_ads' and slot_time_category='오후' then reserve_cnt end) bef_ads_reserve_cnt_afternoon
                       , sum(case when date_cat='3months' and slot_time_category='저녁' then slot_cnt end) _3months_slot_cnt_evening
                       , sum(case when date_cat='3months' and slot_time_category='저녁' then reserve_cnt end) _3months_reserve_cnt_evening
                       , sum(case when date_cat='ads' and slot_time_category='저녁' then slot_cnt end) ads_slot_cnt_evening
                       , sum(case when date_cat='ads' and slot_time_category='저녁' then reserve_cnt end) ads_reserve_cnt_evening
                       , sum(case when date_cat='bef_ads' and slot_time_category='저녁' then slot_cnt end) bef_ads_slot_cnt_evening
                       , sum(case when date_cat='bef_ads' and slot_time_category='저녁' then reserve_cnt end) bef_ads_reserve_cnt_evening
                    from
                    (
                      select '3months' as date_cat
                          , lawyer_id
                          , slot_time_category
                          , CAST(COUNT(DISTINCT CASE WHEN kind in ('phone','video','visiting') THEN slot_opened_dt END) AS numeric) as slot_cnt
                          , CAST(COUNT(DISTINCT CASE WHEN kind in ('phone','video','visiting') AND is_reserved = 1 THEN slot_opened_dt END) AS numeric) as reserve_cnt
                        from slot_summary
                      where b_date between date_sub(date('{{next_ds}}')+1, interval 3 month) and (date('{{next_ds}}'))
                      group by 1,2,3
                      union all
                      select 'ads' as date_cat
                          , lawyer_id
                          , slot_time_category
                          , CAST(COUNT(DISTINCT CASE WHEN kind in ('phone','video','visiting') THEN slot_opened_dt END) AS numeric) as slot_cnt
                          , CAST(COUNT(DISTINCT CASE WHEN kind in ('phone','video','visiting') AND is_reserved = 1 THEN slot_opened_dt END) AS numeric) as reserve_cnt
                        from slot_summary
                      where b_date BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-16')
                                                    else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                                end)
                                      AND (date('{{next_ds}}'))
                      group by 1,2,3
                      union all
                      select 'bef_ads' as date_cat
                          , lawyer_id
                          , slot_time_category
                          , CAST(COUNT(DISTINCT CASE WHEN kind in ('phone','video','visiting') THEN slot_opened_dt END) AS numeric) as slot_cnt
                          , CAST(COUNT(DISTINCT CASE WHEN kind in ('phone','video','visiting') AND is_reserved = 1 THEN slot_opened_dt END) AS numeric) as reserve_cnt
                        from slot_summary
                      where b_date
                            BETWEEN (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')
                                          else date(format_date('%Y-%m',date_sub(date('{{next_ds}}'), interval 1 month))||'-16')
                                    end)
                            AND (case when extract(day from date('{{next_ds}}')) > 15 then date(format_date('%Y-%m',date('{{next_ds}}'))||'-15')
                                      else date(format_date('%Y-%m',date('{{next_ds}}'))||'-01')-1
                                  end)
                      group by 1,2,3
                    ) a
                   group by 1
                )
                -- 이제 다 합치자!
                select date('{{next_ds}}') as b_date
                     , a.slug
                     , a.lawyer_name
                     , a.ad_cnt
                     , a.manager
                     , coalesce(g._3months_ad_round_cnt,0) as _3months_ad_round_cnt
                     , coalesce(b.ads_imp_cart_cnt,0) as ads_imp_cart_cnt
                     , coalesce(b.ads_click_card_cnt,0) as ads_click_card_cnt
                     , case when coalesce(b.ads_imp_cart_cnt,0) = 0 or coalesce(b.ads_click_card_cnt,0) = 0 then 0
                            else coalesce(b.ads_click_card_cnt,0)/coalesce(b.ads_imp_cart_cnt,0)
                       end as ads_imp_click_rate
                     , coalesce(b.bef_ads_imp_cart_cnt,0) as bef_ads_imp_cart_cnt
                     , coalesce(b.bef_ads_click_card_cnt,0) as bef_ads_click_card_cnt
                     , case when coalesce(b.bef_ads_imp_cart_cnt,0) = 0 or coalesce(b.bef_ads_click_card_cnt,0) = 0 then 0
                            else coalesce(b.bef_ads_click_card_cnt,0)/coalesce(b.bef_ads_imp_cart_cnt,0)
                       end as bef_ads_imp_click_rate
                     , coalesce(b._3months_imp_cart_cnt,0) as _3months_imp_cart_cnt
                     , coalesce(b._3months_click_card_cnt,0) as _3months_click_card_cnt
                     , case when coalesce(b._3months_imp_cart_cnt,0) = 0 or coalesce(b._3months_click_card_cnt,0) = 0 then 0
                            else coalesce(b._3months_click_card_cnt,0)/coalesce(b._3months_imp_cart_cnt,0)
                       end as _3months_imp_click_rate
                     , coalesce(c.ads_qna_cnt,0)+coalesce(d.ads_posts_cnt,0)+coalesce(e.ads_video_cnt,0) as ads_tot_contents_cnt
                     , coalesce(c.ads_qna_cnt,0) as ads_qna_cnt
                     , coalesce(d.ads_posts_cnt,0) as ads_posts_cnt
                     , coalesce(e.ads_video_cnt,0) as ads_video_cnt
                     , coalesce(b.ads_click_qna_cnt,0)+coalesce(b.ads_click_posts_cnt,0)+coalesce(b.ads_click_video_cnt,0) as ads_tot_click_contents_cnt
                     , coalesce(b.ads_click_qna_cnt,0) as ads_click_qna_cnt
                     , coalesce(b.ads_click_posts_cnt,0) as ads_click_posts_cnt
                     , coalesce(b.ads_click_video_cnt,0) as ads_click_video_cnt
                     , coalesce(c.bef_ads_qna_cnt,0)+coalesce(d.bef_ads_posts_cnt,0)+coalesce(e.bef_ads_video_cnt,0) as bef_ads_tot_contents_cnt
                     , coalesce(c.bef_ads_qna_cnt,0) as bef_ads_qna_cnt
                     , coalesce(d.bef_ads_posts_cnt,0) as bef_ads_posts_cnt
                     , coalesce(e.bef_ads_video_cnt,0) as bef_ads_video_cnt
                     , coalesce(b.bef_ads_click_qna_cnt,0)+coalesce(b.bef_ads_click_posts_cnt,0)+coalesce(b.bef_ads_click_video_cnt,0) as bef_ads_tot_click_contents_cnt
                     , coalesce(b.bef_ads_click_qna_cnt,0) as bef_ads_click_qna_cnt
                     , coalesce(b.bef_ads_click_posts_cnt,0) as bef_ads_click_posts_cnt
                     , coalesce(b.bef_ads_click_video_cnt,0) as bef_ads_click_video_cnt
                     , coalesce(c._3months_qna_cnt,0)+coalesce(d._3months_posts_cnt,0)+coalesce(e._3months_video_cnt,0) as _3months_tot_contents_cnt
                     , coalesce(c._3months_qna_cnt,0) as _3months_qna_cnt
                     , coalesce(d._3months_posts_cnt,0) as _3months_posts_cnt
                     , coalesce(e._3months_video_cnt,0) as _3months_video_cnt
                     , coalesce(b._3months_click_qna_cnt,0)+coalesce(b._3months_click_posts_cnt,0)+coalesce(b._3months_click_video_cnt,0) as _3months_tot_click_contents_cnt
                     , coalesce(b._3months_click_qna_cnt,0) as _3months_click_qna_cnt
                     , coalesce(b._3months_click_posts_cnt,0) as _3months_click_posts_cnt
                     , coalesce(b._3months_click_video_cnt,0) as _3months_click_video_cnt
                     , coalesce(b.ads_tot_view_cnt,0) as ads_tot_view_cnt
                     , coalesce(b.ads_click_card_cnt,0)+coalesce(b.ads_click_qna_cnt,0)+coalesce(b.ads_click_posts_cnt,0)+coalesce(b.ads_click_video_cnt,0) as ads_in_lawtalk_cnt
                     , coalesce(b.ads_tot_view_cnt,0)-(coalesce(b.ads_click_card_cnt,0)+coalesce(b.ads_click_qna_cnt,0)+coalesce(b.ads_click_posts_cnt,0)+coalesce(b.ads_click_video_cnt,0)) as ads_out_lawtalk_cnt
                     , coalesce(f.ads_counsel_cnt,0) as ads_counsel_cnt
                     , case when coalesce(f.ads_counsel_cnt,0) = 0 or coalesce(b.ads_tot_view_cnt,0) = 0 then 0
                            else coalesce(f.ads_counsel_cnt,0)/coalesce(b.ads_tot_view_cnt,0)
                       end as ads_view_counsel_rate
                     , coalesce(b.bef_ads_tot_view_cnt,0) as bef_ads_tot_view_cnt
                     , coalesce(b.bef_ads_click_card_cnt,0)+coalesce(b.bef_ads_click_qna_cnt,0)+coalesce(b.bef_ads_click_posts_cnt,0)+coalesce(b.bef_ads_click_video_cnt,0) as bef_ads_in_lawtalk_cnt
                     , coalesce(b.bef_ads_tot_view_cnt,0)-(coalesce(b.bef_ads_click_card_cnt,0)+coalesce(b.bef_ads_click_qna_cnt,0)+coalesce(b.bef_ads_click_posts_cnt,0)+coalesce(b.bef_ads_click_video_cnt,0)) as bef_ads_out_lawtalk_cnt
                     , coalesce(f.bef_ads_counsel_cnt,0) as bef_ads_counsel_cnt
                     , case when coalesce(f.bef_ads_counsel_cnt,0) = 0 or coalesce(b.bef_ads_tot_view_cnt,0) = 0 then 0
                            else coalesce(f.bef_ads_counsel_cnt,0)/coalesce(b.bef_ads_tot_view_cnt,0)
                       end as bef_ads_view_counsel_rate
                     , coalesce(b._3months_tot_view_cnt,0) as _3months_tot_view_cnt
                     , coalesce(b._3months_click_card_cnt,0)+coalesce(b._3months_click_qna_cnt,0)+coalesce(b._3months_click_posts_cnt,0)+coalesce(b._3months_click_video_cnt,0) as _3months_in_lawtalk_cnt
                     , coalesce(b._3months_tot_view_cnt,0)-(coalesce(b._3months_click_card_cnt,0)+coalesce(b._3months_click_qna_cnt,0)+coalesce(b._3months_click_posts_cnt,0)+coalesce(b._3months_click_video_cnt,0)) as _3months_out_lawtalk_cnt
                     , coalesce(f._3months_counsel_cnt,0) as _3months_counsel_cnt
                     , case when coalesce(f._3months_counsel_cnt,0) = 0 or coalesce(b._3months_tot_view_cnt,0) = 0 then 0
                            else coalesce(f._3months_counsel_cnt,0)/coalesce(b._3months_tot_view_cnt,0)
                       end as _3months_view_counsel_rate
                     , case when (case when coalesce(f.ads_counsel_price,0)+coalesce(f.ads_predict_price,0)=0 then 1
                                       else coalesce(g.ads_tot_fee,0)/(coalesce(f.ads_counsel_price,0)+coalesce(f.ads_predict_price,0))
                                  end) <= 0.2 then '만족'
                            else '불만족'
                       end as ads_satisfy
                     , coalesce(g.ads_tot_fee,0) as ads_tot_fee
                     , coalesce(f.ads_counsel_price,0)+coalesce(f.ads_predict_price,0) as ads_feeling_value
                     , coalesce(f.ads_counsel_price,0) as ads_counsel_price
                     , coalesce(f.ads_predict_price,0) as ads_predict_price
                     , case when (case when coalesce(f.bef_ads_counsel_price,0)+coalesce(f.bef_ads_predict_price,0)=0 then 1
                                       else coalesce(g.bef_ads_tot_fee,0)/(coalesce(f.bef_ads_counsel_price,0)+coalesce(f.bef_ads_predict_price,0))
                                  end) <= 0.2 then '만족'
                            else '불만족'
                       end as bef_ads_satisfy
                     , coalesce(g.bef_ads_tot_fee,0) as bef_ads_tot_fee
                     , coalesce(f.bef_ads_counsel_price,0)+coalesce(f.bef_ads_predict_price,0) as bef_ads_feeling_value
                     , coalesce(f.bef_ads_counsel_price,0) as bef_ads_counsel_price
                     , coalesce(f.bef_ads_predict_price,0) as bef_ads_predict_price
                     , case when (case when coalesce(f._3months_counsel_price,0)+coalesce(f._3months_predict_price,0)=0 then 1
                                       else coalesce(g._3months_tot_fee,0)/(coalesce(f._3months_counsel_price,0)+coalesce(f._3months_predict_price,0))
                                  end) <= 0.2 then '만족'
                            else '불만족'
                       end as _3months_satisfy
                     , coalesce(g._3months_tot_fee,0) as _3months_tot_fee
                     , coalesce(f._3months_counsel_price,0)+coalesce(f._3months_predict_price,0) as _3months_feeling_value
                     , coalesce(f._3months_counsel_price,0) as _3months_counsel_price
                     , coalesce(f._3months_predict_price,0) as _3months_predict_price
                     , coalesce(h.ads_slot_cnt_morning,0) as ads_slot_cnt_morning
                     , coalesce(h.ads_reserve_cnt_morning,0) as ads_reserve_cnt_morning
                     , case when coalesce(h.ads_slot_cnt_morning,0) = 0 or coalesce(h.ads_reserve_cnt_morning,0) = 0 then 0
                            else coalesce(h.ads_reserve_cnt_morning,0)/coalesce(h.ads_slot_cnt_morning,0)
                       end as ads_slot_counsel_rate_morning
                     , coalesce(h.bef_ads_slot_cnt_morning,0) as bef_ads_slot_cnt_morning
                     , coalesce(h.bef_ads_reserve_cnt_morning,0) as bef_ads_reserve_cnt_morning
                     , case when coalesce(h.bef_ads_slot_cnt_morning,0) = 0 or coalesce(h.bef_ads_reserve_cnt_morning,0) = 0 then 0
                            else coalesce(h.bef_ads_reserve_cnt_morning,0)/coalesce(h.bef_ads_slot_cnt_morning,0)
                       end as bef_ads_slot_counsel_rate_morning
                     , coalesce(h._3months_slot_cnt_morning,0) as _3months_slot_cnt_morning
                     , coalesce(h._3months_reserve_cnt_morning,0) as _3months_reserve_cnt_morning
                     , case when coalesce(h._3months_slot_cnt_morning,0) = 0 or coalesce(h._3months_reserve_cnt_morning,0) = 0 then 0
                            else coalesce(h._3months_reserve_cnt_morning,0)/coalesce(h._3months_slot_cnt_morning,0)
                       end as _3months_slot_counsel_rate_morning
                     , coalesce(h.ads_slot_cnt_afternoon,0) as ads_slot_cnt_afternoon
                     , coalesce(h.ads_reserve_cnt_afternoon,0) as ads_reserve_cnt_afternoon
                     , case when coalesce(h.ads_slot_cnt_afternoon,0) = 0 or coalesce(h.ads_reserve_cnt_afternoon,0) = 0 then 0
                            else coalesce(h.ads_reserve_cnt_afternoon,0)/coalesce(h.ads_slot_cnt_afternoon,0)
                       end as ads_slot_counsel_rate_afternoon
                     , coalesce(h.bef_ads_slot_cnt_afternoon,0) as bef_ads_slot_cnt_afternoon
                     , coalesce(h.bef_ads_reserve_cnt_afternoon,0) as bef_ads_reserve_cnt_afternoon
                     , case when coalesce(h.bef_ads_slot_cnt_afternoon,0) = 0 or coalesce(h.bef_ads_reserve_cnt_afternoon,0) = 0 then 0
                            else coalesce(h.bef_ads_reserve_cnt_afternoon,0)/coalesce(h.bef_ads_slot_cnt_afternoon,0)
                       end as bef_ads_slot_counsel_rate_afternoon
                     , coalesce(h._3months_slot_cnt_afternoon,0) as _3months_slot_cnt_afternoon
                     , coalesce(h._3months_reserve_cnt_afternoon,0) as _3months_reserve_cnt_afternoon
                     , case when coalesce(h._3months_slot_cnt_afternoon,0) = 0 or coalesce(h._3months_reserve_cnt_afternoon,0) = 0 then 0
                            else coalesce(h._3months_reserve_cnt_afternoon,0)/coalesce(h._3months_slot_cnt_afternoon,0)
                       end as _3months_slot_counsel_rate_afternoon
                     , coalesce(h.ads_slot_cnt_evening,0) as ads_slot_cnt_evening
                     , coalesce(h.ads_reserve_cnt_evening,0) as ads_reserve_cnt_evening
                     , case when coalesce(h.ads_slot_cnt_evening,0) = 0 or coalesce(h.ads_reserve_cnt_evening,0) = 0 then 0
                            else coalesce(h.ads_reserve_cnt_evening,0)/coalesce(h.ads_slot_cnt_evening,0)
                       end as ads_slot_counsel_rate_evening
                     , coalesce(h.bef_ads_slot_cnt_evening,0) as bef_ads_slot_cnt_evening
                     , coalesce(h.bef_ads_reserve_cnt_evening,0) as bef_ads_reserve_cnt_evening
                     , case when coalesce(h.bef_ads_slot_cnt_evening,0) = 0 or coalesce(h.bef_ads_reserve_cnt_evening,0) = 0 then 0
                            else coalesce(h.bef_ads_reserve_cnt_evening,0)/coalesce(h.bef_ads_slot_cnt_evening,0)
                       end as bef_ads_slot_counsel_rate_evening
                     , coalesce(h._3months_slot_cnt_evening,0) as _3months_slot_cnt_evening
                     , coalesce(h._3months_reserve_cnt_evening,0) as _3months_reserve_cnt_evening
                     , case when coalesce(h._3months_slot_cnt_evening,0) = 0 or coalesce(h._3months_reserve_cnt_evening,0) = 0 then 0
                            else coalesce(h._3months_reserve_cnt_evening,0)/coalesce(h._3months_slot_cnt_evening,0)
                       end as _3months_slot_counsel_rate_evening
                     , case when g.bef_ads_tot_fee is null then 0 else 1 end as is_bef_ad
                     , case when g._3months_tot_fee is null then 0 else 1 end as is_3months_ad
                  from main_info a
                  left join profile b
                    on a.slug = b.slug
                  left join qna c
                    on a.lawyer_id = c.lawyer_id
                  left join posts d
                    on a.lawyer_id = d.lawyer_id
                  left join video e
                    on a.lawyer_id = e.lawyer_id
                  left join counsel f
                    on a.lawyer_id = f.lawyer_id
                  left join ad_sale g
                    on a.lawyer_id = g.lawyer_id
                  left join slot h
                    on a.lawyer_id = h.lawyer_id
                 where date('{{next_ds}}') = date(current_timestamp,'Asia/Seoul')-1
                '''
        )

        delete_lt_s_ad_lawyer_value >> insert_lt_s_ad_lawyer_value

start >> lt_s_ad_lawyer_value
