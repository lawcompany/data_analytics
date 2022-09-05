# -*- coding: utf-8 -*-
import os
print(os.getpid())
import sys
import warnings
import gc

import re
import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split

# !pip install --upgrade google-cloud-bigquery
from google.cloud import bigquery

warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 15)
pd.set_option('display.max_columns', 50)
pd.set_option('max_colwidth', 50)
pd.options.display.float_format = '{:,.2f}'.format

def bigquery_to_pandas(query_string) :

    credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
    bqclient = bigquery.Client()
    
    job_config = bigquery.QueryJobConfig(
        allow_large_results=True
    )
    
    b = (
        bqclient.query(query_string, job_config=job_config)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    
    return b



query = '''
WITH t11 AS (
SELECT
    user_pseudo_id,
    event_name,
    DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_datetime,
    FORMAT_DATE("%Y%m%d", DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul')) AS event_ymd,
    REGEXP_REPLACE(udfs.param_value_by_key('page_location',event_params).string_value, r'.+/qna/([0-9]+).*', r'\\1') as question_slug,
    udfs.param_value_by_key('page_location',event_params).string_value as page_location,
    udfs.param_value_by_key('page_referrer',event_params).string_value as page_referrer
FROM
    `lawtalk-bigquery.analytics_265523655.events_*`
WHERE TRUE
    -- AND _TABLE_SUFFIX BETWEEN '20211001' AND '20211101'
    AND REGEXP_CONTAINS(udfs.param_value_by_key('page_location',event_params).string_value, r'.+/qna/[0-9]+.*') 
    AND udfs.param_value_by_key('page_location',event_params).string_value not like '%manager%' -- manager
) 
, t1 AS (
SELECT 
    user_pseudo_id
    , event_datetime
    , event_ymd
    , question_slug
    , page_location
    , page_referrer
FROM t11
WHERE  
    TRUE
    AND NOT CONTAINS_SUBSTR(page_referrer, "lawtalk.co.kr")
    AND (CONTAINS_SUBSTR(page_referrer, "google")
    OR CONTAINS_SUBSTR(page_referrer, "naver"))
GROUP BY 1, 2, 3, 4, 5, 6
)
, t2 AS (
SELECT 
    _id
    , answers
    , CAST(number AS string) AS question_slug
    , title
    , body
    , viewCount
    , createdAt
FROM `lawtalk-bigquery.raw.questions`

)
, t3 AS (
SELECT 
    t2.*
    , MIN(j1.createdAt) AS answer_createdAt
FROM t2
LEFT JOIN `lawtalk-bigquery.raw.answers` AS j1
ON (t2._id = j1.question)
GROUP BY 1, 2, 3, 4, 5, 6, 7
)
, t4 AS (
SELECT 
    t1.user_pseudo_id
    , t1.event_datetime
    , t1.question_slug
    , t1.event_ymd
    -- , t1.page_location
    , t1.page_referrer
    , t3.title
    , t3.body
    -- , t3.answers
    -- , t3.viewCount
    , t3.createdAt
    , t3.answer_createdAt
FROM t1
LEFT JOIN t3
USING (question_slug)
)
SELECT 
    LEFT(event_ymd, 6) AS event_ym
    , question_slug
    , createdAt
    , answer_createdAt
    , title
    , body
    , COUNT(user_pseudo_id) AS pv
    , COUNT(DISTINCT user_pseudo_id) AS uv
FROM t4
GROUP BY 1, 2, 3, 4, 5, 6
;
'''

# 
%time gdf = bigquery_to_pandas(query)
gdf = gdf[gdf.createdAt.notna()]
gdf["ym"] = gdf.createdAt.dt.year.astype(int).astype(str).str.zfill(4) + gdf.createdAt.dt.month.astype(int).astype(str).str.zfill(2)
gdf["x"] = gdf.title + gdf.body.str[:50]


# 한달에 한번이라도 10회 이상을 가진 아이들만 땅값을 계t
data1 = gdf[(gdf.ym < "202201") & (gdf.question_slug.isin(gdf.groupby("question_slug").uv.max()[gdf.groupby("question_slug").uv.max() > 10].index))]

# 충분한 땅값을 얻지 못하였을것이라 가정하고 데이터를 따로
data2 = gdf[(gdf.ym >= "202201")]

data1 = data1.groupby(["question_slug", "title", "body", "x", "createdAt"]).agg(pv = ("pv", "mean"), uv = ("uv", "mean")).reset_index()
data2 = data2.groupby(["question_slug", "title", "body", "x", "createdAt"]).agg(pv = ("pv", "mean"), uv = ("uv", "mean")).reset_index()

train, val1 = train_test_split(data1, random_state = 1, test_size = 0.2)
val2 = data2

# train set에 이상치 하나 제거
train = train[train.uv != train.uv.max()]
train.uv.describe()
val1.uv.describe()
val2.uv.describe()

train.to_csv(f"train_data/train_{datetime.datetime.now().strftime('%Y%m%d')}.csv", index = False, sep = "\t")
val1.to_csv(f"train_data/val1_{datetime.datetime.now().strftime('%Y%m%d')}.csv", index = False, sep = "\t")
val2.to_csv(f"train_data/val2_{datetime.datetime.now().strftime('%Y%m%d')}.csv", index = False, sep = "\t")
