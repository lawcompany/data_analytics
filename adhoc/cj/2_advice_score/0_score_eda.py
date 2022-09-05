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

import matplotlib as mpl
# !pip install plotnine
from plotnine import *

# !pip install --upgrade google-cloud-bigquery
from google.cloud import bigquery

warnings.filterwarnings("ignore")

%matplotlib inline

pd.set_option('display.max_rows', 15)
pd.set_option('display.max_columns', 50)
pd.set_option('max_colwidth', 50)
pd.options.display.float_format = '{:,.2f}'.format

import matplotlib
import matplotlib.pyplot as plt 
import matplotlib.font_manager as fm  

path = '/usr/share/fonts/truetype/nanum/NanumMyeongjoExtraBold.ttf' 
font_name = fm.FontProperties(fname=path, size=10).get_name()
print(font_name)
plt.rc('font', family=font_name)
font = fm.FontProperties(fname=path, size=9)
    

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
SELECT * FROM t4
'''

# 16,682,253 
%time df = bigquery_to_pandas(query)
df.page_referrer = df.page_referrer.fillna("")
df = df[~df.page_referrer.str.contains("google|naver", regex = True)]
df["event_ym"] = df.event_datetime.dt.year.astype(str) + df.event_datetime.dt.month.astype(str).str.zfill(2)


gdf = df.groupby(["event_ym", "question_slug", "createdAt", "answer_createdAt", "title", "body"]).agg(pv = ("user_pseudo_id", "count"), uv = ("user_pseudo_id", pd.Series.nunique)).reset_index()

%time gdf["ym_rnk"] = gdf.sort_values(["question_slug", "event_ym"]).groupby(["question_slug"]).event_ym.rank(method = "first")
%time gdf["pv_rnk"] = gdf.sort_values(["question_slug", "pv"]).groupby(["question_slug"]).pv.rank(method = "first", ascending = False)
%time gdf["uv_rnk"] = gdf.sort_values(["question_slug", "uv"]).groupby(["question_slug"]).uv.rank(method = "first", ascending = False)


gdf[(gdf.uv_rnk == 1) & (gdf.createdAt.dt.year >= 2021) & (gdf.createdAt.dt.month == 3)].ym_rnk.hist(bins = range(14))

gdf[(gdf.uv_rnk == 1) & (gdf.createdAt.dt.year >= 2021) & (gdf.createdAt.dt.month == 3) & (gdf.question_slug.isin(gdf.groupby("question_slug").uv.mean()[gdf.groupby("question_slug").uv.mean() > 5].index))].ym_rnk.hist(bins = range(14))

gdf[(gdf.uv_rnk == 1) & (gdf.createdAt.dt.year >= 2021) & (gdf.createdAt.dt.month == 3) & (gdf.question_slug.isin(gdf.groupby("question_slug").uv.mean()[gdf.groupby("question_slug").uv.mean() > 10].index))].ym_rnk.hist(bins = range(14))

gdf[gdf.question_slug.isin(gdf[gdf.ym_rnk >= 12].question_slug)].groupby("ym_rnk").uv.sum().plot()

tmp = gdf[(gdf.createdAt.dt.year >= 2021) & (gdf.createdAt.dt.month == 3) & (gdf.question_slug.isin(gdf.groupby("question_slug").uv.mean()[gdf.groupby("question_slug").uv.mean() > 10].index))]

tmp.groupby("question_slug").uv.mean().mean()

tmp[tmp.ym_rnk <= 9].groupby("question_slug").uv.mean().mean()

pd.merge(tmp, tmp.assign(event_ym = lambda x : x.event_ym.astype(int)).groupby("question_slug").event_ym.nlargest(n = 2).reset_index().drop(columns = "level_1").assign(event_ym = lambda x : x.event_ym.astype(str)), on = ["question_slug", "event_ym"], how = "inner").groupby("question_slug").uv.mean().mean()

tmp[tmp.event_ym.isin(["202201", "202202"])].groupby("question_slug").uv.mean().mean()

gdf[(gdf.event_ym.isin(["202201", "202202"])) & (gdf.question_slug.isin(gdf.groupby("question_slug").uv.mean()[gdf.groupby("question_slug").uv.mean() > 10].index))].groupby("question_slug").uv.mean().mean()

gdf[gdf.question_slug.isin(gdf.groupby("question_slug").ym_rnk.max()[gdf.groupby("question_slug").ym_rnk.max() == 1].index)].uv.describe(percentiles = [i * 0.1 for i in range(11)])

gdf[gdf.question_slug.isin(gdf.groupby("question_slug").ym_rnk.max()[gdf.groupby("question_slug").ym_rnk.max() != 1].index)].uv.describe(percentiles = [i * 0.1 for i in range(11)])


gdf[(gdf.event_ym < "202201")].uv.describe()
gdf[(gdf.event_ym <= "202201")].uv.describe()
