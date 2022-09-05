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
WITH t1 AS (
SELECT 
    _id
    , answers
    , CAST(number AS string) AS question_slug
    , REGEXP_REPLACE(category, r'\"', "") AS category
    , title
    , body
    , viewCount
    , createdAt
FROM `lawtalk-bigquery.raw.questions`, UNNEST(JSON_EXTRACT_ARRAY(REGEXP_REPLACE(categories, r'ObjectId\(|\)', ""))) AS category
)
, t2 AS (
SELECT 
    _id AS category
    , name AS categoryName
FROM `lawtalk-bigquery.raw.adcategories`  
)
SELECT 
    t1.*
    , t2.categoryName
FROM t1
LEFT JOIN t2
USING (category)
;

'''


df = bigquery_to_pandas(query)

df = df[df.createdAt.dt.year > 2019]
df = df[df.groupby('categoryName')['viewCount'].transform(lambda x : x <= x.quantile(0.99))]

r = df.groupby("categoryName").viewCount.sum().rank(ascending = False).sort_values().reset_index().rename(columns = {"viewCount" : "viewRank"})

df = pd.merge(df, r, on = "categoryName")

df[df.viewRank <= 10].groupby("categoryName").viewCount.describe().sort_values("count", ascending = False)


(
    ggplot() +
    geom_histogram(data = df[df.viewRank <= 10], mapping = aes(x = "viewCount", fill = "categoryName"), position = "identity", bins = 50, alpha = 0.3) +
    facet_grid("categoryName~.") +
    theme_bw() +
    theme(figure_size = (10, 15), text = element_text(fontproperties = font))
)





query = '''
WITH t1 AS (
SELECT 
    _id
    , REGEXP_REPLACE(answer, r'\"', "") AS answer
    , CAST(number AS string) AS question_slug
    , categories
    , title
    , body
    , viewCount
    , createdAt
FROM `lawtalk-bigquery.raw.questions`, UNNEST(JSON_EXTRACT_ARRAY(REGEXP_REPLACE(answers, r'ObjectId\(|\)', ""))) AS answer
)
, t2 AS  (
SELECT
    _id
    , answer
    , question_slug
    , REGEXP_REPLACE(category, r'\"', "") AS category
    , title
    , body
    , viewCount
    , createdAt
FROM t1, UNNEST(JSON_EXTRACT_ARRAY(REGEXP_REPLACE(categories, r'ObjectId\(|\)', ""))) AS category
)
, l1 AS (
SELECT 
    _id AS answer
    , lawyer
FROM `lawtalk-bigquery.raw.answers`  
)
, c1 AS (
SELECT 
    _id AS category
    , name AS categoryName
FROM `lawtalk-bigquery.raw.adcategories`  
)
, j1 AS (
SELECT 
    t2.*
    , c1.categoryName
FROM t2
LEFT JOIN c1
USING (category)
)
, j2 AS (
SELECT 
    j1.*
    , l1.lawyer
FROM j1
LEFT JOIN l1
USING (answer)    
)
, g1 AS (
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
, g2 AS (
SELECT 
    user_pseudo_id
    , event_datetime
    , event_ymd
    , question_slug
    , page_location
    , page_referrer
FROM g1
WHERE  
    TRUE
    AND NOT CONTAINS_SUBSTR(page_referrer, "lawtalk.co.kr")
    AND (CONTAINS_SUBSTR(page_referrer, "google")
    OR CONTAINS_SUBSTR(page_referrer, "naver"))
GROUP BY 1, 2, 3, 4, 5, 6
)
, g3 AS (
SELECT 
    question_slug
    , COUNT(user_pseudo_id) AS pv
    , COUNT(DISTINCT user_pseudo_id) AS uv
FROM g2
GROUP BY 1
)
, f1 AS (
SELECT
    j2.*
    , g3.pv
    , g3.uv
FROM j2
LEFT JOIN g3
USING (question_slug)
)
SELECT * FROM f1
;

'''

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 10)
pd.set_option('max_colwidth', -1)


df = bigquery_to_pandas(query)
df = df[df.createdAt.dt.year > 2019]
df = df[df.groupby('categoryName')['viewCount'].transform(lambda x : x <= x.quantile(0.99))]


gdf = df.groupby("question_slug").agg(answer_cnt = ("answer", pd.Series.nunique), viewCount = ("viewCount", lambda x : np.unique(x)[0]), categories = ("categoryName", np.unique), category_cnt = ("categoryName", pd.Series.nunique), uv = ("uv", np.unique), pv = ("pv", np.unique))

gdf.categories = gdf.categories.astype(str)

# uv, pv, 페이지인입의 상관관계
gdf[["viewCount", "uv", "pv"]].corr()


# 평균 페이지인입 많은 카테고리
gdf.groupby("categories").viewCount.describe()[gdf.groupby("categories").viewCount.describe()["count"] > 5].sort_values("mean", ascending = False).head(15)

# 최소 페이지인입 많은 카테고리
gdf.groupby("categories").viewCount.describe()[gdf.groupby("categories").viewCount.describe()["count"] > 5].sort_values("min", ascending = False).head(15)


# 평균 uv 많은 카테고리
gdf.groupby("categories").uv.describe()[gdf.groupby("categories").uv.describe()["count"] > 5].sort_values("mean", ascending = False).head(15)

# 최소 uv 많은 카테고리
gdf.groupby("categories").uv.describe()[gdf.groupby("categories").uv.describe()["uv"] > 5].sort_values("min", ascending = False).head(15)


# 평균 pv 많은 카테고리
gdf.groupby("categories").pv.describe()[gdf.groupby("categories").pv.describe()["count"] > 5].sort_values("mean", ascending = False).head(15)

# 최소 pv 많은 카테고리
gdf.groupby("categories").pv.describe()[gdf.groupby("categories").pv.describe()["uv"] > 5].sort_values("min", ascending = False).head(15)


comp = pd.DataFrame(
    {
        "viewCount_rnk" : gdf.groupby("categories").viewCount.describe()[gdf.groupby("categories").viewCount.describe()["count"] > 5].sort_values("mean", ascending = False).head(15).index,
        
        "viewCount_cnt" : gdf.groupby("categories").viewCount.describe()[gdf.groupby("categories").viewCount.describe()["count"] > 5].sort_values("mean", ascending = False).head(15)["count"].values,
        
        "uv_rnk" : gdf.groupby("categories").uv.describe()[gdf.groupby("categories").uv.describe()["count"] > 5].sort_values("mean", ascending = False).head(15).index,
        "uv_cnt" : gdf.groupby("categories").uv.describe()[gdf.groupby("categories").uv.describe()["count"] > 5].sort_values("mean", ascending = False).head(15)["count"].values,
        
        "pv_rnk" : gdf.groupby("categories").pv.describe()[gdf.groupby("categories").pv.describe()["count"] > 5].sort_values("mean", ascending = False).head(15).index,
        "pv_cnt" : gdf.groupby("categories").pv.describe()[gdf.groupby("categories").pv.describe()["count"] > 5].sort_values("mean", ascending = False).head(15)["count"].values
    }
)


gdf.categories.value_counts()
gdf.groupby("categories").uv.describe()[gdf.groupby("categories").uv.describe()["count"] > 5].sort_values("mean", ascending = False).head(15)

gdf.groupby("categories").uv.describe()[gdf.groupby("categories").uv.describe()["count"] > 5].sort_values("min", ascending = False).head(15)

gdf.groupby("categories").uv.describe()[gdf.groupby("categories").uv.describe()["count"] > 5].sort_values("count", ascending = False).head(15)


df[df.question_slug.isin(gdf[gdf.categories == "['성범죄' '지식재산권']"].index)][["title", "body", "categoryName"]].drop_duplicates()

comp[["viewCount_rnk", "uv_rnk", "pv_rnk"]]


gdf[gdf.index.isin(df[df.categoryName == "체포/구속"].question_slug)].categories.value_counts()

# 변호사가 답변을 많이 단 글
gdf.assign(categories = lambda x : x.categories.astype(str)).groupby("categories").viewCount.describe().sort_values("count", ascending = False)

gdf.groupby("answer_cnt").viewCount.describe()

"https://www.lawtalk.co.kr/qna/" + gdf[gdf.answer_cnt > 23].index

gdf.groupby("answer_cnt").viewCount.describe()



# 상위 10개의 분포도
df = df[df.createdAt.dt.year > 2019]
df = df[df.groupby(['lawyer' ''])['viewCount'].transform(lambda x : x <= x.quantile(0.99))]

r = df.groupby("categoryName").viewCount.sum().rank(ascending = False).sort_values().reset_index().rename(columns = {"viewCount" : "viewRank"})

df = pd.merge(df, r, on = "categoryName")

df[df.viewRank <= 10].groupby("categoryName").viewCount.describe().sort_values("count", ascending = False)


(
    ggplot() +
    geom_histogram(data = df[df.viewRank <= 10], mapping = aes(x = "viewCount", fill = "categoryName"), position = "identity", bins = 50, alpha = 0.3) +
    facet_grid("categoryName~.") +
    theme_bw() +
    theme(figure_size = (10, 15), text = element_text(fontproperties = font))
)






