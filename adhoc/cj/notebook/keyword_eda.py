import os
import sys
import warnings

from glob import glob
import random
import datetime
import re
from tqdm import tqdm

import pandas as pd
import numpy as np

from google.cloud import bigquery

import matplotlib as mpl
import matplotlib.pyplot as plt 
import matplotlib.font_manager as fm  


from plotnine import *

warnings.filterwarnings('ignore')


path = '/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf' 
font_name = fm.FontProperties(fname=path, size=10).get_name()
print(font_name)
plt.rc('font', family=font_name)
font = fm.FontProperties(fname=path, size=9)


pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('max_colwidth', -1)
pd.options.display.float_format = '{:.2f}'.format

def bigquery_to_pandas(query_string) :

    credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
    bqclient = bigquery.Client()
    
    b = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(
            # Optionally, explicitly request to use the BigQuery Storage API. As of
            # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
            # API is used by default.
            create_bqstorage_client=True,
        )
    )
    
    return b


adorder_query = '''
WITH A AS (
      SELECT
      _id as adorders_id
      ,lawyer as lawyers_id
      ,status as adorders_status
      ,date(DATETIME(createdAt,'Asia/Seoul')) as adorders_createdAt_date
      ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul')  as adorders_startAt_timestamp
      ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as adorders_endAt_timestamp
      , adLocations
      , keywords
      , categories
      , item
    from `lawtalk-bigquery.raw.adorders`
    where date(createdAt) <= '2022-12-31'
)
, B AS (
      SELECT 
      orders_id
      ,_id as adpayments_id
    FROM `lawtalk-bigquery.raw.adpayments`, unnest(REGEXP_EXTRACT_ALL(orders, r"ObjectId\('(.*?)'\)")) as orders_id
        where date(createdAt) <= '2022-12-31'
)
, C AS (
      select
      _id as adpayments_id
      ,method as adpayments_method
      ,status as adpayments_status
    from `lawtalk-bigquery.raw.adpayments`
        where date(createdAt) <= '2022-12-31'
)
, adorders_history AS (
SELECT
    A.adorders_id
    , A.lawyers_id
    , A.adorders_createdAt_date
    , date(A.adorders_startAt_timestamp) as adorders_start_date
    , date(A.adorders_endAt_timestamp) as adorders_end_date
    , A.adorders_status
    , C.adpayments_id
    , C.adpayments_status
    , C.adpayments_method
    , A.adLocations
    , A.keywords
    , A.categories
    , A.item
FROM A 
  LEFT OUTER JOIN B
    ON A.adorders_id = B.orders_id
  LEFT OUTER JOIN C
    ON B.adpayments_id = C.adpayments_id
)
, adorders_pausehistory_tmp AS (
  SELECT 
  _id as adorders_id
  ,pauseHistory
  ,regexp_extract_all(pauseHistory, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as pauseHistory_startAt
  ,regexp_extract_all(pauseHistory, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})") as pauseHistory_endAt
FROM `lawtalk-bigquery.raw.adorders`
where 1=1
  and date(DATETIME(createdAt, 'Asia/Seoul')) <= '2022-12-31'
  and REGEXP_CONTAINS(pauseHistory, 'ObjectId')
)
, adorders_pausehistory AS (
SELECT 
  adorders_id
  ,DATE(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_startAt), 'Asia/Seoul')) as pauseHistory_startAt_date
  ,DATE(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', pauseHistory_endAt), 'Asia/Seoul')) as pauseHistory_endAt_date
FROM adorders_pausehistory_tmp
  ,unnest(pauseHistory_startAt) as pauseHistory_startAt with offset as pos1
  ,unnest(pauseHistory_endAt) as pauseHistory_endAt with offset as pos2
where 1=1
and pos1=pos2
)
, adorder_with_pause AS (
  SELECT 
    adorders_id
    , lawyers_id
    , adorders_createdAt_date
    , adorders_start_date
    , adorders_end_date
    , pauseHistory_startAt_date
    , pauseHistory_endAt_date
    , adpayments_id
    , adorders_status
    , adpayments_status
    , adpayments_method
    , adLocations
    , keywords
    , categories
    , item
  FROM adorders_history AS A
  LEFT JOIN adorders_pausehistory AS B
  USING (adorders_id)
)
, adorders_item AS (
SELECT
  A.*
  ,B.name as item_name
  ,B.price
FROM adorder_with_pause AS A
left outer join `lawtalk-bigquery.raw.adproducts` as B on A.item = B._id
WHERE 1 = 1
)
, adorders_adloaction AS
(
  SELECT
    * EXCEPT(adLocations_id)
    , adLocations_id
  FROM adorders_item
   ,UNNEST(REGEXP_EXTRACT_ALL(adLocations, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)")) AS adLocations_id
)
,adlocations_name as
(
  SELECT
    A._id,
    A.name as adlocations_name,
    A.adLocationGroup as adLocationGroup_id,
    B.name as adLocationGroup_id_name
  FROM `lawtalk-bigquery.raw.adlocations` as A
  LEFT OUTER JOIN `lawtalk-bigquery.raw.adlocationgroups` as B on A.adLocationGroup = B._id
)
, adlocation AS (
  SELECT 
    A.adorders_id
    , adlocations_name
    , adLocationGroup_id
    , adLocationGroup_id_name
  FROM adorders_adloaction AS A
  LEFT JOIN adlocations_name AS B
  ON A.adLocations_id = B._id
)
, adkeywords_unnest AS (
    SELECT 
    _id as adkeywords_id
    ,name as adkeywords_name
    , adCategories_id
  FROM `lawtalk-bigquery.raw.adkeywords` AS adkeywords
    ,UNNEST(REGEXP_EXTRACT_ALL(adCategories, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)")) as adCategories_id  
)
, adkeywords_with_category AS (
  SELECT
    adcategories.name AS adCategories_name
    , adkeywords.adCategories_id
    , adkeywords.adkeywords_name
    , adkeywords.adkeywords_id
  FROM adkeywords_unnest AS adkeywords
  LEFT JOIN `lawtalk-bigquery.raw.adcategories` as adcategories on adkeywords.adCategories_id = adcategories._id
)
, adorders_keywords_tmp as
(
  SELECT
    * EXCEPT(keywords_id)
    , keywords_id
  FROM adorders_item
    ,UNNEST(regexp_extract_all(keywords, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)")) AS keywords_id
)
, adkeyword AS (
  SELECT
    A.adorders_id
    , B.*
  FROM adorders_keywords_tmp AS A
  LEFT JOIN adkeywords_with_category AS B
  ON A.keywords_id = B.adkeywords_id 
)
, adorders_item_adCategory_tmp AS (
  SELECT  
    * EXCEPT(adCategoryId)
    , adCategoryId
  FROM adorders_item
  , UNNEST(regexp_extract_all(categories, r"'adCategoryId': ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)"))  as adCategoryId
)
, adnormal AS (
SELECT
  A.adorders_id
  ,C.name as adCategory_name
FROM adorders_item_adCategory_tmp AS A
left outer join `lawtalk-bigquery.raw.adcategories` as C on A.adCategoryId = C._id
WHERE 1 = 1
)
, adorder_end AS (
SELECT * FROM adorders_item
LEFT JOIN adlocation USING (adorders_id)
LEFT JOIN adkeyword USING (adorders_id)
LEFT JOIN adnormal USING (adorders_id)
)
, adorder_lawyer AS (
SELECT 
   A.*
   , B.slug
FROM adorder_end AS A
LEFT JOIN `lawtalk-bigquery.raw.lawyers` AS B
ON A.lawyers_id = B._id
)
, tmp_table AS (
SELECT 
  slug
  , lawyers_id
  , item_name
  , adorders_start_date	
  , adorders_end_date	
  , pauseHistory_startAt_date	
  , pauseHistory_endAt_date
  , adorders_status
  , adpayments_status
  , CASE WHEN adorders_status = 'apply' AND adpayments_status = 'paid' THEN '광고주' ELSE '비광고주' END AS ad_tf
  , CASE WHEN adorders_status = 'apply' AND adpayments_status = 'paid' THEN 1 ELSE 0 END AS ad_cnt
FROM adorder_lawyer
WHERE 1 = 1
  AND '2022-07-01' BETWEEN adorders_start_date AND adorders_end_date	
  -- AND pauseHistory_startAt_date IS NOT NULL
  AND ('2022-07-01' NOT BETWEEN pauseHistory_startAt_date AND pauseHistory_endAt_date	OR pauseHistory_startAt_date IS NULL)
)
SELECT 
  slug
  , lawyers_id
  , STRING_AGG(ad_tf ORDER BY ad_tf LIMIT 1) AS ad_tf
  , SUM(ad_cnt) AS ad_cnt
FROM tmp_table
GROUP BY 1, 2
;
'''

advice_query = '''
WITH advice AS (
SELECT
  _id
  , lawyer AS lawyers_id
  , adCategory
  , createdAt
  , status
FROM `lawtalk-bigquery.raw.advice`
WHERE 1 = 1 
  AND status = 'complete'
  AND createdAt between '2022-07-01' AND '2022-07-15'
)
, adCategory AS (
SELECT
  _id AS adCategory
  , name AS adCategory_name
FROM `lawtalk-bigquery.raw.adcategories`
)
SELECT 
  _id
  , lawyers_id
  , adCategory
  , createdAt
  , status
  , adCategory_name
FROM advice A
LEFT JOIN adCategory B
USING (adCategory)
'''

ga_query = '''
SELECT 
	event_date	
	, event_timestamp
	, user_pseudo_id
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'lawyer_id') as lawyer_id 
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'search_lawyer_노출이유_단어') as expose_reason
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'search_keyword') as search_keyword
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id 
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_title') as page_title 
    , (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location 
FROM `lawtalk-bigquery.analytics_265523655.events_*`
WHERE 1 = 1 
	AND _TABLE_SUFFIX BETWEEN '20220701' AND '20220715'
	AND event_name = 'tag.search.프로필노출'
;
'''


adorder = bigquery_to_pandas(adorder_query)
advice = bigquery_to_pandas(advice_query)
ga = bigquery_to_pandas(ga_query)


def get_gini(df, val_col, color = "red", plot_tf = True) :
    tmp = df.copy()
    tmp["ratio"] = tmp[val_col] / tmp[val_col].max()
    tmp["rnk"] = tmp.ratio.rank(method = "first")  / tmp.shape[0]
    tmp = tmp.sort_values("rnk")
    tmp["eq_line"] = [i / tmp.shape[0] for i in range(tmp.shape[0])]
    
    lorenz = (tmp.rnk * tmp.shape[0] * (tmp.ratio / tmp.shape[0])).sum()
    tri = ((tmp.rnk * tmp.shape[0]).max() * tmp.rnk.max()) / 2
    
    if plot_tf == True :
        print((
            ggplot(data = tmp) +
            geom_col(mapping = aes(x = "rnk", y = "ratio"), fill = color, alpha = 0.6) +
            geom_line(mapping = aes(x = "rnk", y = "eq_line"), group = 1) +
            theme_bw() +
            theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
        ))
    
    return (tri - lorenz) / tri



ga.groupby("lawyer_id").user_pseudo_id.nunique().sort_values().tail()
ga.groupby("lawyer_id").event_date.count().sort_values().tail()
ga.search_keyword = ga.search_keyword.str.replace(pat = "~2F", repl = "/")
ga.search_keyword = ga.search_keyword.fillna("")
ga.space_len = ga.search_keyword.str.split(" |/", regex = True).apply(lambda x : len(x))

ga.search_keyword.nunique()

# 조건1 : 검색어와 검색 결과가 같지 않은(대분야 클릭으로 들어오지 않은) 검색어들
if1 = (ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") &  (~ga.search_keyword.str.contains("/"))
# 조건2 : 조건1 + 한단어(뛰어쓰기가 없도록)
if2 = (ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1) & (~ga.search_keyword.str.contains("/"))
# 조건3 : 조건1에 100개 이상 검색된 결과
if3 = (ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") &  (~ga.search_keyword.str.contains("/"))
# 조건4 : 조건2에 100개 이상 검색된 결과
if4 = (ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1) & (~ga.search_keyword.str.contains("/"))

ga[if1].search_keyword.value_counts()

ga[if2].search_keyword.value_counts().reset_index()

ga[if3].search_keyword.value_counts().reset_index().query("search_keyword > 100")

ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1) & (~ga.search_keyword.str.contains("/"))].search_keyword.value_counts().reset_index().query("search_keyword > 100")

ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") &  (~ga.search_keyword.str.contains("/"))].search_keyword.value_counts().reset_index().search_keyword.sum()

ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1) & (~ga.search_keyword.str.contains("/"))].search_keyword.value_counts().reset_index().search_keyword.sum()

ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") &  (~ga.search_keyword.str.contains("/"))].search_keyword.value_counts().reset_index().query("search_keyword > 100").search_keyword.sum()

ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1) & (~ga.search_keyword.str.contains("/"))].search_keyword.value_counts().reset_index().query("search_keyword > 100").search_keyword.sum()


(
    ggplot() +
    
)


ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "")].search_keyword.nunique()


ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1)].search_keyword.value_counts().reset_index()

ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1)].search_keyword.value_counts().reset_index().query("search_keyword > 100")


ga[(ga.search_keyword != '') & (ga.search_keyword != ga.expose_reason)].search_keyword.value_counts()
ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1)].search_keyword.value_counts().head(20)

ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1)].search_keyword.value_counts().reset_index().query("search_keyword > 100")

get_gini(ga[(ga.search_keyword != '') & (ga.search_keyword != ga.expose_reason)].search_keyword.value_counts().reset_index(), "search_keyword")
get_gini(ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1)].search_keyword.value_counts().reset_index(), "search_keyword")

get_gini(ga[(ga.search_keyword != ga.expose_reason) & (ga.search_keyword.notna()) & (ga.search_keyword != "") & (ga.space_len == 1)].search_keyword.value_counts().reset_index().query("search_keyword > 100"), "search_keyword")



get_gini(ga.groupby("lawyer_id").event_date.count().reset_index(), "event_date")
get_gini(ga.groupby("lawyer_id").user_pseudo_id.nunique().reset_index(), "user_pseudo_id")


"pull test"
