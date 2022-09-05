'''
광고 무료화 : 2022/02/16 ~ 2022.06.31
분야개편 : 2022.07.01 ~


분야 70여개-> 40개로 줄어든 후 성과에 대한 측정
- 분야 별 광고료가 균등한 것과 유료 상담의 건수가 불균등하진 않은지
    - 분야별 광고 변호사 수
    - 분야별 유료 상담건 수
    - 분야별 광고 변호사 인당 유료 상담 건수
- 분야 개편으로 인하여 파레토(특정 변호사의 독식)이 없어졌는지
    - 분야별 gini 계수 - 관련하여 용호님/대건님께서 리서치해주신 자료https://www.notion.so/lawcompany/b21d10a9b4834c09bbb73b35cba8becf
    - 지속적으로 분야 개편에서 독식, 의뢰인 편향들이 생기지 않는지 검사

'''

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

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import matplotlib as mpl
import matplotlib.pyplot as plt 
import matplotlib.font_manager as fm  

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

def gs_read(url, credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json', scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']) : 
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    sheet_content = sheet.get_all_values()
    
    df_ = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    return df_



query = '''
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
-- SELECT * FROM adorders_item 
-- SELECT * FROM adlocation -- adLocations_id,	adlocations_name,	adLocationGroup_id,	adLocationGroup_id_name
-- SELECT * FROM adkeyword -- 	keywords_id,	adCategories_name,	adCategories_id,	adkeywords_name,	adkeywords_id
-- SELECT * FROM adnormal -- adCategoryId	adCategory_name
-- SELECT COUNT(DISTINCT adorders_id) FROM adorders_item 
-- SELECT COUNT(DISTINCT adorders_id) FROM adlocation -- adLocations_id,	adlocations_name,	adLocationGroup_id,	adLocationGroup_id_name
-- SELECT COUNT(DISTINCT adorders_id) FROM adkeyword -- 	keywords_id,	adCategories_name,	adCategories_id,	adkeywords_name,	adkeywords_id
-- SELECT COUNT(DISTINCT adorders_id) FROM adnormal -- adCategoryId	adCategory_name
SELECT * FROM adorders_item
LEFT JOIN adlocation USING (adorders_id)
LEFT JOIN adkeyword USING (adorders_id)
LEFT JOIN adnormal USING (adorders_id)
;

'''


query_ = '''
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
    from `lawtalk-bigquery.raw_old.adorders`
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
FROM `lawtalk-bigquery.raw_old.adorders`
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
  FROM `lawtalk-bigquery.raw_old.adkeywords` AS adkeywords
    ,UNNEST(REGEXP_EXTRACT_ALL(adCategories, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)")) as adCategories_id  
)
, adkeywords_with_category AS (
  SELECT
    adcategories.name AS adCategories_name
    , adkeywords.adCategories_id
    , adkeywords.adkeywords_name
    , adkeywords.adkeywords_id
  FROM adkeywords_unnest AS adkeywords
  LEFT JOIN `lawtalk-bigquery.raw_old.adcategories` as adcategories on adkeywords.adCategories_id = adcategories._id
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
left outer join `lawtalk-bigquery.raw_old.adcategories` as C on A.adCategoryId = C._id
WHERE 1 = 1
)
-- SELECT * FROM adorders_item 
-- SELECT * FROM adlocation -- adLocations_id,	adlocations_name,	adLocationGroup_id,	adLocationGroup_id_name
-- SELECT * FROM adkeyword -- 	keywords_id,	adCategories_name,	adCategories_id,	adkeywords_name,	adkeywords_id
-- SELECT * FROM adnormal -- adCategoryId	adCategory_name
-- SELECT COUNT(DISTINCT adorders_id) FROM adorders_item 
-- SELECT COUNT(DISTINCT adorders_id) FROM adlocation -- adLocations_id,	adlocations_name,	adLocationGroup_id,	adLocationGroup_id_name
-- SELECT COUNT(DISTINCT adorders_id) FROM adkeyword -- 	keywords_id,	adCategories_name,	adCategories_id,	adkeywords_name,	adkeywords_id
-- SELECT COUNT(DISTINCT adorders_id) FROM adnormal -- adCategoryId	adCategory_name
SELECT * FROM adorders_item
LEFT JOIN adlocation USING (adorders_id)
LEFT JOIN adkeyword USING (adorders_id)
LEFT JOIN adnormal USING (adorders_id)
;
'''

def pd_datetime_cols(df, cols) :
    for c in cols :
        df[c] = pd.to_datetime(df[c])
    
    return df


def active_adorder(df, idx_datetime) :
    return ((df.adorders_start_date <= idx_datetime) & (df.adorders_end_date >= idx_datetime)) & ~((df.pauseHistory_startAt_date <= idx_datetime) & (df.pauseHistory_endAt_date >= idx_datetime)) & (df.adorders_status == "apply") & (df.adpayments_status == "paid")


view_cols = ["adorders_id", "item_name", "lawyers_id", "adCategory_name", 'adorders_createdAt_date', 'adorders_start_date', 'adorders_end_date', 'adorders_status', 'adpayments_status', 'pauseHistory_startAt_date', 'pauseHistory_endAt_date']


adorder_new = bigquery_to_pandas(query)
adorder_old = bigquery_to_pandas(query_)

adorder_new = pd_datetime_cols(adorder_new, ["adorders_createdAt_date", "adorders_start_date", "adorders_end_date", "pauseHistory_startAt_date", "pauseHistory_endAt_date"])
adorder_old = pd_datetime_cols(adorder_old, ["adorders_createdAt_date", "adorders_start_date", "adorders_end_date", "pauseHistory_startAt_date", "pauseHistory_endAt_date"])


adorder_new = adorder_new[adorder_new.adorders_start_date >= datetime.datetime(2022, 1, 1)]
adorder_old = adorder_old[adorder_old.adorders_start_date >= datetime.datetime(2022, 1, 1)]

layoff_a = adorder_new[active_adorder(adorder_new, datetime.datetime(2022, 7, 1))]
layoff_b = adorder_old[active_adorder(adorder_old, datetime.datetime(2022, 6, 1))]


ads = pd.concat([layoff_a.assign(tp = "a"), layoff_b.assign(tp = "b")])

ads.groupby(["tp", "adCategory_name"]).lawyers_id.nunique()

from plotnine import *



(
    ggplot() +
    geom_col(data = ads[ads.tp == 'a'].groupby(["adCategory_name"]).lawyers_id.nunique().reset_index().assign(adCategory_name = lambda x : x.lawyers_id.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.adCategory_name), mapping = aes(x = "adCategory_name", y = "lawyers_id"), fill = "red", alpha = 0.6) +
    theme_bw() +
    theme(legend_position= "none", figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)


(
    ggplot() +
    geom_col(data = ads[ads.tp == 'b'].groupby(["adCategory_name"]).lawyers_id.nunique().reset_index().assign(adCategory_name = lambda x : x.lawyers_id.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.adCategory_name), mapping = aes(x = "adCategory_name", y = "lawyers_id"), fill = "red", alpha = 0.6) +
    theme_bw() +
    theme(legend_position= "none", figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)



query = '''
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

query_ = '''
WITH advice AS (
SELECT
  _id
  , lawyer AS lawyers_id
  , adCategory
  , createdAt
  , status
FROM `lawtalk-bigquery.raw_old.advice`
WHERE 1 = 1 
  AND status = 'complete'
  AND createdAt between '2022-06-01' AND '2022-06-15'
)
, adCategory AS (
SELECT
  _id AS adCategory
  , name AS adCategory_name
FROM `lawtalk-bigquery.raw_old.adcategories`
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

advice_new = bigquery_to_pandas(query)
advice_old = bigquery_to_pandas(query_)

advice = pd.concat([advice_new.assign(tp = "a"), advice_old.assign(tp = "b")])



(
    ggplot() +
    geom_col(data = advice[(advice.tp == 'a') & (advice.adCategory_name != '기타')].groupby(["adCategory_name"])._id.nunique().reset_index().assign(adCategory_name = lambda x : x._id.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.adCategory_name), mapping = aes(x = "adCategory_name", y = "_id"), fill = "red", alpha = 0.6) +
    theme_bw() +
    theme(legend_position= "none", figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)

pd.melt(advice[(advice.tp == 'a') & (advice.adCategory_name != '기타')].groupby(["adCategory_name"]).agg(n_advice = ("_id", lambda x : x.nunique()), n_lawyer = ("lawyers_id", lambda x : x.nunique())).reset_index().assign(adCategory_name = lambda x : x.n_advice.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.adCategory_name), id_vars = "adCategory_name")

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler


mm_scaler = MinMaxScaler()

tmp = advice[(advice.tp == 'a') & (advice.adCategory_name != '기타')].groupby(["adCategory_name"]).agg(n_advice = ("_id", lambda x : x.nunique()), n_lawyer = ("lawyers_id", lambda x : x.nunique())).reset_index().assign(ratio = lambda x : x.n_advice / x.n_lawyer * 100)

tmp[["n_advice_mm", "n_lawyer_mm", "ratio_mm"]] = mm_scaler.fit_transform(tmp[["n_advice", "n_lawyer", "ratio"]])

tmp = pd.melt(tmp, id_vars = "adCategory_name")
tmp.assign(adCategory_name = lambda x : x.n_advice.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.adCategory_name)

(
    ggplot() +
    geom_col(data = tmp[tmp.variable.isin(["n_advice_mm", "n_lawyer_mm", "ratio_mm"])], mapping = aes(x = "adCategory_name", y = "value", fill = "variable"), alpha = 0.6, position = "dodge") +
    facet_grid("variable~.") +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)

tmp_advice = tmp[tmp.variable == "n_advice"]
tmp_advice["ratio"] = tmp_advice.value / tmp_advice.value.max()
tmp_advice["rnk"] = tmp_advice.ratio.rank(method = "first")  / tmp_advice.shape[0]
tmp_advice = tmp_advice.sort_values("rnk")
tmp_advice["eq_line"] = [i / tmp_advice.shape[0] for i in range(tmp_advice.shape[0])]

(
    ggplot(data = tmp_advice) +
    geom_col(mapping = aes(x = "rnk", y = "ratio")) +
    geom_line(mapping = aes(x = "rnk", y = "eq_line"), group = 1)
)

(tmp_advice.rnk * tmp_advice.shape[0] * (tmp_advice.ratio / tmp_advice.shape[0])).sum()

((tmp_advice.rnk * tmp_advice.shape[0]).max() * tmp_advice.rnk.max()) / 2

(((tmp_advice.rnk * tmp_advice.shape[0]).max() * tmp_advice.rnk.max() / 2) - ((tmp_advice.rnk * tmp_advice.shape[0] * (tmp_advice.ratio / tmp_advice.shape[0])).sum())) / ((tmp_advice.rnk * tmp_advice.shape[0]).max() * tmp_advice.rnk.max() / 2)


tmp_ = advice[(advice.tp == 'b') & (advice.adCategory_name != '기타')].groupby(["adCategory_name"]).agg(n_advice = ("_id", lambda x : x.nunique()), n_lawyer = ("lawyers_id", lambda x : x.nunique())).reset_index().assign(ratio = lambda x : x.n_advice / x.n_lawyer * 100)

tmp_[["n_advice_mm", "n_lawyer_mm", "ratio_mm"]] = mm_scaler.fit_transform(tmp_[["n_advice", "n_lawyer", "ratio"]])

tmp_ = pd.melt(tmp_, id_vars = "adCategory_name")


(
    ggplot() +
    geom_col(data = tmp_[tmp_.variable.isin(["n_advice_mm", "n_lawyer_mm", "ratio_mm"])], mapping = aes(x = "adCategory_name", y = "value", fill = "variable"), alpha = 0.6, position = "dodge") +
    facet_grid("variable~.") +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)

tmp_advice = tmp_[tmp_.variable == "n_advice"]
tmp_advice["ratio"] = tmp_advice.value / tmp_advice.value.max()
tmp_advice["rnk"] = tmp_advice.ratio.rank(method = "first")  / tmp_advice.shape[0]
tmp_advice = tmp_advice.sort_values("rnk")
tmp_advice["eq_line"] = [i / tmp_advice.shape[0] for i in range(tmp_advice.shape[0])]

(
    ggplot(data = tmp_advice) +
    geom_col(mapping = aes(x = "rnk", y = "ratio")) +
    geom_line(mapping = aes(x = "rnk", y = "eq_line"), group = 1)
)

(tmp_advice.rnk * tmp_advice.shape[0] * (tmp_advice.ratio / tmp_advice.shape[0])).sum()

((tmp_advice.rnk * tmp_advice.shape[0]).max() * tmp_advice.rnk.max()) / 2

(((tmp_advice.rnk * tmp_advice.shape[0]).max() * tmp_advice.rnk.max() / 2) - ((tmp_advice.rnk * tmp_advice.shape[0] * (tmp_advice.ratio / tmp_advice.shape[0])).sum())) / ((tmp_advice.rnk * tmp_advice.shape[0]).max() * tmp_advice.rnk.max() / 2)



(
    ggplot() +
    geom_col(data = pd.melt(advice[(advice.tp == 'a') & (advice.adCategory_name != '기타')].groupby(["adCategory_name"]).agg(n_advice = ("_id", lambda x : x.nunique()), n_lawyer = ("lawyers_id", lambda x : x.nunique())).reset_index().assign(adCategory_name = lambda x : x.n_advice.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.adCategory_name).assign(ratio = lambda x : x.n_advice / x.n_lawyer).loc[:, ["ratio", "adCategory_name"]], id_vars = "adCategory_name"), mapping = aes(x = "adCategory_name", y = "value", fill = "variable"), alpha = 0.6, position = "dodge") +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)


(
    ggplot() +
    geom_col(data = advice[advice.tp == 'b'].groupby(["adCategory_name"])._id.nunique().reset_index().assign(adCategory_name = lambda x : x._id.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.adCategory_name), mapping = aes(x = "adCategory_name", y = "_id"), fill = "red", alpha = 0.6) +
    theme_bw() +
    theme(legend_position= "none", figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)



(
    ggplot() +
    geom_col(data = advice[(advice.tp == 'a') & (advice.adCategory_name == "임대차")].groupby(["lawyers_id"])._id.nunique().reset_index().assign(lawyers_id = lambda x : x._id.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.lawyers_id), mapping = aes(x = "lawyers_id", y = "_id"), fill = "red", alpha = 0.6) +
    theme_bw() +
    theme(legend_position= "none", figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)



(
    ggplot() +
    geom_col(data = advice[(advice.tp == 'b') & (advice.adCategory_name == "성범죄")].groupby(["adCategory_name", "lawyers_id"])._id.nunique().reset_index().assign(lawyers_id = lambda x : x._id.rank(ascending = False).astype(int).astype(str).str.zfill(2) + "_" + x.lawyers_id), mapping = aes(x = "lawyers_id", y = "_id"), fill = "red", alpha = 0.6) +
    theme_bw() +
    theme(legend_position= "none", figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)

