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

import gspread
from oauth2client.service_account import ServiceAccountCredentials

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






advice_query = '''
WITH advice AS (
SELECT
  _id
  , lawyer AS lawyer_id
  , adCategory
  , createdAt
  , status
FROM `lawtalk-bigquery.raw.advice`
WHERE 1 = 1 
  -- AND status = 'complete'
  -- AND createdAt between '2022-07-01' AND '2022-07-15'
)
, adCategory AS (
SELECT
  _id AS adCategory
  , name AS adCategory_name
FROM `lawtalk-bigquery.raw.adcategories`
)
SELECT 
  _id
  , lawyer_id
  , adCategory
  , createdAt
  , status
  , adCategory_name
FROM advice A
LEFT JOIN adCategory B
USING (adCategory)
'''


adorder_query = '''
WITH A AS (
      SELECT
      _id as adorders_id
      ,lawyer as lawyer_id
      ,status as adorders_status
      ,date(DATETIME(createdAt,'Asia/Seoul')) as adorders_createdAt_date
      ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul')  as adorders_startAt_timestamp
      ,DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'endAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul') as adorders_endAt_timestamp
      , adLocations
      , keywords
      , categories
      , item
    from `lawtalk-bigquery.raw.adorders`
    where 1 = 1 
      AND DATE(createdAt) <= '2022-12-31'
      AND DATE(DATETIME(parse_timestamp('%Y, %m, %e, %H, %M', regexp_extract(term, r"'startAt': datetime.datetime\((\d{4}, \d{1,2}, \d{1,2}, \d{1,2}, \d{1,2})")), 'Asia/Seoul')) >= '2022-01-01'
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
    , A.lawyer_id
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
    , lawyer_id
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
    , adLocations_name
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
, slug_lawyers AS (
SELECT 
  _id
  , slug
FROM `lawtalk-bigquery.raw.lawyers` AS B
WHERE 1 = 1
  AND REGEXP_CONTAINS(slug, r'\d{4}-[가-힣]+')
)
, adorder_lawyer AS (
SELECT 
  B._id AS lawyer_id
  , B.slug
  , A.adorders_id
  , A.item_name
  , A.adorders_start_date	
  , A.adorders_end_date	
  , A.pauseHistory_startAt_date	
  , A.pauseHistory_endAt_date
  , A.adorders_status
  , A.adpayments_status
  , A.adLocations_name
  , A.adLocationGroup_id_name
  , A.adCategory_name
  , A.adkeywords_name
  , A.price
FROM slug_lawyers AS B
FULL OUTER JOIN adorder_end AS A
ON A.lawyer_id = B._id
)
SELECT * FROM adorder_lawyer
;
'''

advice = bigquery_to_pandas(advice_query)
adorders = bigquery_to_pandas(adorder_query)

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


def ad_active(df, d) :
    return (df.adorders_start_date <= d) & (df.adorders_end_date >= d) & ~((df.pauseHistory_startAt_date <= d) & (df.pauseHistory_endAt_date >= d)) & (df.adorders_status == 'apply') & (df.adpayments_status == 'paid')

def ad_active_dates(df, date_list) :
    return pd.concat([df[ad_active(df, d)] for d in date_list]).drop_duplicates()

date_range = pd.date_range(datetime.date(2022, 7, 1), datetime.date(2022, 7, 15))


extract_adorders = ad_active_dates(adorders, date_range)
extract_advice = advice[(advice.adCategory_name != '기타') & (advice.createdAt.dt.date.isin(date_range.date))]

def prefix_rnk(df, str_cols, y_cols, zfill_ns = 2) :
    return df[y_cols].rank(ascending = False).astype(int).astype(str).str.zfill(zfill_ns) + "_" + df[str_cols]


tmp = pd.merge(ad_active_dates(adorders, date_range).groupby("adCategory_name").slug.nunique().sort_values(ascending = False).reset_index().rename(columns = {"slug" : "lawyer_cnt"}), advice[(advice.adCategory_name != '기타') & (advice.createdAt.dt.date.isin(date_range.date))].groupby("adCategory_name")._id.count().sort_values(ascending = False).reset_index().rename(columns = {"_id" : "advice_cnt"}), on = "adCategory_name")


tmp["ratio"] = tmp.advice_cnt / tmp.lawyer_cnt
tmp["rnk_category"] = prefix_rnk(tmp, "adCategory_name", "ratio")
tmp_ = pd.melt(tmp.drop(columns = "adCategory_name"), id_vars = "rnk_category")


(
    ggplot() +
    geom_col(data = tmp_, mapping = aes(x = "rnk_category", y = "value", fill = "variable")) +
    facet_grid("variable~.", scales = "free_y") +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)



for idx, i in enumerate(extract_advice.groupby("adCategory_name")._id.count().sort_values(ascending = False).index) :
    
    
    # 해당 분야 광고 주
    chk1 = extract_adorders[extract_adorders.adCategory_name == i]
    
    # 해당 분야 유료 상담 변호사
    chk2 = extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index()
    
    # 해당 분야 광고 주 중 유료 상담 변호사
    chk3 = chk1[chk1.lawyer_id.isin(chk2.lawyer_id)]
    
    print("{}.{} 분야 총 광고 변호사 수 / 상담 변호사 수 / 광고 변호사 중 유료 상담 변호사 = {} / {} / {}".format(idx, i, chk1.lawyer_id.nunique(), chk2.lawyer_id.nunique(), chk3.lawyer_id.nunique()))
    
    print("{} 분야 총 상담 수 = {}".format(i, extract_advice[extract_advice.adCategory_name == i].shape[0]))
    
    print("{} 분야 불평등도 = {}".format(i, get_gini(extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index(), "_id")))
    
    print("-"*50)
    

