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

def gs_append(url, credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json', scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']) : 
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    
    sheet_content = sheet.get_all_values()
    
    df_ = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    return df_




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
  B._id AS lawyers_id
  , B.slug
  , A.item_name
  , A.adorders_start_date	
  , A.adorders_end_date	
  , A.pauseHistory_startAt_date	
  , A.pauseHistory_endAt_date
  , A.adorders_status
  , A.adpayments_status
FROM slug_lawyers AS B
FULL OUTER JOIN adorder_end AS A
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
  , CASE WHEN adorders_status = 'apply' AND adpayments_status = 'paid' AND '2022-07-01' BETWEEN adorders_start_date AND adorders_end_date AND ('2022-07-01' NOT BETWEEN pauseHistory_startAt_date AND pauseHistory_endAt_date	OR pauseHistory_startAt_date IS NULL) THEN '광고주' ELSE '비광고주' END AS ad_tf
  , CASE WHEN adorders_status = 'apply' AND adpayments_status = 'paid' AND '2022-07-01' BETWEEN adorders_start_date AND adorders_end_date AND ('2022-07-01' NOT BETWEEN pauseHistory_startAt_date AND pauseHistory_endAt_date	OR pauseHistory_startAt_date IS NULL) THEN 1 ELSE 0 END AS ad_cnt
FROM adorder_lawyer
WHERE 1 = 1
  -- AND '2022-07-01' BETWEEN adorders_start_date AND adorders_end_date
  -- AND pauseHistory_startAt_date IS NOT NULL
  -- AND ('2022-07-01' NOT BETWEEN pauseHistory_startAt_date AND pauseHistory_endAt_date	OR pauseHistory_startAt_date IS NULL)
)
SELECT 
  slug
  , lawyers_id
  , STRING_AGG(ad_tf ORDER BY ad_tf LIMIT 1) AS ad_tf
  , SUM(ad_cnt) AS ad_cnt
FROM tmp_table
GROUP BY 1, 2
ORDER BY 3 DESC
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


advicet_query = '''
WITH advicetransaction_tmp AS (
SELECT
  _id,
  row_number() over(PARTITION BY advice ORDER BY updatedAt DESC) AS no_by_advice,
  number, 
  DATETIME(createdAt, 'Asia/Seoul') as createdAt,
  DATETIME(updatedAt, 'Asia/Seoul') as updatedAt,
  DATETIME(requestedAt, 'Asia/Seoul') as requestedAt,
  advice,
  user,
  lawyer AS lawyer_id,
  seller,
  tid,
  status,
  method,
  originFee,
  canceled,
  coupon,
  metadata,
  REGEXP_EXTRACT(metadata, r"'page': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_page,
  REGEXP_EXTRACT(metadata, r"'source': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_source,
  REGEXP_EXTRACT(metadata, r"'element': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_element,
  REGEXP_EXTRACT(metadata, r"'position': ([0-9]+),") as metadata_position,
  REGEXP_EXTRACT(metadata, r"'context': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_context,
  REGEXP_EXTRACT(metadata, r"'contextAdditional': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_contextAdditional,
  REGEXP_EXTRACT(metadata, r"'extraInfo': '([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)'") as metadata_extraInfo,
FROM `lawtalk-bigquery.raw.advicetransactions`
where 1=1
  AND DATE(DATETIME(createdAt, 'Asia/Seoul')) BETWEEN '2022-07-01' AND '2022-07-15'
  AND status = 'paid'
)
, slug_lawyers AS (
SELECT 
  _id
  , slug
FROM `lawtalk-bigquery.raw.lawyers` AS B
WHERE 1 = 1
  AND REGEXP_CONTAINS(slug, r'\d{4}-[가-힣]+')
)
SELECT 
    A.*
    , B.slug
FROM advicetransaction_tmp AS A
LEFT JOIN slug_lawyers AS B
ON A.lawyer_id = B._id
WHERE 1 = 1
'''

ga_query = '''
SELECT 
	event_date	
	, event_timestamp
	, user_pseudo_id
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'lawyer_id') as slug 
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

gae_query = '''
SELECT 
	event_date	
	, event_timestamp
	, user_pseudo_id
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'keyword') as keyword
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'ga_session_id') as ga_session_id 
	, (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_title') as page_title 
  , (SELECT param.value.string_value FROM UNNEST(event_params) as param WHERE param.key = 'page_location') as page_location 
FROM `lawtalk-bigquery.analytics_265523655.events_*`
WHERE 1 = 1 
	AND _TABLE_SUFFIX BETWEEN '20220701' AND '20220715'
	AND event_name = 'tag.상단검색창직접입력'
'''

adorder = bigquery_to_pandas(adorder_query)
advice = bigquery_to_pandas(advice_query)
advicet = bigquery_to_pandas(advicet_query)
ga = bigquery_to_pandas(ga_query)
gae = bigquery_to_pandas(gae_query)
lawyer = bigquery_to_pandas(lawyer_query)

keyword = gs_append(url = 'https://docs.google.com/spreadsheets/d/1n9YKC3L9_d4QRKjlDqbHelxw5nC3Fz8hrgbHahOHhxM/edit#gid=0')
keyword.columns = ["type1", "type2", "search_keyword"]

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

adorder.ad_tf.value_counts()

ga_ad = pd.merge(ga, adorder[["slug", "ad_tf", "ad_cnt"]], on = "slug", how = "left")
ga_ad.loc[ga_ad.search_keyword.isin(keyword.search_keyword), "mapping"] = 1
ga_ad.loc[~ga_ad.search_keyword.isin(keyword.search_keyword), "mapping"] = 0

ga_ad.search_keyword = ga_ad.search_keyword.str.replace(pat = "~2F", repl = "/")
ga_ad.search_keyword = ga_ad.search_keyword.fillna("")
ga_ad.space_len = ga_ad.search_keyword.str.split(" |/", regex = True).apply(lambda x : len(x))

if1 = (ga_ad.search_keyword == ga_ad.expose_reason) & (ga_ad.search_keyword.notna()) & (ga_ad.search_keyword != "") &  (~ga_ad.search_keyword.str.contains("/")) & (~ga_ad.search_keyword.isin(lawyer.name))

if1_ = (ga_ad.search_keyword != ga_ad.expose_reason) & (ga_ad.search_keyword.notna()) & (ga_ad.search_keyword != "") &  (~ga_ad.search_keyword.str.contains("/")) & (~ga_ad.search_keyword.isin(lawyer.name))


# 조건1 : 검색어와 검색 결과가 같지 않고(대분야 클릭으로 들어오지 않은) 변호사 이름이 아닌 검색어들
if1 = (ga_ad.search_keyword != ga_ad.expose_reason) & (ga_ad.search_keyword.notna()) & (ga_ad.search_keyword != "") &  (~ga_ad.search_keyword.str.contains("/")) & (~ga_ad.search_keyword.isin(lawyer.name))
# 조건2 : 조건1 + 한단어(뛰어쓰기가 없도록)
if2 = (ga_ad.search_keyword != ga_ad.expose_reason) & (ga_ad.search_keyword.notna()) & (ga_ad.search_keyword != "") & (ga_ad.space_len == 1) & (~ga_ad.search_keyword.str.contains("/")) & (~ga_ad.search_keyword.isin(lawyer.name))
# 조건3 : 조건1에 100개 이상 검색된 결과
if3 = (ga_ad.search_keyword != ga.expose_reason) & (ga_ad.search_keyword.notna()) & (ga_ad.search_keyword != "") &  (~ga_ad.search_keyword.str.contains("/")) & (~ga_ad.search_keyword.isin(lawyer.name))
# 조건4 : 조건2에 100개 이상 검색된 결과
if4 = (ga_ad.search_keyword != ga_ad.expose_reason) & (ga_ad.search_keyword.notna()) & (ga_ad.search_keyword != "") & (ga_ad.space_len == 1) & (~ga_ad.search_keyword.str.contains("/")) & (~ga_ad.search_keyword.isin(lawyer.name))


ga_ad[if1].groupby(["search_keyword"]).slug.count().sort_values(ascending = False)
ga_ad[if2].groupby("search_keyword").slug.count().sort_values(ascending = False)
ga_ad[if3].groupby("search_keyword").slug.count().query("search_keyword > 100").sort_values(ascending = False)
ga_ad[if4].groupby("search_keyword").slug.count().query("search_keyword > 100").sort_values(ascending = False)

ga_ad[if1].groupby("mapping").user_pseudo_id.nunique()

ga_ad[if1 & (ga_ad.mapping == 1)].groupby("search_keyword").user_pseudo_id.nunique().sort_values(ascending = False)
ga_ad[if1 & (ga_ad.mapping == 0)].groupby("search_keyword").user_pseudo_id.nunique().sort_values(ascending = False)


ga[if1].groupby("search_keyword").slug.nunique().sort_values(ascending = False)
ga[if1].groupby("search_keyword").slug.count().sort_values(ascending = False)

ga[if1].search_keyword.value_counts().reset_index().search_keyword.sum()
ga[if2].search_keyword.value_counts().reset_index().search_keyword.sum()
ga[if3].search_keyword.value_counts().reset_index().query("search_keyword > 100").search_keyword.sum()
ga[if4].search_keyword.value_counts().reset_index().query("search_keyword > 100").search_keyword.sum()



get_gini(ga[if1].groupby("search_keyword").slug.nunique().reset_index(), "slug")
get_gini(ga[if1].groupby("search_keyword").slug.count().reset_index(), "slug")

get_gini(ga[if2].groupby("search_keyword").slug.nunique().reset_index(), "slug")

get_gini(ga[if2].search_keyword.value_counts().reset_index(), "search_keyword")
get_gini(ga[if3].search_keyword.value_counts().reset_index().query("search_keyword > 100"), "search_keyword")
get_gini(ga[if4].search_keyword.value_counts().reset_index().query("search_keyword > 100"), "search_keyword")

get_gini(ga.groupby("lawyer_id").event_date.count().reset_index(), "event_date")
get_gini(ga.groupby("slug").user_pseudo_id.nunique().reset_index(), "user_pseudo_id")

ga[if1].groupby("slug").event_date.count().sort_values(ascending = False)

get_gini(ga[(if1)].groupby("lawyer_id").event_date.count().reset_index(), "event_date")
get_gini(ga[if2].groupby("lawyer_id").event_date.count().reset_index(), "event_date")

get_gini(ga[(if1)].groupby("slug").event_date.count().reset_index(), "event_date")
get_gini(ga[if2].groupby("slug").event_date.count().reset_index(), "event_date")






''''''
if1 = (advicet.metadata_contextAdditional != advicet.metadata_extraInfo) & (~advicet.metadata_extraInfo.isin(lawyer.name)) & (~advicet.metadata_extraInfo.fillna("").str.contains("/"))

advicet.metadata_context.value_counts()


# 5177 -> 1550 약 30%
advicet[(advicet.metadata_context == 'category') & (advicet.metadata_extraInfo != advicet.metadata_contextAdditional)].metadata_contextAdditional.value_counts()[:10]

for i in advicet[(advicet.metadata_context == 'category') & (advicet.metadata_extraInfo != advicet.metadata_contextAdditional)].metadata_contextAdditional.value_counts()[:10].index :
    print(advicet[advicet.metadata_contextAdditional.fillna("").str.contains(i)].shape[0])

'''
'''


get_gini(advicet[advicet.metadata_contextAdditional.fillna("").str.contains('임대차')].groupby("lawyer_id")._id.count().reset_index(), "_id")

advicet[advicet.metadata_contextAdditional.fillna("").str.contains('폭행/협박/상해 일반')]
advicet[advicet.metadata_contextAdditional.fillna("").str.contains('임대차')]

advicet[if1]
advicet[if1 & (advicet.metadata_extraInfo.isin(keyword.search_keyword))]

advicet[if1].metadata_extraInfo.value_counts()[:100]
advicet[advicet.metadata_extraInfo == "통신매체이용음란죄"].metadata_contextAdditional.value_counts()

advice


for i in list(advicet[if1].metadata_extraInfo.value_counts()[:10].index) :
    print(advicet[advicet.metadata_extraInfo == i].groupby("slug")._id.count().sort_values(ascending = False)[:1])    
    get_gini(advicet[advicet.metadata_extraInfo == i].groupby("slug")._id.count().reset_index(), "_id")


