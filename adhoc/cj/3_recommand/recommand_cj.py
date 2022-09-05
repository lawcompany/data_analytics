# -*- coding: utf-8 -*-
'''
작성자 : 김찬준

update :
    1. 대상 의로인 : 무료 상담페이지 방문 5개 이상 -> 2개 이상
        상담글 1개 본 방문자의 비율 : 73.64%
        상담글 2개 본 방문자의 비율 : 12.99%
        상담글 3개 본 방문자의 비율 : 4.72% 
        상담글 4개 본 방문자의 비율 : 2.33% 
        상담글 5개 본 방문자의 비율 : 1.39%
    2. implicit matrix : 페이지 방문수 -> 일단위 방문 수
        - 하루에 여러번 보는 것을 다 카운트하면 이상치가 들어갈 것이라 생각
        
        # 기존 페이지 방문 수 describe
        count   7070527.00
        mean    3.76      
        std     3.12      
        min     1.00      
        10%     1.00      
        20%     1.00      
        30%     2.00      
        40%     3.00      
        50%     4.00      
        60%     4.00      
        70%     5.00      
        80%     5.00      
        90%     6.00      
        max     1475.00   
        
        
        # 상담 페이지뷰 수 상위 10개
        2983730    1475
        6618141    1241
        4465       932 
        3087728    725 
        5222512    651 
        2328970    604 
        3814964    576 
        7013027    466 
        4435426    355 
        3100593    328 
        
        # 페이지 방문 일수 describe
        count   4588085.00
        mean    1.05      
        std     0.34      
        min     1.00      
        10%     1.00      
        20%     1.00      
        30%     1.00      
        40%     1.00      
        50%     1.00      
        60%     1.00      
        70%     1.00      
        80%     1.00      
        90%     1.00      
        max     127.00  
        
        # 페이지 방문 일수 상위 10개
        2322348    127
        2921191    93 
        1878881    90 
        1008343    73 
        2895700    71 
        3470514    71 
        2334550    65 
        2544124    53 
        2861716    48 
        1052578    46 

'''
import os
import sys
import warnings

from glob import glob
import random
import time
import datetime
import re
import json
import logging
from tqdm import tqdm

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, mean_squared_error

from google.cloud import bigquery

from datetime import datetime, timedelta
from lightfm import LightFM
from numpy import ndarray

from typing import Dict, List, Tuple



pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 10)
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



def map_index_2_qna(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict, Dict]:
    upi2index = {}
    index2upi = {}
    qna2index = {}
    index2qna = {}

    for i, upi in enumerate(df["user_pseudo_id"].unique()):
        upi2index[upi] = i
        index2upi[i] = upi

    for i, qna in enumerate(df["n_qna"].unique()):
        qna2index[qna] = i
        index2qna[i] = int(qna)

    df["index"] = df["user_pseudo_id"].apply(upi2index.get)
    df["qna"] = df["n_qna"].apply(qna2index.get)
    df = df[["index", "qna", "cnt"]]
    df = df.astype(np.int32)

    return df, qna2index, index2qna


def get_qna_mapper(qna_categories: pd.DataFrame) -> Tuple[Dict, Dict]:
    """
    MAP<qna_id, List[qna_id]>를 미리 만들어두고 활용하기에는 데이터 규모가 너무 크기에 (18Gb+),
    두 가지 map을 만들어서 runtime에 O(1) lookup을 잘 활용하는 방안을 사용한다.
    """
    # MAP<category_id, List[qna_id]>
    category_items = {}
    for category_id, group in qna_categories.explode("category_id").groupby(
        "category_id"
    ):
        category_items[category_id] = group["qna_id"].tolist()

    # MAP<qna_id, List[category_id]>
    qna2categories = {}
    for _, row in qna_categories.iterrows():
        qna2categories[row["qna_id"]] = row["category_id"].tolist()

    return category_items, qna2categories


def get_matrix(df):
    import scipy.sparse as sp

    implicit_matrix = sp.lil_matrix((df["index"].max() + 1, df["qna"].max() + 1))
    for row in df.itertuples(index=False):
        index = getattr(row, "index")
        qna = getattr(row, "qna")
        cnt = getattr(row, "cnt")
        implicit_matrix[index, qna] = cnt
    implicit_csr = implicit_matrix.tocsr()
    return implicit_csr


def train_model(implicit_csr):
    # config :
    no_components = 75
    learning_rate = 0.05
    epoch = 1

    model: LightFM = LightFM(
        no_components=no_components, learning_rate=learning_rate, loss="bpr"
    )
    model.fit(implicit_csr, epochs=epoch, verbose=True)
    _item_biases, item_embedding = model.get_item_representations()
    tag_embeddings = (item_embedding.T / np.linalg.norm(item_embedding, axis=1)).T
    return tag_embeddings


def inference(tag_embeddings, index2qna, category_items, qna2categories):

    bqclient = bigquery.Client()

    PROJECT = "lawtalk-bigquery"
    DATASET = "raw"
    TABLE = "recommend_qna"
    table = bqclient.get_table(f"{PROJECT}.{DATASET}.{TABLE}")

    get_qna_by_index = np.vectorize(lambda x: int(index2qna[x]))

    for i, b in enumerate(batch(np.arange(tag_embeddings.shape[0]), batch_size=1000)):
        logger.info(f"Running batch {i}")
        result = get_similar_tags(tag_embeddings, b)
        query = get_qna_by_index(b)
        result = get_qna_by_index(result)
        recs = []
        for q, r in zip(query, result):
            pool = get_pool_by_category(q, category_items, qna2categories)

            tmp_r = list(np.intersect1d(r, pool))
            if len(tmp_r) > TOPK:
                r = tmp_r
            recs.extend(
                recommend(
                    itemid=q,
                    items=r,
                    inferenced_at=prefect.context.get("scheduled_start_time"),
                )
            )
        query = list(map(str, query))

        logger.info("Inserting new results")
        errors = bqclient.insert_rows_json(table, recs)
        if errors == []:
            logger.info("success")
    
    return



# 5개 이상의 질문글을 본 유저의 유저 당 질문을 읽은 카운트값
qna_counts_query = '''
WITH qna_events AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        udfs.param_value_by_key('page_location',event_params).string_value as page_location
    FROM
        `analytics_265523655.*`
    WHERE TRUE
        AND udfs.param_value_by_key('page_location',event_params).string_value like '%/qna/%'
        AND udfs.param_value_by_key('page_location',event_params).string_value not like '%compose%'
        AND udfs.param_value_by_key('page_location',event_params).string_value not like '%complete%'
), qna_counts AS (
    SELECT
        user_pseudo_id, count(distinct n_qna) as cnt
    FROM (
        SELECT
            user_pseudo_id, CAST(REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') AS BIGINT) as n_qna
        FROM
            qna_events
        WHERE
            REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') is not null
    )
    GROUP BY 1
)

SELECT * FROM qna_counts
'''

query = """
WITH qna_events AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        DATE(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS day,
        udfs.param_value_by_key('page_location',event_params).string_value as page_location
    FROM
        `analytics_265523655.*`
    WHERE TRUE
        AND udfs.param_value_by_key('page_location',event_params).string_value like '%/qna/%'
        AND udfs.param_value_by_key('page_location',event_params).string_value not like '%compose%'
        AND udfs.param_value_by_key('page_location',event_params).string_value not like '%complete%'
), qna_counts AS (
    SELECT
        user_pseudo_id, count(distinct n_qna) as cnt
    FROM (
        SELECT
            user_pseudo_id, CAST(REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') AS BIGINT) as n_qna
        FROM
            qna_events
        WHERE
            REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') is not null
    )
    GROUP BY 1
)
SELECT
    b.*
FROM
    qna_counts a
    LEFT JOIN (
        SELECT
            user_pseudo_id, n_qna, count(DISTINCT day) AS cnt
        FROM (
            SELECT
                user_pseudo_id, REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') as n_qna, day
            FROM
                qna_events
            WHERE
                REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') is not null
        )
        GROUP BY
            1, 2
    ) b ON a.user_pseudo_id = b.user_pseudo_id
WHERE
    a.cnt >= 2
"""


# 상담사례의 카테고리와 제목
'''
# 네이버 노출 상담 사례
origin: # 네이버 질문 필터링
  $ne: 'naver-kin'
rejectFlag: false,
'answers.0': { '$exists': true }
'''

qna_query = """
SELECT
    CAST(SPLIT(slug,'-')[OFFSET(0)] AS BIGINT) AS qna_id,
    createdAt,
    origin,
    udfs.extract_object_ids(categories)        AS category_id,
    title                                      AS title,
    body                                       AS body,
    ARRAY_LENGTH(REGEXP_EXTRACT_ALL(answers, r"ObjectId\('(.*?)'\)")) AS answer_cnt
FROM `lawtalk-bigquery.raw.questions`
WHERE 1 = 1
  AND origin != 'naver-kin'
  AND rejectFlag = false
"""

categories_query = '''
SELECT
    _id AS category_id
    , name AS category_name
FROM `lawtalk-bigquery.raw.adcategories`
'''


# 2920926
df = bigquery_to_pandas(query)
qna_counts = bigquery_to_pandas(qna_counts_query)
qna = bigquery_to_pandas(qna_query)
categories = bigquery_to_pandas(categories_query)

df.cnt.describe(percentiles = [(i + 1) * 0.1 for i in range(9)])
df.cnt.sort_values(ascending = False).head(10)

qna_expose = qna[qna.answer_cnt > 1]

qna_features = qna_expose[["qna_id", "category_id"]].explode("category_id").sort_values(["qna_id", "category_id"])
qna_features["category_rnk"] = qna_features.groupby(["qna_id"]).cumcount()
qna_features = qna_features.pivot(index = "qna_id", columns = "category_rnk", values = "category_id")
qna_features.reset_index(inplace = True)
# qna_features[qna_features[0].notnull()]

df[df.n_qna.isin(qna_expose.qna_id)]

# df, qna2index, index2qna = map_index_2_qna(df)
upi2index = {}
index2upi = {}
qna2index = {}
index2qna = {}


for i, upi in enumerate(df["user_pseudo_id"].unique()):
    upi2index[upi] = i
    index2upi[i] = upi

for i, qna in enumerate(df["n_qna"].unique()):
    qna2index[qna] = i
    index2qna[i] = int(qna)


df["index"] = df["user_pseudo_id"].apply(upi2index.get)
df["qna"] = df["n_qna"].apply(qna2index.get)
# df = df[["index", "qna", "cnt"]]
df[["index", "qna", "cnt"]] = df[["index", "qna", "cnt"]].astype(np.int32)


# category_items, qna2categories = get_qna_mapper(qna_categories)

"""
MAP<qna_id, List[qna_id]>를 미리 만들어두고 활용하기에는 데이터 규모가 너무 크기에 (18Gb+),
두 가지 map을 만들어서 runtime에 O(1) lookup을 잘 활용하는 방안을 사용한다.
"""
# MAP<category_id, List[qna_id]>
category_items = {}
for category_id, group in qna_categories.explode("category_id").groupby("category_id"):
    category_items[category_id] = group["qna_id"].tolist()

# MAP<qna_id, List[category_id]>
qna2categories = {}
for _, row in qna_categories.iterrows() :
    qna2categories[row["qna_id"]] = row["category_id"].tolist()


# implicit_csr = get_matrix(df)
import scipy.sparse as sp
from sklearn.model_selection import train_test_split

train, test = train_test_split(df[["index", "qna", "cnt"]], test_size = 0.2)

# implicit_matrix = sp.lil_matrix((df["index"].max() + 1, df["qna"].max() + 1))
implicit_matrix_train = sp.lil_matrix((train["index"].max() + 1, train["qna"].max() + 1))
implicit_matrix_test = sp.lil_matrix((test["index"].max() + 1, test["qna"].max() + 1))

for row in train.itertuples(index=False) :
    index = getattr(row, "index")
    qna = getattr(row, "qna")
    cnt = getattr(row, "cnt")
    implicit_matrix_train[index, qna] = cnt

for row in test.itertuples(index=False) :
    index = getattr(row, "index")
    qna = getattr(row, "qna")
    cnt = getattr(row, "cnt")
    implicit_matrix_test[index, qna] = cnt

    
implicit_csr_train = implicit_matrix_train.tocsr()
implicit_csr_test = implicit_matrix_test.tocsr()


# tag_embeddings = train_model(implicit_csr)

# config :
no_components = 75
learning_rate = 0.05
epoch = 10

model : LightFM = LightFM(no_components=no_components, learning_rate=learning_rate, loss="bpr")
model.fit(implicit_csr_train, epochs=epoch, verbose=True)

_item_biases, item_embedding = model.get_item_representations()
tag_embeddings = (item_embedding.T / np.linalg.norm(item_embedding, axis=1)).T

model.predict(implicit_csr_test)

# inf = inference(tag_embeddings, index2qna, category_items, qna2categories)


def get_similar_tags(tag_embeddings: ndarray, tag_id, topk=200) :
    query_embedding = tag_embeddings[tag_id]

    similarity = query_embedding @ tag_embeddings.T

    most_similar = similarity.argpartition(-topk, axis=1)[:, ::-1][:, :topk]
    
    return most_similar


def batch(iterable, batch_size):
    total_len = len(iterable)
    for ndx in range(0, total_len, batch_size):
        yield iterable[ndx : min(ndx + batch_size, total_len)]  # noqa: E203



def get_pool_by_category(query: int, qna2categories: Dict[int, str], category_items: Dict[str, int]) :
    """
    query값으로 받은 qna가 속해있는 카테고리들에게 속해있는 qna pool (list)를 리턴하는 함수
    E.g.)
    qna1 : [상속, 부동산, 재산]
    qna2 : [상속, 부동산]
    qna3 : [부동산]
    qna4 : [상속, 재산]
    qna4 => [qna1, qna2, qna4]
    """

    pool = []
    categories = qna2categories.get(query, [])

    for cat in categories:
        pool += category_items[cat]

    return pool



def recommend(itemid: int, items: List[int], topk=TOPK):
    result = []
    rank = 0
    for rec_id in items:
        if itemid == rec_id:
            continue
        result.append(
            {
                "id": int(itemid),
                "rec_id": int(rec_id),
                "rank": int(rank),
            }
        )
        rank += 1
    return result[:topk]



get_qna_by_index = np.vectorize(lambda x: int(index2qna[x]))

from tqdm import tqdm

recs = []

for i, b in tqdm(enumerate(batch(np.arange(tag_embeddings.shape[0]), batch_size=1000))):
    result = get_similar_tags(tag_embeddings, b)
    query = get_qna_by_index(b)
    result = get_qna_by_index(result)
    
    for q, r in zip(query, result) :
        pool = get_pool_by_category(q, category_items, qna2categories)

        tmp_r = list(np.intersect1d(r, pool))
        if len(tmp_r) > TOPK:
            r = tmp_r
        recs.extend(
            recommend(
                itemid=q,
                items=r,
            )
        )
    query = list(map(str, query))



eda_result = pd.DataFrame(recs)

df[df.n_qna.astype(int).isin(eda_result.id)]

eda_result[~eda_result.id.isin(chk[chk.number.isna()].id)]

eda_result.id.nunique()


query = '''
WITH t1 AS (
SELECT 
  *
 FROM `lawtalk-bigquery.raw.recommend_qna`
 WHERE inferenced_at = (SELECT max(inferenced_at) FROM `lawtalk-bigquery.raw.recommend_qna`)
)
, j1 AS (
  SELECT
    number
    , categories
    , title
    , body
    , createdAt
    , viewCount
  FROM `lawtalk-bigquery.raw.questions`
)
, t2 AS (
  SELECT
    j1.*
    , t1.id
    , t1.rank
    , t1.inferenced_at
   FROM j1
   INNER JOIN t1
   ON t1.id = j1.number
)
SELECT 
  *
FROM t2
'''

chk = bigquery_to_pandas(query)

chk[chk.id.isna()]
chk[chk.number.isna()]


query = '''
WITH t1 AS (
SELECT 
  *
 FROM `lawtalk-bigquery.raw.recommend_qna`
 WHERE inferenced_at = (SELECT max(inferenced_at) FROM `lawtalk-bigquery.raw.recommend_qna`)
)
, c1 AS (
SELECT 
  number
  , title
  , body
  , createdAt 
  , adCategoryId
FROM `lawtalk-bigquery.raw.questions`
  , UNNEST(regexp_extract_all(categories, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)")) as adCategoryId
-- WHERE categories != '[]'
)
, c2 AS (
  SELECT
    c1.*
    , c2.name
  FROM c1
  LEFT JOIN `lawtalk-bigquery.raw.adcategories` AS c2
  ON c1.adCategoryId = c2._id
)
, j1 AS (
  SELECT 
    number
  , title
  , body
  , createdAt 
  , STRING_AGG(name) AS categoryName
FROM c2
GROUP BY 1, 2, 3, 4
)
, t2 AS (
  SELECT
    t1.*
    , j1.categoryName AS base_categories
    , j1.title AS base_title
    , j1.body AS base_body
   FROM t1
   LEFT JOIN j1
   ON t1.id = j1.number
)
, t3 AS (
  SELECT
    t2.*
    , j1.categoryName AS rec_categories
    , j1.title AS rec_title
    , j1.body AS rec_body
   FROM t2
   LEFT JOIN j1
   ON t2.rec_id = j1.number
)
SELECT 
  *
FROM t3
;
'''

tmp = bigquery_to_pandas(query)
tmp = tmp.sort_values(["id", "rank"])

df[df.base_body.isna()]
df[df.base_categories == '']
df[(df.base_categories.isna()) & (df.base_title.notna())]

df.groupby("base_categories").rec_categories.value_counts()


query = '''
SELECT 
  *
 FROM `lawtalk-bigquery.raw.recommend_qna`
 WHERE inferenced_at = (SELECT max(inferenced_at) FROM `lawtalk-bigquery.raw.recommend_qna`)
'''

recommand_qna = bigquery_to_pandas(query)

eda_result
eda_result[eda_result.id.isin(recommand_qna.id)]


df[df.n_qna.isin()]


query = '''
WITH qna_events AS (
    SELECT
        user_pseudo_id,
        event_timestamp,
        udfs.param_value_by_key('page_location',event_params).string_value as page_location
    FROM
        `analytics_265523655.*`
    WHERE TRUE
        AND udfs.param_value_by_key('page_location',event_params).string_value like '%/qna/%'
        AND udfs.param_value_by_key('page_location',event_params).string_value not like '%compose%'
        AND udfs.param_value_by_key('page_location',event_params).string_value not like '%complete%'
)
, GA_QNA AS (
SELECT
    user_pseudo_id, n_qna, count(1) AS cnt
FROM (
    SELECT
        user_pseudo_id, REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') as n_qna
    FROM
        qna_events
    WHERE
        REGEXP_EXTRACT(page_location,'/qna/([0-9]+)-') is not null
)
GROUP BY
    1, 2
)
SELECT * FROM GA_QNA
'''

ga_qna = bigquery_to_pandas(query)

df.n_qna.nunique()

ga_qna.n_qna.nunique()

tmp.number.nunique()

query = '''
WITH t1 AS (
SELECT 
  *
 FROM `lawtalk-bigquery.raw.recommend_qna`
 WHERE inferenced_at = (SELECT max(inferenced_at) FROM `lawtalk-bigquery.raw.recommend_qna`)
)
, c1 AS (
SELECT 
  number
  , title
  , body
  , createdAt 
  , adCategoryId
FROM `lawtalk-bigquery.raw.questions`
  , UNNEST(regexp_extract_all(categories, r"ObjectId\(\'([ㄱ-ㅎ가-힣a-zA-Z0-9 !#$%&()*+,-./:;<=>?@[\]^_`{|}~]+)\'\)")) as adCategoryId
WHERE categories != '[]'
)
, c2 AS (
  SELECT
    c1.*
    , c2.name
  FROM c1
  LEFT JOIN `lawtalk-bigquery.raw.adcategories` AS c2
  ON c1.adCategoryId = c2._id
)
, j1 AS (
  SELECT 
    number
  , title
  , body
  , createdAt 
  , STRING_AGG(name) AS categoryName
FROM c2
GROUP BY 1, 2, 3, 4
)
SELECT * FROM j1
'''

questions = bigquery_to_pandas(query)

questions.number.nunique()

questions[~questions.number.isin(ga_qna.n_qna.astype(int))].number.nunique()
ga_qna[~ga_qna.n_qna.astype(int).isin(questions.number.astype(int))].n_qna.nunique()


questions[questions.number == 46257]
