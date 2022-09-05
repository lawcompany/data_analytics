# -*- coding: utf-8 -*-
import os
print(os.getpid())
import sys
import warnings
import gc

import re
import datetime
from tqdm import tqdm

import multiprocessing
from multiprocessing import Pool

import numpy as np
import pandas as pd

import matplotlib as mpl
# !pip install plotnine
from plotnine import *

import sklearn
from sklearn.model_selection import train_test_split

# !pip install --ignore-installed certifi
# !pip install git+https://github.com/haven-jeon/PyKoSpacing.git
# !pip install git+https://github.com/ssut/py-hanspell.git
from pykospacing import Spacing

from tokenizers import BertWordPieceTokenizer

# !pip install --upgrade google-cloud-bigquery
from google.cloud import bigquery
# !pip install --upgrade google-cloud-storage
from google.cloud import storage
credential_path = '../credential/lawtalk-bigquery-2bfd97cf0729.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path
bqclient = bigquery.Client()

warnings.filterwarnings("ignore")

# %matplotlib inline

pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 10)
pd.set_option('max_colwidth', -1)



def bigquery_to_pandas(query_string) :

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

def parallelize_df(series, func, n_cores, shuffle = False) :
    if shuffle == True :
        series = sklearn.utils.shuffle(series)
    series_split = np.array_split(series, n_cores)
    pool = Pool(n_cores)
    sereis = pd.concat(pool.map(func, series_split))
    pool.close()
    pool.join()
    
    return sereis

# data load
# data preprocessing
query_string = """
WITH t1 AS (
    SELECT 
        _id
        , body
        -- , adCategory
        , 'advice_body' AS type
    FROM `lawtalk-bigquery.raw.advice`
)
, t2 AS (
    SELECT 
        _id
        , JSON_EXTRACT(REGEXP_REPLACE(REGEXP_REPLACE(result, r'(datetime.datetime\([0-9]+, [0-9]+, [0-9]+, [0-9]+, [0-9]+, [0-9]+, [0-9]+\))', r"'\1'"), '"', ""), "$.body") AS body
        -- , adCategory
        , 'result_body' AS type
        -- , result
    FROM `lawtalk-bigquery.raw.advice`
    WHERE 1 = 1
        -- AND REGEXP_REPLACE(result, r"{\'body\': (.+), \'date\'.+", r'\1') IS NULL
        -- AND result IS NOT NULL
)
, t3 AS (
    SELECT 
        _id
        , title AS body
        , 'question_title' AS type
    FROM `lawtalk-bigquery.raw.questions`
)
, t4 AS (
    SELECT 
        _id
        , body
        , 'question_body' AS type
    FROM `lawtalk-bigquery.raw.questions`
)
, t5 AS (
    SELECT 
        _id
        , body
        , 'answers_body' AS type
    FROM `lawtalk-bigquery.raw.answerbodies`
)
, t AS (
    SELECT * 
        FROM t1 
        UNION ALL SELECT * FROM t2
        UNION ALL SELECT * FROM t3
        UNION ALL SELECT * FROM t4
        UNION ALL SELECT * FROM t5
)
SELECT _id, body, REGEXP_REPLACE(body, r'\\n+|\\t+|\\\\n|\\\\t|\s{2,}|\{+|\}+|#+|\"+|\\'+|\<+|\>+|^+|=+|\++|\-+|~+|\:+|\&+|\!+|\)+|\(+|-+|[0-9]|\.{2,}|/?|[ㄱ-ㅎ]+|[ㅏ-ㅣ]+', '') AS bodies, type FROM t
WHERE 1 = 1
    AND body IS NOT NULL
    AND body != ' '
    limit 10
;
"""

# %time df = bigquery_to_pandas(query_string)
df = df[df.bodies.notna()]
# 중간에 이상한 문자로 에러가 뜨는 것을 방지
df = df[df.bodies != '\x01']


def multi_spacing(s) :
    # 띄어쓰기 교정
    spacing = Spacing()
    print(f"{os.getpid()} start \n")
    s = s.apply(lambda x: spacing(x))
    print(f"{os.getpid()} end")
    return s


def write_lines(path, lines):
    with open(path, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(f'{line}\n')


write_lines("corpus/test.txt", tuple(df.bodies.values))

# wordpiece 알고리즘을 통해서 토큰(문자 덩어리) 생성
wp_tokenizer = BertWordPieceTokenizer(
    clean_text=True,
    handle_chinese_chars=True,
    strip_accents=False, # Must be False if cased model
    lowercase=False,
    wordpieces_prefix="##"
)

vocab_size = 1000
# %time wp_tokenizer.train(files = "corpus/test.txt", limit_alphabet= 6000, vocab_size= vocab_size)

os.mkdir(f"corpus/vocab_size{vocab_size}")
wp_tokenizer.save_model(f"corpus/vocab_size{vocab_size}")
