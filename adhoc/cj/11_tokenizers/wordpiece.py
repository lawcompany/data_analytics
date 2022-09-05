import os
import sys
import warnings
import gc

import re
import datetime
from tqdm import tqdm

import numpy as np
import pandas as pd

import matplotlib as mpl
# !pip install plotnine
from plotnine import *

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

%matplotlib inline

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
        , REGEXP_REPLACE(result, r"{\'body\': (.+), \'date\'.+", r'\1') AS body
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
SELECT _id, body, REGEXP_REPLACE(body, r'\\n+|\\t+|\s+|\{+|\}+|#+|\"+|\\'+|\<+|\>+|^+|=+|\++|\-+|~+|\:+|\&+|\!+|\)+|\(+|-+|[0-9]|\.{2,}|/?|[ㄱ-ㅎ]+|[ㅏ-ㅣ]+', ' ') AS bodies, type FROM t
;
"""

q = bigquery_to_pandas(query_string)
q = q[q.bodies.notna()]
%time q.f0_ = q.f0_.str.replace(" ", "")

spacing = Spacing()

%time q["bodies_spacing"] = q.f0_.str.replace(" ", "").apply(lambda x : spacing(x))
