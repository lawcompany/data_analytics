# -*- coding: utf-8 -*-
'''
pr팀이 로톡 10주년 홍보 자료를 위해서 연도별 후기에서 워드 클라우드식의 표현을 위해서 후기에서 나오는 단어 집계를 통해서 

최종 결과물 스프레드 시트

https://docs.google.com/spreadsheets/d/1yHw5L3Mf9i2ntlhhIUO7Aeb3QlXgAL0b/edit#gid=1799910977
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
from tqdm import tqdm
from collections import Counter

import argparse

import pandas as pd
import numpy as np

from tokenizers import BertWordPieceTokenizer

from transformers import BertTokenizer, BertForSequenceClassification, AdamW, get_linear_schedule_with_warmup, BertConfig
from transformers import AutoTokenizer, AutoModel, DistilBertTokenizer

from plotnine import *

from google.cloud import bigquery

warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 20)
pd.set_option('max_colwidth', -1)
pd.options.display.float_format = '{:.2f}'.format


def bigquery_to_pandas(query_string, credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json') :

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


def add_tokens() :
    '''
    기존에 vocab을 추가해서 만들어놓은 토크나이저 로드
    '''
    tokenizer = DistilBertTokenizer.from_pretrained("./tokenizer/kcelectra_load")
    
    return tokenizer

reFunc1 = lambda x : re.sub(string = x, pattern = r"\\n+|\\t+|\{+|\}+|#+|\"+|\'+|\<+|\>+|\=+|\++|\-+|~+|\:+|\&+|\!+|\u2028+", repl = " ")
reFunc2 = lambda x : re.sub(string = x, pattern = r"\)+|\(+|-+|\.+|\s+", repl = " ")
reFunc3 = lambda x : re.sub(string = x, pattern = r"\s{2,}", repl = " ")
reFunc4 = lambda x : re.sub(string = x, pattern = "[ㄱ-ㅎㅏ-ㅣ]", repl = "")


tokenizer = add_tokens()


query = '''
SELECT 
    lawyer
    ,_id
    , createdAt
    , updatedAt
    , status
    , adCategory
    , review
    , REGEXP_REPLACE(review, r'datetime.datetime\([\d\s,]+\)|True|False|\[.*\]', "''") AS extract_review
    , REGEXP_EXTRACT(review, r'\\'rate\\': (\d)') AS review_rate
    , JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d\s,]+\)|True|False|\[.*\]', "''"), '$.body') AS review_body
    , JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d\s,]+\)|True|False|\[.*\]', "''"), '$.opinion') AS review_opinion
    FROM `lawtalk-bigquery.raw.advice` 
    WHERE 1 = 1
        AND status = 'complete'
        -- AND (JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d,]+\)|True|False|\[.*\]', "''"), '$.body') IS NOT NULL
        --     OR JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d,]+\)|True|False|\[.*\]', "''"), '$.body') = ""
        --    OR JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d,]+\)|True|False|\[.*\]', "''"), '$.opinion') IS NOT NULL)
'''

df = bigquery_to_pandas(query)

df["year"] = df.createdAt.dt.year

# 모든 후기를 모으기 위해서 후기(3~5점), 불만사항(1~2점)을 모두 가져오기
df.loc[df.review_body.notnull(), "body"] = df.loc[df.review_body.notnull(), "review_body"]
df.loc[df.review_opinion.notnull(), "body"] = df.loc[df.review_opinion.notnull(), "review_opinion"]

df.body = df.body.fillna("")

df.body = df.body.apply(reFunc1)
df.body = df.body.apply(reFunc2)
df.body = df.body.apply(reFunc3)
df.body = df.body.apply(reFunc4)

# 점수를 주지 않으면 99로 일단 표기
# 시각화를 위해서 string으로 처리
df.review_rate = df.review_rate.fillna("99")
df.review_rate = df.review_rate.astype(str)

gdf = df.groupby(["year", "review_rate"])._id.count().reset_index()
gdf["ratio"] = gdf.groupby("year")._id.transform(lambda x : x / sum(x))
gdf["ratio_99"] = gdf.assign(_id = lambda x : np.where(x.review_rate == "99", 0, x._id)).groupby("year")._id.transform(lambda x : x / sum(x))


gdf = gdf.rename(columns = {"_id" : "cnt"})
gdf_pivot = gdf.pivot(index = ["review_rate"], columns = "year", values = ["cnt", "ratio", "ratio_99"])

gdf_pivot.columns = ["{}_{}".format(c[0], c[1]) for c in gdf_pivot.columns]

gdf_pivot.to_csv("to_pr/rate_ratio.csv")

tmp = df[df.body != '']
tmp = tmp[tmp.body != ' ']

# 토크화를 시켜 카운팅
%time result = pd.DataFrame([{"_id" : _id, "word_cnt" : Counter(tokenizer.tokenize(body))} for _id, body in zip(tmp._id, tmp.body)])

result = result[result.word_cnt != {}]
result = pd.merge(tmp, result, on = "_id")

import collections, functools, operator

total_cnt = result[["year", "word_cnt"]].groupby("year").agg(word_cnt = ("word_cnt", lambda x : dict(functools.reduce(operator.add, map(collections.Counter, x))) ))

%time total_cnt = pd.DataFrame([*total_cnt['word_cnt']], total_cnt.index).stack().rename_axis([None,'word']).reset_index(1, name='cnt')

total_cnt.reset_index(inplace = True)


total_cnt["rnk"] = total_cnt.groupby("index").cnt.rank(method = "first", ascending = False).astype(int)
total_cnt.sort_values(["index", "cnt"])

pivot_df = total_cnt.pivot(index = "rnk", columns = "index", values = ["word", "cnt"])



std_word.loc[std_word.words.str.contains("잘"), "words"] = "잘"
std_word = std_word[~std_word.words.isin(["부분", "제가", "많은", "것", "일"])]
std_word.reset_index(inplace = True, drop = True)
std_word.loc[std_word.words.str.contains("좋았"), "words"] = "좋"

tmp = pd.DataFrame(columns = ["기준", "파생단어"])

for i in std_word.words.unique() :
    print(i)
    print(total_except[(total_except.word.str.contains(i))].word.unique())
    # print(total_except[(total_except.word.str.contains(i)) & (~total_except.word.str.contains("잘못|쓰잘|자잘|잘라|잘가"))].word.unique())
    print("-"*50)
    tmp_ = pd.DataFrame(columns = ["기준", "파생단어"])
    tmp_["파생단어"] = list(total_except[(total_except.word.str.contains(i))].word.unique())
    tmp_["기준"] = i
    tmp = pd.concat([tmp, tmp_])



    
tmp.to_csv("to_pr/기준단어.csv", index = False)

pd.DataFrame([i, list(total_except[(total_except.word.str.contains(i))].word.unique())], columns = ["기준", "파생단어"])

# total_except = total_cnt[~total_cnt.word.str.startswith("##")]
total_except = total_cnt[total_cnt.word.str.isalnum()]
total_except["rnk"] = total_except.groupby("index").cnt.rank(method = "first", ascending = False).astype(int)
total_except.sort_values(["index", "cnt"])

total_except[total_except.word.str.contains("형사")].sort_values("rnk")

pv_df = total_except.pivot(index = "rnk", columns = "index", values = ["word", "cnt"])


os.makedirs("to_pr")
result.groupby("word")["count"].sum().sort_values(ascending = False).reset_index().to_csv("to_pr/chk.csv", index = False)


std_word = pd.read_csv("from_pr/pr전달단어.csv")
std_word = std_word.iloc[:, 1:]
std_word.columns = ["grouping_word", "word", "chk"]
std_word = std_word[std_word.chk != "x"]

tmp = pd.merge(total_except, std_word, on = "word", how = "outer")
tmp = tmp.rename(columns = {"index" : "year"})

tmp.loc[tmp.grouping_word.isnull(), "grouping_word"] = tmp.loc[tmp.grouping_word.isnull(), "word"]

# tmp = tmp.drop(columns = ["group_rnk"])

tmp["group_cnt"] = tmp.groupby(["year", "grouping_word"]).cnt.transform("sum")

tmp_ = tmp[["year", "grouping_word", "group_cnt"]].drop_duplicates()
tmp_["group_rnk"] = tmp_.groupby(["year"]).group_cnt.rank(method = "first", ascending = False).astype(int)

tmp = pd.merge(tmp, tmp_[["year", "grouping_word", "group_rnk"]], on = ["year", "grouping_word"])
tmp["group_in_rnk"] = tmp.groupby(["year", "group_rnk"]).cnt.rank(method = "first", ascending = False)

pivot_df = tmp.pivot(index = ["group_rnk", "group_in_rnk"], columns = "year", values = ["grouping_word", "group_cnt", "rnk", "word"])
pivot_df.columns = ["{}_{}".format(c[0], c[1]) for c in pivot_df.columns]
pivot_df = pivot_df[sorted(pivot_df.columns, key = lambda x : int(x.split("_")[-1]))]

pivot_df.reset_index(inplace = True)

pivot_df.to_csv("to_pr/final.csv")



pivot_df_ = tmp[["year", "grouping_word", "group_cnt", "group_rnk"]].drop_duplicates().pivot(index = ["group_rnk"], columns = "year", values = ["grouping_word", "group_cnt"])
pivot_df_.columns = ["{}_{}".format(c[0], c[1]) for c in pivot_df_.columns]
pivot_df_ = pivot_df_[sorted(pivot_df_.columns, key = lambda x : int(x.split("_")[-1]))]
pivot_df_.reset_index(inplace = True)

pivot_df_.to_csv("to_pr/final2.csv")

tmp__ = tmp.groupby("grouping_word").cnt.sum().reset_index()
tmp__.sort_values("cnt", ascending = False).to_csv("to_pr/total_year.csv")
