#!/usr/bin/env python
# coding: utf-8

import os
import sys
import warnings

import json
import re
from glob import glob
from tqdm import tqdm
import random
import datetime
from collections import Counter

import numpy as np
import pandas as pd

from pykospacing import Spacing
from hanspell import spell_checker
from konlpy.tag import Okt

import pandas_gbq

warnings.filterwarnings("ignore")


c_date = datetime.date.today().strftime("%Y-%m-%d")

# from google.cloud import storage
# from google.cloud import bigquery

# def explicit():
#     

#     # Explicitly use service account credentials by specifying the private key
#     # file.
#     storage_client = storage.Client.from_service_account_json(credential_path)

#     # Make an authenticated API request
#     buckets = list(storage_client.list_buckets())
#     print("Success connect google.cloud storage")


# try :
#     explicit()
# except : 
#     print("Client error : maybe you will check credential.json")


# bqclient = bigquery.Client().from_service_account_json(credential_path)



'''방법 2. pandas_gbq - 자동화가 되는지 확인해야함...'''
# import pandas_gbq
# import pydata_google_auth
# credentials = pydata_google_auth.get_user_credentials(
#     ['https://www.googleapis.com/auth/cloud-platform'],
# )
# project_id = "lawtalk-bigquery"

# sql = """
# SELECT * 
# FROM `lawtalk-bigquery.raw.advice`
# LIMIT 10
# """

# df = pandas_gbq.read_gbq(sql, project_id=project_id)

standwords_list = ["상담", "변호사", "설명"]
standpos_list = ["Noun", "Verb", "Adverb", "Adjective"]

def spell_check(x):
    spacing = Spacing()
    x_sent = spacing(x)
    x_sent = spell_checker.check(x_sent)
    return(x_sent.checked)


def tokenize_okt(text, all_body = False):
    okt_pos = Okt().pos(text, norm=True, stem=True)
    okt_filtering = [x for x,y in okt_pos if y in ["Verb", 'Adverb', "Adjective", "Noun"]]
    if all_body == True :
        word_and_pos = pd.DataFrame(okt_pos, columns = ["word", "pos"]).drop_duplicates()
        word_and_pos = word_and_pos[word_and_pos.pos.isin(["Verb", 'Adverb', "Adjective", "Noun"])]
        return(okt_filtering, word_and_pos)
    return(okt_filtering)


def extract_n_gram(x, word, n = 5) :
    return re.findall(pattern = "[^\s]*? " * n + f"[^\s]*{word}[^\s]*?" + " [^\s]*?" * (n + 1), string = x)



import ray
ray.init(num_cpus = 8)

@ray.remote
def ray_tokenize_okt(text, all_body = False):
    okt_pos = Okt().pos(text, norm=True, stem=True)
    okt_filtering = [x for x,y in okt_pos if y in ["Verb", 'Adverb', "Adjective", "Noun"]]
    if all_body == True :
        word_and_pos = pd.DataFrame(okt_pos, columns = ["word", "pos"]).drop_duplicates()
        word_and_pos = word_and_pos[word_and_pos.pos.isin(["Verb", 'Adverb', "Adjective", "Noun"])]
        return(okt_filtering, word_and_pos)
    return(okt_filtering)

@ray.remote
def ray_spell_check(x):
    spacing = Spacing()
    x_sent = spacing(x)
    x_sent = spell_checker.check(x_sent)
    return(x_sent.checked)



q = 1
sql = f'''
WITH t1 AS(
SELECT 
    lawyer
    , createdAt
    , updatedAt
    , review
    , adCategory
    , JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d\s,]+\)|True|False|\[.*\]', "''"), '$.body') AS review_body
    FROM `lawtalk-bigquery.raw.advice` 
    WHERE 1 = 1
        AND status = 'complete'
        AND JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d\s,]+\)|True|False|\[.*\]', "''"), '$.body') IS NOT NULL
        AND EXTRACT(DATE FROM updatedAt) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 MONTH)
)
, t2 AS (
SELECT 
    t1.lawyer
    , t1.createdAt
    , t1.updatedAt
    , t1.review
    , t1.adCategory
    , REGEXP_REPLACE(review_body, r"''|\\"", '') AS review_body
    FROM t1
    WHERE REGEXP_REPLACE(review_body, r"''|\\"", '') <> ''
)

, select_lawyer AS (

    SELECT 
        DISTINCT J1.lawyer, adCategory
        FROM `lawtalk-bigquery.raw.advice` AS J1
    JOIN
    (SELECT 
        lawyer
        , COUNT(review_body) AS cnt
        FROM t2
        GROUP BY lawyer
        HAVING 1 = 1
            AND COUNT(review) = (
                SELECT 
                    APPROX_QUANTILES(cnt, 100)[OFFSET({q})]
                FROM
                (SELECT 
                    lawyer
                    , COUNT(review_body) AS cnt
                    FROM t2
                    GROUP BY lawyer
                    HAVING cnt > 5
                )
            )
        ORDER BY RAND() DESC
        LIMIT 1
    ) AS J2
    ON (J1.lawyer = J2.lawyer)
)

, adCategory_add AS (
SELECT 
    J3.lawyer
    , J3.adCategory
    , J4.name
    FROM select_lawyer AS J3
    LEFT JOIN 
    `lawtalk-bigquery.raw.adcategories` AS J4
    ON (J3.adCategory = J4._id)
)

SELECT 
    T2.lawyer
    , T2.adCategory
    , J5.name
    , T2.review_body
    , T2.updatedAt
    , CASE WHEN T2.lawyer = J5.lawyer THEN 1 ELSE 0 END AS target_lawyer
    , CASE WHEN J5.adCategory IS NULL THEN 0 ELSE 1 END AS target_category
    FROM T2
FULL OUTER JOIN adCategory_add AS J5
ON (T2.adCategory = J5.adCategory)
;
'''


# read data
project_id = "lawtalk-bigquery"
advice = pandas_gbq.read_gbq(sql, project_id=project_id)

# 각 내용에 필요없는 특수문자를 제거합니다.
advice["review_body"] = advice["review_body"].astype(str).apply(lambda x: re.sub(r" \\+s|\\+n|\\+t|~|\:|\)|\(|&|!| ", repl = "", string = x))

print("spell checking AND tokenizing")
# 띄어쓰기 및 철자를 정규화하고 토크나이징 실행
# %time advice["review_body"] = ray.get([ray_spell_check.remote(r) for r in advice["review_body"]])
# %time advice["review_body"] = advice[:300]["review_body"].apply(spell_check)
# %time advice["chk"] = advice.review_body.apply(tokenize_okt)

advice.to_csv(f'data/{datetime.datetime.now().strftime("%Y%m%d")}.csv')


target_lawyers = advice.groupby("lawyer").name.count()[(advice.groupby("lawyer").name.count() > 30)].sample(100, replace=True).index

t = target_lawyers[0]


for t in tqdm(target_lawyers.values) :

    advice.reset_index(drop = True, inplace = True)
    advice.loc[advice["lawyer"] == t, "target_lawyer"] = 1    
    advice.loc[advice["lawyer"] != t, "target_lawyer"] = 0

    print("-"*50)
    print(f"target_lawyer : {advice[advice.target_lawyer == 1].lawyer.unique()}")
    print(advice.groupby("target_lawyer").review_body.count())

    # target 변호사와 아닌 변호사를 구분하여 리뷰 갯수와 모든 후기를 연결
    review_group = advice.groupby(["target_lawyer"]).agg(
        body = ("review_body", " ".join),
        body_count = ("review_body", "count"))

    # 5-gram을 실시
    review_group["ngram1"] = review_group.groupby("target_lawyer").body.apply(lambda x : extract_n_gram(" ".join(x), standwords_list[0], 5)).values
    review_group["ngram2"] = review_group.groupby("target_lawyer").body.apply(lambda x : extract_n_gram(" ".join(x), standwords_list[1], 5)).values
    review_group["ngram3"] = review_group.groupby("target_lawyer").body.apply(lambda x : extract_n_gram(" ".join(x), standwords_list[2], 5)).values


    review_group["word_count1"] = review_group.ngram1.apply(lambda x : len(x))
    review_group["word_count2"] = review_group.ngram2.apply(lambda x : len(x))
    review_group["word_count3"] = review_group.ngram3.apply(lambda x : len(x))


    review_group["ngrams1"] = review_group["ngram1"].apply(lambda x : " ".join(x))
    review_group["ngrams2"] = review_group["ngram2"].apply(lambda x : " ".join(x))
    review_group["ngrams3"] = review_group["ngram3"].apply(lambda x : " ".join(x))

    review_group.reset_index(inplace = True)

    # %time word_count, pos_list = tokenize_okt(" ".join(review_group.ngrams1.values + review_group.ngrams2.values + review_group.ngrams3.values), all_body = True)
    pos_list.drop_duplicates(subset = "word", inplace = True)
    total_counter = pd.merge(pd.Series(Counter(word_count)).reset_index().rename(columns = {"index" : "word", 0 : "cnt"}), pos_list, on = "word", how = "outer")



    word_count1 = review_group.ngrams1.apply(lambda x : pd.Series(Counter(tokenize_okt(x)))).T.reset_index().assign(stand = standwords_list[0])
    word_count2 = review_group.ngrams2.apply(lambda x : pd.Series(Counter(tokenize_okt(x)))).T.reset_index().assign(stand = standwords_list[1])
    word_count3 = review_group.ngrams3.apply(lambda x : pd.Series(Counter(tokenize_okt(x)))).T.reset_index().assign(stand = standwords_list[2])
    word_count = pd.concat([word_count1, word_count2, word_count3]).rename(columns = {"index" : "word", 0 : "non_target", 1 : "target"}).fillna(0)

    word_count["nontarget_review"] = review_group.loc[review_group.target_lawyer == 0, "body_count"].values[0]
    word_count["target_review"] = review_group.loc[review_group.target_lawyer == 1, "body_count"].values[0]

    word_count["target_ratio"] = word_count["target"] / word_count["target_review"]
    word_count["nontarget_ratio"] = word_count["non_target"] / word_count["nontarget_review"]

    word_count["ratio_diff"] = word_count["target_ratio"] - word_count["nontarget_ratio"]


    # extract_word = word_count[word_count["ratio_diff"] > 0]
    extract_word = word_count[(word_count["ratio_diff"] > 0) & (word_count.target > word_count[word_count.target > 0].target.mean())]
    extract_word = pd.merge(extract_word, pos_list, on = "word", how = "left")

    extract_word = extract_word[extract_word.pos.isin(standpos_list)]
    

    if len([x for x in glob("words_dictionary/*.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None]) == 0:
        dict_path = "words_dictionary/word_dictionary_base.csv"
    else :
        dict_path = glob("words_dictionary/*.csv")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}").group(0), "%Y-%m-%d") for x in glob("words_dictionary/*.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None])]


    word_dict = pd.read_csv(dict_path)
    extract_word.to_csv("lawyer_words_before/{}_{}_{}.csv".format(advice[advice.target_lawyer == 1].lawyer.unique()[0], len(advice[advice.target_lawyer == 1]), c_date), index = False)

    pd.concat([word_dict, extract_word[(~extract_word.word.isin(word_dict.words)) & (~extract_word.word.isin(word_dict.final_words))].assign(words = lambda x : x.word, update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M"), updateLawyer = advice[advice.target_lawyer == 1].lawyer.unique()[0])[["words", "pos", "update_time", "updateLawyer"]]]).drop_duplicates(subset = "words").to_csv("words_dictionary/word_dictionary_{}.csv".format(c_date), index = False)



    


from plotnine import *
tmp = extract_word.copy()
(
    ggplot() +
    geom_col(data = extract_word, mapping = aes(x = ))
)

tmp["ranks"] = tmp.groupby("stand").ratio_diff.transform("rank", method = "first", ascending = False)
tmp["rank_word"] = tmp["ranks"].astype(int).astype(str).str.zfill(4) + "_" + tmp.word


(
        ggplot() +
        geom_col(data = tmp[tmp.ranks <= 10], mapping = aes(x = "rank_word", y = "ratio_diff", fill = "stand"), position = "identity") +
        coord_flip() +
        facet_grid("stand~.", scales = "free") +
        theme_bw() +
        theme(legend_position= "none", figure_size = (10, 7), text = element_text(family = "AppleGothic"))
)

gtmp = tmp.groupby("word").ratio_diff.sum().reset_index()
gtmp["ranks"] = gtmp.ratio_diff.rank(method = "first", ascending = False)
gtmp["rank_word"] = gtmp["ranks"].astype(int).astype(str).str.zfill(4) + "_" + gtmp.word

gtmp[gtmp.ranks <= 10]
(
        ggplot() +
        geom_col(data = gtmp[gtmp.ranks <= 10], mapping = aes(x = "rank_word", y = "ratio_diff"), position = "identity") +
        coord_flip() +
        theme_bw() +
        theme(legend_position= "none", figure_size = (10, 7), text = element_text(family = "Malgun Gothic"))
    )


import matplotlib
import matplotlib.font_manager

[f.fname for f in matplotlib.font_manager.fontManager.ttflist]
[f.name for f in matplotlib.font_manager.fontManager.ttflist if 'Nanum' in f.name]

