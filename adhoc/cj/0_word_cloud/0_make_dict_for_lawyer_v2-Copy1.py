#!/usr/bin/env python
# coding: utf-8
import os
import sys
import warnings

import argparse

import json
import re
from glob import glob
from tqdm import tqdm
import random
import datetime
from collections import Counter
import requests

import numpy as np
import pandas as pd

from pykospacing import Spacing
from hanspell import spell_checker
from konlpy.tag import Okt

from google.cloud import bigquery
from google.cloud import storage

import gspread
from oauth2client.service_account import ServiceAccountCredentials

warnings.filterwarnings("ignore")

c_date = datetime.date.today().strftime("%Y-%m-%d")


parser = argparse.ArgumentParser()

parser.add_argument("-y", default=datetime.date.today().year, type= int)
parser.add_argument("-m", default= datetime.date.today().month, type= int)
parser.add_argument("-d", default= datetime.date.today().day, type= int)

# parser.add_argument("-y", default=2022, type= int)
# parser.add_argument("-m", default= 3, type= int)
# parser.add_argument("-d", default= 31, type= int)

args = parser.parse_args('')

y = str(args.y).zfill(4)
m = str(args.m).zfill(2)
d = str(args.d).zfill(2)

standwords_list = ["상담", "변호사", "설명"]
standpos_list = ["Noun", "Verb", "Adverb", "Adjective"]


def spell_check(x):
    spacing = Spacing()
    x_sent = spacing(x)
    x_sent = spell_checker.check(x_sent)
    return(x_sent.checked)


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

def gs_append(df, url = "https://docs.google.com/spreadsheets/d/1P_sFwDbiHL-yszUyxlcJ-DjFjUtFS3vR9S7zWCfPMyk/edit#gid=0", credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json', scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']) :
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    
    sheet_content = sheet.get_all_values()
    
    df_ = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    df = df[~df.words.isin(df_.words)]
    
    if sheet_content == [] :
        r = sheet.append_row(df.columns.to_list())
    r = sheet.append_rows(df.values.tolist())
    
    return r, df_


def read_dict(url = "https://docs.google.com/spreadsheets/d/1P_sFwDbiHL-yszUyxlcJ-DjFjUtFS3vR9S7zWCfPMyk/edit#gid=0", credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json', scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']) :
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    sheet_content = sheet.get_all_values()
    
    df_ = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    
    return df_


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


def upload_gcs(bucket_name, source_file_name, destination_blob_name) :
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    
    print(f"upload {bucket_name} {source_file_name} to {destination_blob_name}")
    
    return


def write_file(dirs = f"lawyer_{y}{m}{d}/{t}.csv") :
    
    upload_gcs('lawtalk-bigquery-bucket', dirs, dirs)
    
    return


def send_to_slack(text) :
    
    url = "https://hooks.slack.com/services/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"
    requests.post(url, headers={'Content-type': 'application/json'}, json = {"text" : text})
    
    return

# import ray
# ray.init(num_cpus = os.cpu_count() - 1)
# # ray.shutdown()

# def write_json(path, data) :
#     with open(path, 'w') as outfile:
#         json.dump(data, outfile)
#     return

# @ray.remote
# def ray_tokenize_okt(text, all_body = False):
#     okt_pos = Okt().pos(text, norm=True, stem=True)
#     okt_filtering = [x for x,y in okt_pos if y in ["Verb", 'Adverb', "Adjective", "Noun"]]
#     if all_body == True :
#         word_and_pos = pd.DataFrame(okt_pos, columns = ["word", "pos"]).drop_duplicates()
#         word_and_pos = word_and_pos[word_and_pos.pos.isin(["Verb", 'Adverb', "Adjective", "Noun"])]
#         return(okt_filtering, word_and_pos)
#     return(okt_filtering)

# @ray.remote
# def ray_spell_check(x):
#     spacing = Spacing()
#     x_sent = spacing(x)
#     x_sent = spell_checker.check(x_sent)
#     return(x_sent.checked)



sql = '''
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
        AND EXTRACT(DATE FROM createdAt) <= DATE('{}-{}-{}')
        -- AND EXTRACT(DATE FROM createdAt) <= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
        -- AND EXTRACT(DATE FROM updatedAt) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 MONTH)
)
, t2 AS (
SELECT 
    t1.lawyer
    , t1.createdAt
    , t1.updatedAt
    , t1.review
    , t1.adCategory
    , REGEXP_REPLACE(review_body, r"''|\\"", '') AS review_body
    , RANK() OVER(PARTITION BY lawyer ORDER BY createdAt DESC) AS advice_rnk
    FROM t1
    WHERE REGEXP_REPLACE(review_body, r"''|\\"", '') <> ''
)
, select_lawyer AS (
    SELECT
        DISTINCT lawyer
    FROM t2
    WHERE advice_rnk >= 50
)
, t3 AS (
SELECT
    t2.*
FROM t2
INNER JOIN select_lawyer
USING (lawyer)
)
, t4 AS (
SELECT
    t3.*
    , J1.name
    , J1.slug
FROM t3
LEFT JOIN
`lawtalk-bigquery.raw.lawyers` AS J1
ON (t3.lawyer = J1._id)
)
SELECT 
    * 
FROM t4
WHERE advice_rnk <= 50
ORDER BY lawyer, createdAt DESC
;
'''.format(y, m, d)



# read data
advice = bigquery_to_pandas(sql)

# 각 내용에 필요없는 특수문자를 제거합니다.
advice["review_body"] = advice["review_body"].astype(str).apply(lambda x: re.sub(r" \\+s|\\+n|\\+t|~|\:|\)|\(|&|!|", repl = "", string = x))
advice["review_body"] = advice["review_body"].astype(str).apply(lambda x: re.sub(r" {2,}", repl = " ", string = x))

advice.review_body = advice.apply(lambda x : x.review_body.replace(x["name"], ""), axis = 1)

print("spell checking AND tokenizing")
# 띄어쓰기 및 철자를 정규화하고 토크나이징 실행
# %time advice["review_body"] = ray.get([ray_spell_check.remote(r) for r in advice["review_body"]])
# ray.shutdown()

advice.to_csv(f'data/{y}{m}{d}.csv', index = False)
# advice = pd.read_csv(f'data/20220316.csv')
# advice.drop(columns = advice.columns[0], inplace = True)

send_to_slack("start making review for lawyer")


chk = [{i : pd.read_csv(i).shape[0]} for i in glob(f"lawyer_{y}{m}{d}/*.csv") if pd.read_csv(i).shape[0] != 10]

len(chk)

t = list(chk[0].keys())[0].split("/")[1].split(".")[0]

for idx, t in tqdm(enumerate(advice.lawyer.unique())) : 

    advice.reset_index(drop = True, inplace = True)
    advice.loc[advice["lawyer"] == t, "target_lawyer"] = 1
    advice.loc[advice["lawyer"] != t, "target_lawyer"] = 0
    
    print("-"*50)
    print(f"target_lawyer : {advice[advice.target_lawyer == 1].lawyer.unique()[0]}")
    print(f"lawyer_name : {advice[advice.target_lawyer == 1].name.unique()[0]}")
    
    os.makedirs(f"lawyer_{y}{m}{d}", exist_ok = True)
    
    if os.path.isfile(f"lawyer_{y}{m}{d}/{t}.csv") :
        print("already exist")
        if pd.read_csv(f"lawyer_{y}{m}{d}/{t}.csv").shape[0] >= 10 :
            print("Over 10")
            continue
            
    
    print("Processing")

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

    word_count, pos_list = tokenize_okt(" ".join(review_group.ngrams1.values + review_group.ngrams2.values + review_group.ngrams3.values), all_body = True)
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
    
    word_count = pd.merge(word_count, pos_list, on = "word", how = "left")
    word_count = word_count[word_count.pos.isin(standpos_list)]
    
    if idx == 0 :
        dict_update = word_count.drop_duplicates(subset = "word")

        dict_update = dict_update.assign(words = lambda x : x.word, final_words = "", type = "", update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M"), updateLawyer = "base", cut_date = "-".join([str(y), m, d]))[["words", "final_words", "type", "pos", "update_time", "updateLawyer", "cut_date"]].drop_duplicates(subset = "words")

        os.makedirs("share_dict", exist_ok = True)
        dict_path = glob("share_dict/*.csv")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}").group(0), "%Y-%m-%d") for x in glob("share_dict/*.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None])]

        dict_downdate = pd.read_csv(dict_path)

        dict_update = pd.concat([dict_downdate, dict_update]).drop_duplicates("words")
        dict_update.to_csv(f"share_dict/{y}-{m}-{d}.csv", index = False)

        r, gs_dict = gs_append(dict_update[dict_update.cut_date == f"{y}-{m}-{d}"])
        # gs_dict = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
        

    gs_dict = read_dict()
    gs_dict = gs_dict[gs_dict["type"] != "stopwords"]
    gs_dict = gs_dict[gs_dict["type"] != "questionable"]
    gs_dict = gs_dict[gs_dict["type"] != ""]
    gs_dict.loc[gs_dict["final_words"] == "", "final_words"] = gs_dict.loc[gs_dict["final_words"] == "", "words"].values

    # extract_word = word_count[(word_count["ratio_diff"] > 0) & (word_count.target > word_count[word_count.target > 0].target.mean())]
    
    tmp = word_count.copy()

    tmp = pd.merge(tmp, gs_dict.assign(word = lambda x : x.words)[["word", "final_words", "type"]], on = "word", how = "inner")
    tmp.loc[tmp.final_words.isna() , "final_words"] = tmp.loc[tmp.final_words.isna() , "word"]
    tmp = tmp[tmp.pos.isin(standpos_list)]
    tmp = tmp[~tmp.final_words.isin(standwords_list)]
    
    gtmp = tmp.groupby("final_words").ratio_diff.sum().reset_index()
    gtmp["ranks"] = gtmp.ratio_diff.rank(method = "first", ascending = False)
    gtmp["rank_word"] = gtmp["ranks"].astype(int).astype(str).str.zfill(4) + "_" + gtmp.final_words
    
    gtmp = gtmp.sort_values("ranks")
    gtmp = gtmp.head(10)
    
    if gtmp.ratio_diff.min() <= 0 :
        print("Minus edited")
        send_to_slack(f"lawyer_name : {advice[advice.target_lawyer == 1].name.unique()[0]} is not enough less than 10")
        gtmp.ratio_diff = (-1 * gtmp.ratio_diff.min()) + 0.005
    
    
    gtmp.to_csv(f"lawyer_{y}{m}{d}/{t}.csv", index = False)
    
    write_file(f"lawyer_{y}{m}{d}/{t}.csv")
    
    send_to_slack(f"upload {advice[advice.target_lawyer == 1].name.unique()[0]}변호사 {idx + 1} / {advice.lawyer.nunique()}")
    


