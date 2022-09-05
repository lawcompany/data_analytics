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

from google.cloud import bigquery

warnings.filterwarnings("ignore")


c_date = datetime.date.today().strftime("%Y-%m-%d")

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
ray.init(num_cpus = os.cpu_count() - 1)
# ray.shutdown()

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
SELECT 
    * 
FROM t3
WHERE advice_rnk <= 50
ORDER BY lawyer, createdAt DESC
;
'''


# read data
advice = bigquery_to_pandas(sql)

'''
# 2022/3/24 기준
advice.lawyer.nunique() # 214 / 519
advice.createdAt.describe() # 
advice.createdAt.hist()
advice.createdAt.dt.year.value_counts()
'''

# 각 내용에 필요없는 특수문자를 제거합니다.
advice["review_body"] = advice["review_body"].astype(str).apply(lambda x: re.sub(r" \\+s|\\+n|\\+t|~|\:|\)|\(|&|!| ", repl = "", string = x))

print("spell checking AND tokenizing")
# 띄어쓰기 및 철자를 정규화하고 토크나이징 실행
# %time advice["review_body"] = ray.get([ray_spell_check.remote(r) for r in advice["review_body"]])

advice.to_csv(f'data/{datetime.datetime.now().strftime("%Y%m%d")}.csv', index = False)
# advice = pd.read_csv(f'data/20220316.csv')
# advice.drop(columns = advice.columns[0], inplace = True)



for t in advice.lawyer.values :
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


    if len([x for x in glob("dictionary/*_edit.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None]) == 0:
        dict_path = "words_dictionary/word_dictionary_base.csv"
    else :
        dict_path = glob("dictionary/*_edit.csv")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}").group(0), "%Y-%m-%d") for x in glob("dictionary/*_edit.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None])]    

    word_dict = pd.read_csv(dict_path)


    tmp = extract_word.copy()

    tmp = pd.merge(tmp, word_dict.assign(word = lambda x : x.words)[["word", "final_words", "type"]], on = "word", how = "left")
    tmp.loc[tmp.final_words.isna() , "final_words"] = tmp.loc[tmp.final_words.isna() , "word"]
    tmp = tmp[tmp.type != "stopwords"]
    tmp = tmp[tmp.pos.isin(standpos_list)]
    tmp = tmp[~tmp.final_words.isin(standwords_list)]


    # tmp["ranks"] = tmp.groupby("stand").ratio_diff.transform("rank", method = "first", ascending = False)
    # tmp["rank_word"] = tmp["ranks"].astype(int).astype(str).str.zfill(4) + "_" + tmp.final_words

    # ttmp = tmp[tmp.ranks <= 10]
    # ttmp["ratio"] = ttmp.groupby("stand").ratio_diff.transform(lambda x : x / sum(x))
    # ttmp["ratio"] = ttmp["ratio"] * 100

    # (
    #         ggplot() +
    #         geom_col(data = ttmp, mapping = aes(x = "rank_word", y = "ratio", fill = "stand"), position = "identity") +
    #         coord_flip() +
    #         facet_grid("stand~.", scales = "free") +
    #         theme_bw() +
    #         theme(legend_position= "none", figure_size = (10, 7), text = element_text(fontproperties = font))
    # )

    gtmp = tmp.groupby("final_words").ratio_diff.sum().reset_index()
    gtmp["ranks"] = gtmp.ratio_diff.rank(method = "first", ascending = False)
    gtmp["rank_word"] = gtmp["ranks"].astype(int).astype(str).str.zfill(4) + "_" + gtmp.final_words
    gtmp = gtmp[gtmp.ranks <= 10]
    gtmp["ratio"] = gtmp.ratio_diff.transform(lambda x : x / sum(x))
    gtmp["ratio"] = gtmp["ratio"] * 100
    
    gtmp.to_csv(f"lawyers_test/{t}.csv", index = False)


'''
{
	_id: (자동생성obj id)
	lawyer: (변호사 obj id)
	reviewData: [ 
		{
			text: '좋았어요', //후기 클라우드에 노출될 텍스트
			rate: 35 //퍼센트
		},
		{
			text: '기분 나빴어요',
			rate: 20
		}
	]
	createdAt: (데이터 넣은 날짜)
}
'''



import matplotlib
import matplotlib.pyplot as plt 
import matplotlib.font_manager as fm  

path = '/usr/share/fonts/truetype/nanum/NanumMyeongjoExtraBold.ttf' 
font_name = fm.FontProperties(fname=path, size=10).get_name()
print(font_name)
plt.rc('font', family=font_name)
font = fm.FontProperties(fname=path, size=9)


from plotnine import *

{
    "_id" : "obj" ,
    "lawyer" : t , 
    "reviewData" : [
        {"text" : i[1]["final_words"] , "rate" : i[1]["ratio"]} for i in gtmp.iterrows()
    ]
}


(
        ggplot() +
        geom_col(data = gtmp, mapping = aes(x = "rank_word", y = "ratio"), position = "identity", fill = "red", alpha = 0.7) +
        coord_flip() +
        theme_bw() +
        theme(legend_position= "none", figure_size = (10, 7), text = element_text(fontproperties = font))
)


