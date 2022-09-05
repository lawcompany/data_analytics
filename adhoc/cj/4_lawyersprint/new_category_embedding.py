#!/usr/bin/env python
# coding: utf-8

# ### 기존 분야가 성범죄에서
# 1. 성매매
# 2. 성폭력/강제추행 등
# 3. 미성년 등 특수성범죄
# 4. 온라인 성범죄
# \
# 로 나누어지는 것에 대하여 transformers의 embedding을 값을 cosine_similarity으로 나누는 작업

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

import sklearn
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
from umap import UMAP

import torch

from transformers import BertTokenizer, BertForSequenceClassification, AdamW, get_linear_schedule_with_warmup, BertConfig, AutoTokenizer, AutoModel, DistilBertTokenizer



from google.cloud import bigquery
from google.cloud import storage

warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 10)
pd.set_option('max_colwidth', -1)


import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib import rcParams

import seaborn as sns

path = '/usr/share/fonts/truetype/nanum/NanumMyeongjoExtraBold.ttf' 
font_name = fm.FontProperties(fname=path, size=10).get_name()
print(font_name)
plt.rc('font', family=font_name)
font = fm.FontProperties(fname=path, size=9)

rcParams['figure.figsize'] = 15, 10

from plotnine import *

path = '/usr/share/fonts/truetype/nanum/NanumMyeongjoExtraBold.ttf' 
font_name = fm.FontProperties(fname=path, size=10).get_name()
print(font_name)
plt.rc('font', family=font_name)
font = fm.FontProperties(fname=path, size=9)

def theme_font(figure_size = (10, 7), legend_position = "right") :
    return theme(legend_position= "right", figure_size = figure_size, text = element_text(fontproperties = font))


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


# 참고자료 : https://huggingface.co/jhgan/ko-sroberta-multitask 에서 pytorch로 사용하는 예시가 있길래 공부차 아래 예시의 코드로 작성해보았습니다.
#Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    return sum_embeddings / sum_mask


def n_idxmax(df, n = 1) :
    return df.apply(lambda x : (x.nlargest(n).index[n-1]).astype(int), axis = 1)


# ### 카테고리를 풀어서 매칭한 뒤 가져오기

query = '''
WITH c1 AS (
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
SELECT * FROM c2
'''


df = bigquery_to_pandas(query)
print(df.name.unique())


# body에 있는 특수문자 등에 대한 전처리

reFunc1 = lambda x : re.sub(string = x, pattern = r"\n+|\t+|\{+|\}+|#+|\"+|\'+|\<+|\>+|\=+|\++|\-+|~+|\:+|\&+|\!+|\u2028+", repl = " ")
reFunc2 = lambda x : re.sub(string = x, pattern = r"\)+|\(+|-+|\.+|\s+", repl = " ")
reFunc3 = lambda x : re.sub(string = x, pattern = r"\s{2,}", repl = " ")
reFunc4 = lambda x : re.sub(string = x, pattern = "[ㄱ-ㅎㅏ-ㅣ]", repl = "")


df.body = df.body.apply(reFunc1)
df.body = df.body.apply(reFunc2)
df.body = df.body.apply(reFunc3)
df.body = df.body.apply(reFunc4)


gdf = df.groupby("number").agg(category = ("name", list), title = ("title", lambda x : np.unique(x)[0]), body = ("body", lambda x : np.unique(x)[0])).reset_index()


# 상담사례 다중 레이블을 이용하여 성범죄에서 재분류된 4종의 카테고리로 나눠보기

gdf["category"]


#Sentences we want sentence embeddings for

'''무료 상담에서 IT테크와 성범죄가 같이 있는 범죄는 온라인 성범죄'''
it_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & ("IT/테크" in x["category"]), axis = 1)].number)].assign(y = "온라인성범죄")

'''아동/소년범죄와 성범죄가 같이 있는 범죄는 미성년 성범죄'''
child_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & ("아동/소년범죄" in x["category"]), axis = 1)].number)].assign(y = "미성년성범죄")


# normal_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ( x["category"] == ["성범죄"]), axis = 1)].number)]
'''성매매는 기존의 분류로 찾기 어려워 일단 성매매라는 단어가 있으면 성매매'''
trade_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & (re.findall(string = x["body"], pattern = "성매매") != []), axis = 1)].number)].assign(y = "성매매")
# '''그 외 나머지는 성범죄일반 - 성범죄 일반에는 너무 많은 오염이 들어가있어 제외''' 
# vio_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & ("폭행/협박" in x["category"]), axis = 1)].number)].assign(y = "성폭력")
vio_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & (re.findall(string = x["body"], pattern = "성폭행|성폭력|성추행") != []), axis = 1)].number)].assign(y = "성폭력")


'''헷갈리지 않도록 네개의 카테고리 중 가장 작은 값의 샘플만 비교 - 95개?씩만 뽑아서 -> 메모리가 터져서 다시 80개로 줄임'''
sample_n = min(it_crime.shape[0], child_crime.shape[0], normal_crime.shape[0], trade_crime.shape[0], 80)
print(sample_n)

dataset = pd.concat([it_crime.sample(sample_n), child_crime.sample(sample_n), vio_crime.sample(sample_n), trade_crime.sample(sample_n)]).reset_index(drop = True)


dataset[dataset.y == "성폭력"].head(3)


dataset[dataset.y == "성매매"].head(3)


dataset[dataset.y == "미성년성범죄"].head(3)


dataset[dataset.y == "온라인성범죄"].head(3)


'''
모델 불러오기 
    궁금증 : KcElectra 모델로도 테스트를 해보았지만 어느정도 embedding이 될줄 알았으나 consine_similarity가 거의 다 비슷하게 나오는 현상이 있음 - 아래쪽에 참고
'''
#Load AutoModel from huggingface model repository
tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask')
# model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")


encoded_input = tokenizer(list(dataset.body.values), padding=True, truncation=True, max_length=512, return_tensors='pt')

# batch로 돌리지 않아서 ram이 작거나 input이 많이 들어가면 커널이 날아갈 수 있어요~
with torch.no_grad():
    model_output = model(**encoded_input)

#Perform pooling. In this case, mean pooling
sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

emb_np = sentence_embeddings.numpy()


train_x, test_x, train_y, test_y = train_test_split(emb_np, dataset["y"], test_size = 0.2, stratify = dataset["y"], random_state = 1)

train = dataset.iloc[train_y.index, :]
test = dataset.iloc[test_y.index, :].reset_index(drop = True)

similarity = cosine_similarity(train_x, test_x)


train["idxmax1"] = list(n_idxmax(pd.DataFrame(similarity), 1))
train["idxmax2"] = list(n_idxmax(pd.DataFrame(similarity), 2))
train["idxmax3"] = list(n_idxmax(pd.DataFrame(similarity), 3))

result = pd.merge(train, test[["number", "title", "body", "category", "y"]].add_suffix("_1"), left_on = "idxmax1", right_index = True)
result = pd.merge(result, test[["number", "title", "body", "category", "y"]].add_suffix("_2"), left_on = "idxmax2", right_index = True)
result = pd.merge(result, test[["number", "title", "body", "category", "y"]].add_suffix("_3"), left_on = "idxmax3", right_index = True)


result[["y", "y_1", "y_2", "y_3", "title", "body", "title_1", "body_1", "title_2", "body_2", "title_3", "body_3"]].to_csv("chk/chk.csv", index = False)


result[["y", "y_1", "y_2", "y_3"]].head()


# ### 기준 사례에 대한 제목과 본문(title, body)에 가장 유사도(코사인 유사도)가 높은 3순위에 대한 제목과 본문(title_1, body_1 ~ title_3, body_3 순서)

result[["y", "title", "body", "title_1", "body_1", "title_2", "body_2", "title_3", "body_3"]].head()


test_result1 = result.groupby("idxmax1").y.unique()
test_result2 = result.groupby("idxmax2").y.unique()
test_result3 = result.groupby("idxmax3").y.unique()

test_result = pd.concat([test_result1, test_result2, test_result3]).reset_index()
test_result = test_result.explode("y").groupby("index").agg(y_mapping = ("y", set))
test_result = pd.merge(test, pd.DataFrame(test_result), right_index = True, left_index = True)


# #### 3순위까지 하면 test set 64 중 59개를 커버하고 있으므로, 커버를 못하는 사례도 있을 수 있을 것 같습니다.

print(test["number"].nunique())
print(result["idxmax1"].nunique())
print(result["idxmax2"].nunique())
print(result["idxmax3"].nunique())

print(len(set(result["idxmax1"].unique().tolist() + result["idxmax2"].unique().tolist() + result["idxmax3"].unique().tolist())))


# ## 최종 Test 셋에 대한 결과(y_mapping) : 개인적으로 급한 상황이라면 못 쓸 정도는 아니지 않을까 싶습니다

test_result.rename(columns = {"category" : "현재카테고리", "y_mapping" : "최종결과"})[["number", "현재카테고리", "title", "body", "최종결과"]].head()


test_result.rename(columns = {"category" : "현재카테고리", "y_mapping" : "최종결과"})[["number", "현재카테고리", "title", "body", "최종결과"]].to_csv("chk/최종결과.csv", index = False)


test_result


test_result.apply(lambda x : x["y"] in x["y_mapping"], axis = 1).mean()


test_result[test_result.apply(lambda x : x["y"] not in x["y_mapping"], axis = 1)]


# #### 궁금증 
# 1. kcelectra가 embedding을 위한 모델이 아니더라도 어느 정도 문장에 대한 정보를 담아야할텐데 어떻게 이렇게 다른 성향을 나타낼까요..?
#     - ko-sroberta-multitask 모델도 학습이 된 모델인데, 그렇다면 모델의 성능에 따라서 달라진다면 로톡의 데이터셋에 맞춘 모델을 대략적으로 생성한다면 Embedding에 대한 결과가 더 좋게 나올까요?

tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask')
# model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")

#Tokenize sentences
encoded_input = tokenizer(dataset.body.values.tolist(), padding=True, truncation=True, max_length=512, return_tensors='pt')

#Compute token embeddings
with torch.no_grad():
    model_output = model(**encoded_input)

#Perform pooling. In this case, mean pooling
sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

emb_np = sentence_embeddings.numpy()

similarity = cosine_similarity(emb_np, emb_np)


sns.heatmap(similarity, cmap='viridis', xticklabels = range(sample_n * 4), yticklabels = range(sample_n * 4))
plt.show()


# tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
# model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask')
model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")

#Tokenize sentences
encoded_input = tokenizer(dataset.body.values.tolist(), padding=True, truncation=True, max_length=512, return_tensors='pt')

#Compute token embeddings
with torch.no_grad():
    model_output = model(**encoded_input)

#Perform pooling. In this case, mean pooling
sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

emb_np = sentence_embeddings.numpy()

similarity = cosine_similarity(emb_np, emb_np)


sns.heatmap(similarity, cmap='viridis', xticklabels = range(sample_n * 4), yticklabels = range(sample_n * 4))
plt.show()




