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

rcParams['figure.figsize'] = 30, 25


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


#Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    return sum_embeddings / sum_mask



def add_tokens() :
    tokenizer = DistilBertTokenizer.from_pretrained("./tokenizer/kcelectra_load")
    
    return tokenizer


def normalize_sentence(x) :
    reFunc1 = lambda x : re.sub(string = x, pattern = r"\n+|\t+|\{+|\}+|#+|\"+|\'+|\<+|\>+|\=+|\++|\-+|~+|\:+|\&+|\!+|\u2028+", repl = " ")
    reFunc2 = lambda x : re.sub(string = x, pattern = r"\)+|\(+|-+|\.+|\s+", repl = " ")
    reFunc3 = lambda x : re.sub(string = x, pattern = r"\s{2,}", repl = " ")
    
    return reFunc3(reFunc2(reFunc1(x)))



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

reFunc1 = lambda x : re.sub(string = x, pattern = r"\n+|\t+|\{+|\}+|#+|\"+|\'+|\<+|\>+|\=+|\++|\-+|~+|\:+|\&+|\!+|\u2028+", repl = " ")
reFunc2 = lambda x : re.sub(string = x, pattern = r"\)+|\(+|-+|\.+|\s+", repl = " ")
reFunc3 = lambda x : re.sub(string = x, pattern = r"\s{2,}", repl = " ")
reFunc4 = lambda x : re.sub(string = x, pattern = "[ㄱ-ㅎㅏ-ㅣ]", repl = "")

df.body = df.body.apply(reFunc1)
df.body = df.body.apply(reFunc2)
df.body = df.body.apply(reFunc3)
df.body = df.body.apply(reFunc4)

gdf = df.groupby("number").agg(category = ("name", list), title = ("title", lambda x : np.unique(x)[0]), body = ("body", lambda x : np.unique(x)[0])).reset_index()




sample_n = 50

#Sentences we want sentence embeddings for
sentences1 = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & ("IT/테크" in x["category"]), axis = 1)].number)].sort_values("number").sample(sample_n).body.values

sentences2 = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & ("아동/소년범죄" in x["category"]), axis = 1)].number)].sort_values("number").sample(sample_n).body.values

sentences3 = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ( x["category"] == ["성범죄"]), axis = 1)].number)].sort_values("number").sample(sample_n).body.values

sentences = list(sentences1) + list(sentences2) + list(sentences3)
sentences = [normalize_sentence(s) for s in sentences]




#Load AutoModel from huggingface model repository
tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask')
# model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = add_tokens()
# model.resize_token_embeddings(len(tokenizer)) 

#Tokenize sentences
encoded_input = tokenizer(sentences, padding=True, truncation=True, max_length=512, return_tensors='pt')

#Compute token embeddings
with torch.no_grad():
    model_output = model(**encoded_input)

#Perform pooling. In this case, mean pooling
sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

emb_np = sentence_embeddings.numpy()



umap_fitter = UMAP(n_neighbors=100, n_components=2, n_epochs=100, min_dist=0.5, local_connectivity=2, random_state=42)

result = umap_fitter.fit_transform(emb_np)
result = pd.DataFrame(result, columns = ["x", "y"])
result.loc[:sample_n, "label"] = "IT성범죄"
result.loc[sample_n:sample_n*2, "label"] = "미성년자성범죄"
result.loc[sample_n*2:sample_n*3, "label"] = "성범죄"


(
    ggplot() +
    geom_point(data = result, mapping = aes(x = "x", y = "y", color = "label")) +
    theme_font()
)



similarity = cosine_similarity(emb_np, emb_np)

sns.heatmap(similarity, cmap='viridis', xticklabels = range(sample_n * 3), yticklabels = range(sample_n * 3))
plt.show()


sentence_df = pd.DataFrame(sentences, columns = ["body"])


pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 0).index)]
pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 50).index)]



keyword = "성매매"
sample_sentence = sentence_df[sentence_df.body.str.contains("성매매")].sample(1)
sentence_df.loc[(pd.DataFrame(similarity).nlargest(5, sample_sentence.index).index)]










#Load AutoModel from huggingface model repository
# tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
# model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask')
model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = add_tokens()
# model.resize_token_embeddings(len(tokenizer)) 


#Tokenize sentences
encoded_input = tokenizer(sentences, padding=True, truncation=True, max_length=512, return_tensors='pt')

#Compute token embeddings
with torch.no_grad():
    model_output = model(**encoded_input)

#Perform pooling. In this case, mean pooling
sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

emb_np = sentence_embeddings.numpy()



umap_fitter = UMAP(n_neighbors=100, n_components=2, n_epochs=100, min_dist=0.5, local_connectivity=2, random_state=42)

result = umap_fitter.fit_transform(emb_np)
result = pd.DataFrame(result, columns = ["x", "y"])
result.loc[:sample_n, "label"] = "IT성범죄"
result.loc[sample_n:sample_n*2, "label"] = "미성년자성범죄"
result.loc[sample_n*2:sample_n*3, "label"] = "성범죄"


(
    ggplot() +
    geom_point(data = result, mapping = aes(x = "x", y = "y", color = "label")) +
    theme_font()
)


similarity = cosine_similarity(emb_np, emb_np)

sns.heatmap(similarity, cmap='viridis', xticklabels = range(sample_n * 3), yticklabels = range(sample_n * 3))
plt.show()


pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 0).index)]
pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 50).index)]
pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 100).index)]








#Load AutoModel from huggingface model repository
# tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
# model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask')
model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
tokenizer = add_tokens()
model.resize_token_embeddings(len(tokenizer)) 


#Tokenize sentences
encoded_input = tokenizer(sentences, padding=True, truncation=True, max_length=512, return_tensors='pt')

#Compute token embeddings
with torch.no_grad():
    model_output = model(**encoded_input)

#Perform pooling. In this case, mean pooling
sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

emb_np = sentence_embeddings.numpy()



umap_fitter = UMAP(n_neighbors=100, n_components=2, n_epochs=100, min_dist=0.5, local_connectivity=2, random_state=42)

result = umap_fitter.fit_transform(emb_np)
result = pd.DataFrame(result, columns = ["x", "y"])
result.loc[:sample_n, "label"] = "IT성범죄"
result.loc[sample_n:sample_n*2, "label"] = "미성년자성범죄"
result.loc[sample_n*2:sample_n*3, "label"] = "성범죄"


(
    ggplot() +
    geom_point(data = result, mapping = aes(x = "x", y = "y", color = "label")) +
    theme_font()
)



similarity = cosine_similarity(emb_np, emb_np)

sns.heatmap(similarity, cmap='viridis', xticklabels = range(sample_n * 3), yticklabels = range(sample_n * 3))
plt.show()


pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 0).index)]
pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 50).index)]
pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 100).index)]





from sklearn.model_selection import train_test_split



#Sentences we want sentence embeddings for
it_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & ("IT/테크" in x["category"]), axis = 1)].number)].assign(y = "온라인성범죄")

child_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ("성범죄" in x["category"]) & ("아동/소년범죄" in x["category"]), axis = 1)].number)].assign(y = "미성년성범죄")

normal_crime = gdf[gdf.number.isin(gdf[gdf.apply(lambda x : ( x["category"] == ["성범죄"]), axis = 1)].number)]
trade_crime = normal_crime[normal_crime.body.str.contains("성매매")].assign(y = "성매매")
normal_crime = normal_crime[~normal_crime.body.str.contains("성매매")].assign(y = "성범죄일반")

sample_n = min(it_crime.shape[0], child_crime.shape[0], normal_crime.shape[0], trade_crime.shape[0])


dataset = pd.concat([it_crime.sample(sample_n), child_crime.sample(sample_n), normal_crime.sample(sample_n), trade_crime.sample(sample_n)]).reset_index(drop = True)


#Load AutoModel from huggingface model repository
tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask')
# model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
# tokenizer = add_tokens()
# model.resize_token_embeddings(len(tokenizer)) 

encoded_input = tokenizer(list(dataset.body.values), padding=True, truncation=True, max_length=512, return_tensors='pt')

with torch.no_grad():
    model_output = model(**encoded_input)

#Perform pooling. In this case, mean pooling
sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

emb_np = sentence_embeddings.numpy()

train_x, test_x, train_y, test_y = train_test_split(emb_np, dataset["y"], test_size = 0.2, stratify = dataset["y"])

train = dataset.iloc[train_y.index, :]
test = dataset.iloc[test_y.index, :].reset_index(drop = True)

similarity = cosine_similarity(train_x, test_x)

def n_idxmax(df, n = 1) :
    return df.apply(lambda x : (x.nlargest(n).index[n-1]).astype(int), axis = 1)

train["idxmax1"] = list(n_idxmax(pd.DataFrame(similarity), 1))
train["idxmax2"] = list(n_idxmax(pd.DataFrame(similarity), 2))
train["idxmax3"] = list(n_idxmax(pd.DataFrame(similarity), 3))

result = pd.merge(train, test[["number", "title", "body", "category", "y"]].add_suffix("_1"), left_on = "idxmax1", right_index = True)
result = pd.merge(result, test[["number", "title", "body", "category", "y"]].add_suffix("_2"), left_on = "idxmax2", right_index = True)
result = pd.merge(result, test[["number", "title", "body", "category", "y"]].add_suffix("_3"), left_on = "idxmax3", right_index = True)


result[["y", "y_1", "y_2", "y_3", "title", "body", "title_1", "body_1", "title_2", "body_2", "title_3", "body_3"]].to_csv("chk/chk.csv", index = False)

test_result1 = result.groupby("idxmax1").y.unique()
test_result2 = result.groupby("idxmax2").y.unique()
test_result3 = result.groupby("idxmax3").y.unique()

test_result = pd.concat([test_result1, test_result2, test_result3]).reset_index()
test_result = test_result.explode("y").groupby("index").agg(y_mapping = ("y", set))
test_result = pd.merge(test, pd.DataFrame(test_result), right_index = True, left_index = True)


len(set(result["idxmax1"].unique().tolist() + result["idxmax2"].unique().tolist() + result["idxmax3"].unique().tolist()))

dataset[dataset.index.isin(train_y.index)]
pd.concat([pd.DataFrame(train_y, columns = ["y"]), pd.DataFrame(similarity)])

sns.heatmap(similarity, cmap='viridis', xticklabels = range(sample_n * 3), yticklabels = range(sample_n * 3))
plt.show()


sentence_df = pd.DataFrame(sentences, columns = ["body"])


pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 0).index)]
pd.DataFrame(sentences).loc[(pd.DataFrame(similarity).nlargest(5, 50).index)]



keyword = "성매매"
sample_sentence = sentence_df[sentence_df.body.str.contains("성매매")].sample(1)
sentence_df.loc[(pd.DataFrame(similarity).nlargest(5, sample_sentence.index).index)]