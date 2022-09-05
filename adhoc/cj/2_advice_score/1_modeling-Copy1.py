# -*- coding: utf-8 -*-
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

import argparse

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, mean_squared_error

import torch
from torch import nn
from torch.utils.data import TensorDataset, DataLoader, RandomSampler, SequentialSampler

from transformers import BertTokenizer, BertForSequenceClassification, AdamW, get_linear_schedule_with_warmup, BertConfig
from transformers import AutoTokenizer, AutoModel, DistilBertTokenizer

from keras.preprocessing.sequence import pad_sequences

from google.cloud import bigquery


train = pd.read_csv("data/train.csv", sep = "\t")

train.sum_uv.describe()
train.sort_values("sum_uv", ascending = False).head()

train[train.sum_uv > 10].sum_uv.hist(bins = 300)

train[train.title.str.contains("영상 소지")].sum_uv.hist(bins = 100)
train[train.title.str.contains("아청법")].sum_uv.describe()


warnings.filterwarnings("ignore")

def get_logger(log_dir, formatter = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', setLevels = logging.INFO) :
    
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_dir)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger



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


def load_data(args, path):
    temp = pd.read_csv(path, sep="\t")
    temp = temp
    document = temp[args.x_col].tolist()
    labels = temp[args.y_col].tolist()
    
    return document, labels

def add_special_token(document):
    added = ["[CLS]" + str(sentence) + "[SEP]" for sentence in document]
    return added

def tokenization(tokenizer, document):
    # tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased', do_lower_case=False, )
    tokenized = [tokenizer.tokenize(sentence) for sentence in document]
    ids = [tokenizer.convert_tokens_to_ids(sentence) for sentence in tokenized]

    return ids

def padding(ids, args):
    ids = pad_sequences(ids, maxlen=args.max_len, dtype="long", truncating='post', padding='post')
    return ids

def attention_mask(ids):
    masks = []
    for id in ids:
        mask = [float(i>0) for i in id]
        masks.append(mask)
        
    return masks


def preprocess(path, tokenizer):
    document, labels = load_data(args, path)
    document = add_special_token(document)
    ids = tokenization(tokenizer, document)
    ids = padding(ids, args)
    masks = attention_mask(ids)
    del document
    return ids, masks, labels


def train_test_data_split(ids, masks, labels):
    train_ids, test_ids, train_labels, test_labels = train_test_split(ids, labels, random_state=42, test_size=0.1)
    train_masks, test_masks, _, _ = train_test_split(masks, ids, random_state=42, test_size=0.1)
    
    return train_ids, train_masks, train_labels, test_ids, test_masks, test_labels

def build_dataloader(ids, masks, label, args):
    dataloader = TensorDataset(torch.tensor(ids), torch.tensor(masks), torch.tensor(label))
    dataloader = DataLoader(dataloader, sampler=RandomSampler(dataloader), batch_size=args.batch_size)
    return dataloader



def add_tokens(tokenizer) :
    tokenizer = DistilBertTokenizer.from_pretrained("../1_category_automatching/tokenizer/kcelectra_load")
    
    return tokenizer


def get_model(model_name, add_token) :
    
    if model_name == "kcelectra" :
        tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
        model = BertForSequenceClassification.from_pretrained("beomi/KcELECTRA-base", num_labels = len(predict_crime) + 1)
    elif model_name == "kcbert" :
        tokenizer = AutoTokenizer.from_pretrained("beomi/kcbert-base")
        model = BertForSequenceClassification.from_pretrained("beomi/kcbert-base", num_labels = len(predict_crime) + 1)
    elif model_name == "koelectra" :
        tokenizer = AutoTokenizer.from_pretrained("monologg/koelectra-base-v3-discriminator")
        model = BertForSequenceClassification.from_pretrained("monologg/koelectra-base-v3-discriminator", num_labels = len(predict_crime) + 1)
    else : 
        tokenizer = AutoTokenizer.from_pretrained("skt/kobert-base-v1", use_fast=False)
        model = BertForSequenceClassification.from_pretrained("skt/kobert-base-v1", num_labels = len(predict_crime) + 1)
        
    if add_token == 'a':
        tokenizer = add_tokens(tokenizer)
        model.resize_token_embeddings(len(tokenizer)) 
    
    return model, tokenizer

    

def execute(args, model, tokenizer) :
    train_ids, train_masks, train_labels = preprocess(args.train_path, tokenizer)
    test_ids, test_masks, test_labels = preprocess(args.val_path, tokenizer)
    logger.info("Making data set finish")
    
    train_dataloader = build_dataloader(train_ids, train_masks, train_labels, args)
    test_dataloader = build_dataloader(test_ids, test_masks, test_labels, args)
    logger.info("Start training")
    acc_dict = train(model, train_dataloader, test_dataloader, args)
    logger.info("Finish training")
    
    acc = pd.DataFrame(acc_dict)
    # 결과 저장
    acc.to_csv(f"model/{args.fname}_acc.csv", index = False)




def preprocessing_for_automatching(df) :

    reFunc1 = lambda x : re.sub(string = x, pattern = r"\n+|\t+|\{+|\}+|#+|\"+|\'+|\<+|\>+|\=+|\++|\-+|~+|\:+|\&+|\!+|\u2028+", repl = " ")
    reFunc2 = lambda x : re.sub(string = x, pattern = r"\)+|\(+|-+|\.+|\s+", repl = " ")
    reFunc3 = lambda x : re.sub(string = x, pattern = r"\s{2,}", repl = " ")
    
    df.title = df.title.apply(reFunc1)
    df.title = df.title.apply(reFunc2)
    df.title = df.title.apply(reFunc3)
    
    df.body = df.body.apply(reFunc1)
    df.body = df.body.apply(reFunc2)
    df.body = df.body.apply(reFunc3)

   
    return df


def get_model(model_name, add_token) :
    
    if model_name == "kcelectra" :
        tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
        model = AutoModel.from_pretrained("beomi/KcELECTRA-base")
    elif model_name == "kcbert" :
        tokenizer = AutoTokenizer.from_pretrained("beomi/kcbert-base")
        model = AutoModel.from_pretrained("beomi/kcbert-base", num_labels = len(predict_crime) + 1)
    elif model_name == "koelectra" :
        tokenizer = AutoTokenizer.from_pretrained("monologg/koelectra-base-v3-discriminator")
        model = AutoModel.from_pretrained("monologg/koelectra-base-v3-discriminator", num_labels = len(predict_crime) + 1)
    else : 
        tokenizer = AutoTokenizer.from_pretrained("skt/kobert-base-v1", use_fast=False)
        model = AutoModel.from_pretrained("skt/kobert-base-v1", num_labels = len(predict_crime) + 1)
        
    if add_token == 'a':
        tokenizer = add_tokens(tokenizer)
        model.resize_token_embeddings(len(tokenizer)) 
    
    return model, tokenizer


def add_tokens() :
    tokenizer = DistilBertTokenizer.from_pretrained("../1_category_automatching/tokenizer/kcelectra_load")
    return tokenizer





def add_special_token(document):
    added = ["[CLS]" + str(sentence) + "[SEP]" for sentence in document]
    return added

def tokenization(tokenizer, document):
    # tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased', do_lower_case=False, )
    tokenized = [tokenizer.tokenize(sentence) for sentence in document]
    ids = [tokenizer.convert_tokens_to_ids(sentence) for sentence in tokenized]

    return ids

def padding(ids, args):
    ids = pad_sequences(ids, maxlen=args.max_len, dtype="long", truncating='post', padding='post')
    return ids

def attention_mask(ids):
    masks = []
    for id in ids:
        mask = [float(i>0) for i in id]
        masks.append(mask)
        
    return masks


def preprocess(path, tokenizer):
    document, labels = load_data(args, path)
    document = add_special_token(document)
    ids = tokenization(tokenizer, document)
    ids = padding(ids, args)
    masks = attention_mask(ids)
    del document
    return ids, masks, labels




query = '''
WITH t11 AS (
SELECT
    user_pseudo_id,
    event_name,
    DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') AS event_datetime,
    FORMAT_DATE("%Y%m%d", DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul')) AS event_ymd,
    REGEXP_REPLACE(udfs.param_value_by_key('page_location',event_params).string_value, r'.+/qna/([0-9]+).*', r'\\1') as question_slug,
    udfs.param_value_by_key('page_location',event_params).string_value as page_location,
    udfs.param_value_by_key('page_referrer',event_params).string_value as page_referrer
FROM
    `lawtalk-bigquery.analytics_265523655.events_*`
WHERE TRUE
    -- AND _TABLE_SUFFIX BETWEEN '20211001' AND '20211101'
    AND REGEXP_CONTAINS(udfs.param_value_by_key('page_location',event_params).string_value, r'.+/qna/[0-9]+.*') 
    AND udfs.param_value_by_key('page_location',event_params).string_value not like '%manager%' -- manager
) 
, t1 AS (
SELECT 
    user_pseudo_id
    , event_datetime
    , event_ymd
    , question_slug
    , page_location
    , page_referrer
FROM t11
GROUP BY 1, 2, 3, 4, 5, 6
)
, t2 AS (
SELECT 
    _id
    , answers
    , CAST(number AS string) AS question_slug
    , title
    , body
    , viewCount
    , createdAt
FROM `lawtalk-bigquery.raw.questions`

)
, t3 AS (
SELECT 
    t2.*
    , MIN(j1.createdAt) AS answer_createdAt
FROM t2
LEFT JOIN `lawtalk-bigquery.raw.answers` AS j1
ON (t2._id = j1.question)
GROUP BY 1, 2, 3, 4, 5, 6, 7
)
, t4 AS (
SELECT 
    t1.user_pseudo_id
    , t1.event_datetime
    , t1.question_slug
    , t1.event_ymd
    , t1.page_location
    , t1.page_referrer
    , t3.title
    , t3.body
    , t3.answers
    , t3.viewCount
    , t3.createdAt
    , t3.answer_createdAt
FROM t1
LEFT JOIN t3
USING (question_slug)
)
, d1 AS (
SELECT 
    question_slug
    , LEFT(event_ymd, 6) AS event_ym
    , page_referrer
    , title
    , body
    , answers
    , createdAt
    , answer_createdAt
    , COUNT(user_pseudo_id) AS pv
    , COUNT(DISTINCT user_pseudo_id) AS uv
FROM t4
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT * FROM d1
WHERE title IS NOT NULL
;
'''

# %time df = bigquery_to_pandas(query)
df["event_ym"] = pd.to_datetime(df.event_ym.astype(str), format = "%Y%m")
df = df[df.createdAt.dt.year > 2019]
df = df[~((df.createdAt.dt.year <= 2022) & (df.createdAt.dt.month <= 1))]


gdf = df.groupby(["question_slug", "event_ym", "createdAt", "title", "body"]).agg(
    sum_pv = ("pv", "sum"),
    sum_uv = ("uv", "sum")
).reset_index()

gdf = gdf[gdf.question_slug.isin(gdf.question_slug.value_counts()[(gdf.question_slug.value_counts() > 3)].index)]

# %time gdf["ym_rnk"] = gdf.sort_values(["question_slug", "event_ym"]).groupby(["question_slug"]).event_ym.rank(method = "first")
# %time gdf["sumpv_rnk"] = gdf.sort_values(["question_slug", "sum_pv"]).groupby(["question_slug"]).sum_pv.rank(method = "first", ascending = False)
# %time gdf["sumuv_rnk"] = gdf.sort_values(["question_slug", "sum_uv"]).groupby(["question_slug"]).sum_pv.rank(method = "first", ascending = False)

gdf[gdf.sumuv_rnk <= 2].question_slug.value_counts()[gdf[gdf.sumuv_rnk <= 2].question_slug.value_counts() != 2]

gdf = gdf[gdf.sumuv_rnk <= 2]

data = gdf.groupby(["question_slug", "title", "body"]).agg(
    sum_pv = ("sum_pv", "sum"),
    sum_uv = ("sum_uv", "sum")
).reset_index()


# tmp = data[data.sum_uv < 100]
# tmp.sum_uv.hist(bins = 100)
# tmp.sum_uv.value_counts().sort_index().head(15)
# data.sum_uv.sort_values(ascending = False).head(15)

data = data[data.sum_uv >= 10]
data = data[data.sum_uv != data.sum_uv.max()]


# %time data = preprocessing_for_automatching(data)


train, test = train_test_split(data, test_size = 0.1)
train.to_csv("data/train.csv", sep = "\t")
test.to_csv("data/test.csv", sep = "\t")

num_epochs = 15
warmup_ratio = 0.1
max_grad_norm = 1
learning_rate =  5e-5

model = BertForSequenceClassification.from_pretrained("beomi/KcELECTRA-base", num_labels = 1)
tokenizer = add_tokens()
model.resize_token_embeddings(len(tokenizer)) 


args = {"train_path" : "data/train.csv",
        "val_path" : "data/test.csv",
        "x_col" : "title",
        "y_col" : "sum_uv",
        "max_len" : 100,
        "batch_size" : 32,
        "epochs" : num_epochs,
        "warmup_ratio" : warmup_ratio,
        "max_grad_norm" : max_grad_norm,
        "seed_val" : 42,
        "fname" : f"{model_name}"
       }

parser = argparse.ArgumentParser()

for a in args.items() :
    parser.add_argument(f"-{a[0]}", default=a[1], type=type(a[1]))

args = parser.parse_args('')

model_name = "proto_v1"
logger = get_logger(f"./log/{datetime.datetime.now().strftime('%Y%m%d')}_{model_name}_atitle.log")

def load_data(args, path):
    temp = pd.read_csv(path, sep="\t")
    temp = temp
    document = temp[args.x_col].tolist()
    labels = temp[args.y_col].tolist()
    
    return document, labels

train_ids, train_masks, train_labels = preprocess(args.train_path, tokenizer)
test_ids, test_masks, test_labels = preprocess(args.val_path, tokenizer)

train_dataloader = build_dataloader(train_ids, train_masks, train_labels, args)
test_dataloader = build_dataloader(test_ids, test_masks, test_labels, args)

def build_model(args, model):
    # model = BertForSequenceClassification.from_pretrained("bert-base-multilingual-cased", num_labels=args.num_labels)
    if torch.cuda.is_available():
        device = torch.device("cuda")
        logger.info(f"{torch.cuda.get_device_name(0)} available")
        model = model.cuda()
    else:
        device = torch.device("cpu")
        logger.info("no GPU available")
        model = model
    
    return model, device



def loss_fn(output,target):
    return torch.sqrt(nn.MSELoss()(output,target))


model, device = build_model(args, model)

optimizer = AdamW(model.parameters(), lr=2e-5, eps=1e-8)

scheduler = get_linear_schedule_with_warmup(optimizer, num_warmup_steps=0, num_training_steps=len(train_dataloader)*args.epochs)

random.seed(args.seed_val)
np.random.seed(args.seed_val)
torch.manual_seed(args.seed_val)
torch.cuda.manual_seed_all(args.seed_val)
model.zero_grad()

acc_dict = {x : [] for x in ["epoch", "loss", "type", "datetime"]}
best_test_acc = 0.0

import gc
gc.collect()
torch.cuda.empty_cache()

for epoch in range(0, args.epochs) :
    model.train()
    print("-"*30)
    losses = []
    allpreds = []
    alltargets = []

    for step, batch in tqdm(enumerate(train_dataloader), total=len(train_dataloader)) :
        # if step % args.log_interval == 0 :
        #     print(f"Epoch : {epoch+1} in {args.epochs} / Step : {step}")

        batch = tuple(index.to(device) for index in batch)
        ids, masks, labels = batch
        # outputs = model(ids, token_type_ids=None, attention_mask=masks, labels=labels)
        output = model(ids,masks)
        output = output["logits"].squeeze(-1)
        output_ = output.to(torch.float32)
        labels_ = labels.to(torch.float32)

        # loss = outputs.loss
        # loss = loss_fn(output, labels.to(device))
        loss = loss_fn(output_, labels_.to(device))

        # total_loss += loss.item()
        losses.append(loss.item())
        allpreds.append(output.detach().cpu().numpy())
        alltargets.append(labels.detach().squeeze(-1).cpu().numpy())

        loss.backward()
        # scaler.scale(loss).backward() # backwards of loss
        # scaler.step(optimizer) # Update optimizer
        # scaler.update() # scaler update

        torch.nn.utils.clip_grad_norm_(model.parameters(), args.max_grad_norm)
        
        if step % 50 == 0 :
            print(f"Epoch : {epoch+1} in Step : {step} / rmse : {np.mean(losses)}")


        optimizer.step()
        scheduler.step()
        model.zero_grad()

    # avg_loss = total_loss / len(train_dataloader)
    # avg_accuracy = total_accuracy/len(train_dataloader)
    allpreds = np.concatenate(allpreds)
    alltargets = np.concatenate(alltargets)
    losses = np.mean(losses)

    acc_dict["epoch"] += [epoch]
    acc_dict["loss"] += [losses]
    acc_dict["type"] += ["train"]
    acc_dict["datetime"] += [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    
    logger.info(f" {epoch+1} Epoch Average train rmse : {losses}")

    acc = test(test_dataloader, model, device)

    logger.info(f" {epoch+1} Epoch Average test rmse : {acc}")


    acc_dict["epoch"] += [epoch+1]
    acc_dict["loss"] += [acc]
    acc_dict["type"] += ["validation"]
    acc_dict["datetime"] += [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

    if acc > best_test_acc :
        best_test_acc = acc
        os.makedirs("model", exist_ok=True)
        torch.save(model, f"model/{args.fname}.pt")
        torch.save(model.state_dict(), f"model/{args.fname}_state.pt")
        logger.info(f"Saved model at {epoch + 1} And Best Average test accuracy = {best_test_acc}")




def test(test_dataloader, model, device):
    model.eval() 
    losses = []
    
    for batch in test_dataloader :
        batch = tuple(index.to(device) for index in batch)
        ids, masks, labels = batch
        
        with torch.no_grad() :
            # outputs = model(ids, token_type_ids=None, attention_mask=masks)
            output = model(ids,masks)
        output = output["logits"].squeeze(-1)

        output_ = output.to(torch.float32)
        labels_ = labels.to(torch.float32)

        # loss = outputs.loss
        # loss = loss_fn(output, labels.to(device))
        loss = loss_fn(output_, labels_.to(device))
        
        losses.append(loss.item())

    losses = np.mean(losses)
    
    return losses



acc_dict = train(model, train_dataloader, test_dataloader, args)