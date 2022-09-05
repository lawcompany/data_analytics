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

from plotnine import *

warnings.filterwarnings("ignore")



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



parser = argparse.ArgumentParser()

parser.add_argument("-data_path", default="train_data", type= str)
parser.add_argument("-x_col", default="x", type= str)
parser.add_argument("-y_col", default="uv", type= str)
parser.add_argument("-max_len", default=200, type= int)
parser.add_argument("-batch_size", default=32, type= int)
parser.add_argument("-epochs", default=15, type= int)
parser.add_argument("-warmup_ratio", default=0.1, type= float)
parser.add_argument("-max_grad_norm", default=1, type= int)
parser.add_argument("-seed_val", default=42, type= int)
parser.add_argument("-fname", default="kcelectra_regression", type= str)
parser.add_argument("-model_name", default="kcelectra_regression", type= str)

args = parser.parse_args('')


if torch.cuda.is_available():
    device = torch.device("cuda")
else:
    device = torch.device("cpu")



model_path = glob(f"model/{args.model_name}_[1-30000000]*")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{8}").group(0), "%Y%m%d") if re.search(string = x, pattern = "\d{8}") != None else datetime.datetime.strptime("19920503", "%Y%m%d") for x in glob(f"model/{args.model_name}_[1-30000000]*")])]

model_state_path = glob(f"model/{args.model_name}_state_*")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{8}").group(0), "%Y%m%d") if re.search(string = x, pattern = "\d{8}") != None else datetime.datetime.strptime("19920503", "%Y%m%d") for x in glob(f"model/{args.model_name}_state_*")])]

%time model = torch.load(model_path, map_location=device)
%time model.load_state_dict(torch.load(model_state_path, map_location=device))

tokenizer = add_tokens()
model.resize_token_embeddings(len(tokenizer)) 

result = pd.read_csv(glob(f"result/*.csv")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{8}").group(0), "%Y%m%d") if re.search(string = x, pattern = "\d{8}") != None else datetime.datetime.strptime("19920503", "%Y%m%d") for x in glob(f"result/*.csv")])])


(
    ggplot() +
    geom_line(data = result[result.type != "validation2"], mapping = aes(x = "epoch", y = "rmse", group = "type", color = "type")) +
    geom_point(data = pd.DataFrame(result.iloc[result[(result.type == "validation1")].rmse.idxmin(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), rmse = lambda x : x.rmse.astype(float)), mapping = aes(x = "epoch", y = "rmse")) +
    geom_text(data = pd.DataFrame(result.iloc[result[(result.type == "validation1")].rmse.idxmin(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), rmse = lambda x : x.rmse.astype(float), labels = lambda x : f"{x.epoch.values[0]} : {x.rmse.values[0]}"), mapping = aes(x = "epoch", y = "rmse", label = "labels"), nudge_y =0.02) +
    theme_bw() 
)


(
    ggplot() +
    geom_line(data = result[result.type != "validation2"], mapping = aes(x = "epoch", y = "rmsle", group = "type", color = "type")) +
    geom_point(data = pd.DataFrame(result.iloc[result[(result.type == "validation1")].rmsle.idxmin(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), rmsle = lambda x : x.rmsle.astype(float)), mapping = aes(x = "epoch", y = "rmsle")) +
    geom_text(data = pd.DataFrame(result.iloc[result[(result.type == "validation1")].rmsle.idxmin(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), rmsle = lambda x : x.rmsle.astype(float), labels = lambda x : f"{x.epoch.values[0]} : {x.rmsle.values[0]}"), mapping = aes(x = "epoch", y = "rmsle", label = "labels"), nudge_y =0.02) +
    theme_bw() 
)


def preprocessing_for_automatching(df, col) :

    reFunc1 = lambda x : re.sub(string = x, pattern = r"\n+|\t+|\{+|\}+|#+|\"+|\'+|\<+|\>+|\=+|\++|\-+|~+|\:+|\&+|\!+|\u2028+", repl = " ")
    reFunc2 = lambda x : re.sub(string = x, pattern = r"\)+|\(+|-+|\.+|\s+", repl = " ")
    reFunc3 = lambda x : re.sub(string = x, pattern = r"\s{2,}", repl = " ")
    
    df[col] = df[col].apply(reFunc1)
    df[col] = df[col].apply(reFunc2)
    df[col] = df[col].apply(reFunc3)
   
    return df[col]


def load_data(args, path):
    temp = pd.read_csv(path, sep="\t")
    temp = temp
    temp[args.x_col] = preprocessing_for_automatching(temp, args.x_col)
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


def build_dataloader(ids, masks, label, args):
    dataloader = TensorDataset(torch.tensor(ids), torch.tensor(masks), torch.tensor(label))
    dataloader = DataLoader(dataloader, shuffle = False, batch_size=args.batch_size)
    return dataloader



def loss_rmse(output,target) :
    return torch.sqrt(nn.MSELoss()(output,target))
    
def loss_rmsle(output,target) :
    return torch.sqrt(nn.MSELoss()(torch.log(output + 1), torch.log(target + 1)))

def max_path(args, keywords) :
    tmp_path = glob(f"{args.data_path}/{keywords}")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{8}").group(0), "%Y%m%d") if re.search(string = x, pattern = "\d{8}") != None else datetime.datetime.strptime("19920503", "%Y%m%d") for x in glob(f"{args.data_path}/{keywords}")])]
    
    return tmp_path



def validation(test_dataloader, model, device):
    model.eval() 
    rmses = []
    rmsles = []
    predict = []
    
    for batch in tqdm(test_dataloader) :
        batch = tuple(index.to(device) for index in batch)
        ids, masks, labels = batch
        
        with torch.no_grad() :
            # outputs = model(ids, token_type_ids=None, attention_mask=masks)
            output = model(ids, masks)
        output = output["logits"].squeeze(-1)

        output_ = output.to(torch.float32)
        labels_ = labels.to(torch.float32)

        # loss = outputs.loss
        # loss = loss_fn(output, labels.to(device))
        loss = loss_rmse(output_, labels_.to(device))
        rmses.append(loss.item())
        
        loss = loss_rmsle(output_, labels_.to(device))
        rmsles.append(loss.item())
        
        predict += output_.tolist()
        
    rmses = np.mean(rmses)
    rmsles = np.mean(rmsles)
    
    return predict, rmses, rmsles

val_path1 = max_path(args, "val1_*")
val_path2 = max_path(args, "val2_*")


test_ids1, test_masks1, test_labels1 = preprocess(val_path1, tokenizer)
test_ids2, test_masks2, test_labels2 = preprocess(val_path2, tokenizer)

test_dataloader1 = build_dataloader(test_ids1, test_masks1, test_labels1, args)
test_dataloader2 = build_dataloader(test_ids2, test_masks2, test_labels2, args)

%time predict1, rmse1, rmsle1 = validation(test_dataloader1, model, device)
%time predict2, rmse2, rmsle2 = validation(test_dataloader2, model, device)

train = pd.read_csv(train_path, sep = "\t")
val1 = pd.read_csv(val_path1, sep="\t")
val2 = pd.read_csv(val_path2, sep="\t")

val1["predict"] = predict1
val2["predict"] = predict2

val1.sort_values("predict")[["question_slug", "title", "body", "x", "uv", "predict"]].tail()
val1.sort_values("predict")[["question_slug", "title", "body", "x", "uv", "predict"]].head()

from sklearn.metrics import r2_score, mean_absolute_percentage_error

val1["uv_log"] = np.log(val1.uv)
val1["predict_log"] = np.log(val1.predict)

r2_score(val1.uv, val1.predict)
mean_absolute_percentage_error(val1.uv, val1.predict)

r2_score(val1.uv_log, val1.predict)
mean_absolute_percentage_error(val1.uv_log, val1.predict)

val1.uv.describe()
val1.predict.describe()

val1.uv.hist()
val1.predict.hist()

val1.uv_log.hist()
val1.predict_log.hist()
