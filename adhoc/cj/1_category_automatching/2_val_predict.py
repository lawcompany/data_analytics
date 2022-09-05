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
from sklearn.metrics import accuracy_score

import torch
from torch.utils.data import TensorDataset, DataLoader, RandomSampler, SequentialSampler

from transformers import BertTokenizer, BertForSequenceClassification, AdamW, get_linear_schedule_with_warmup, BertConfig
from transformers import AutoTokenizer, AutoModel, DistilBertTokenizer

from keras.preprocessing.sequence import pad_sequences

from google.cloud import bigquery

warnings.filterwarnings("ignore")

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

def preprocess(args, path, tokenizer):
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

## GPU
device1 = torch.device("cuda:0")
device2 = torch.device("cuda:1")

predict_crime = ["명예훼손/모욕", "성범죄", "재산범죄", "폭행/협박", "이혼", "임대차"]

model_name = "kcelectra"
add_token = ""

max_len_title = 100
max_len_body = 512
batch_size_title = 32
batch_size_body = 8

base_title = f"{model_name}_title{add_token}_{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}"
base_body = f"{model_name}_body{add_token}_{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}"

# %time model_title = torch.load(f"model/{base_title}.pt", map_location=device1)
# %time model_title.load_state_dict(torch.load(f'model/{base_title}_state.pt', map_location=device1))

# %time model_body = torch.load(f"model/{base_body}.pt", map_location=device2)
# %time model_body.load_state_dict(torch.load(f'model/{base_body}_state.pt', map_location=device2))


val_path = f"./predict_data/val_{re.sub(string = '_'.join(sorted(predict_crime)), pattern = '/', repl = '')}.csv"

df = pd.read_csv(val_path, sep = "\t")

if add_token == 'a' :
    tokenizer = DistilBertTokenizer.from_pretrained("./tokenizer/kcelectra_load")
else :
    tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")



args_title = {"val_path" : val_path,
        "predict_crime" : predict_crime,
        "x_col" : "title",
        "y_col" : "y",
        "num_labels" : len(predict_crime) + 1, 
        "max_len" : max_len_title,
        "batch_size" : batch_size_title,
       }

args_body = {"val_path" : val_path,
        "predict_crime" : predict_crime,
        "x_col" : "body",
        "y_col" : "y",
        "num_labels" : len(predict_crime) + 1, 
        "max_len" : max_len_body,
        "batch_size" : batch_size_body,
       }

parser_title = argparse.ArgumentParser()
parser_body = argparse.ArgumentParser()

for a in args_title.items() :
    parser_title.add_argument(f"-{a[0]}", default=a[1], type=type(a[1]))

for a in args_body.items() :
    parser_body.add_argument(f"-{a[0]}", default=a[1], type=type(a[1]))

args_title = parser_title.parse_args('')
args_body = parser_body.parse_args('')


test_ids_title, test_masks_title, test_labels_title = preprocess(args_title, val_path, tokenizer)
test_ids_body, test_masks_body, test_labels_body = preprocess(args_body, val_path, tokenizer)

title_dataloader = build_dataloader(test_ids_title, test_masks_title, test_labels_title, args_title)
body_dataloader = build_dataloader(test_ids_body, test_masks_body, test_labels_body, args_body)

torch.cuda.empty_cache()

model_title.eval() 
total_accuracy = 0

title_logit = []
title_pred = []

for batch in tqdm(title_dataloader) :
    batch = tuple(index.to(device1) for index in batch)
    ids, masks, labels = batch

    with torch.no_grad() :
        outputs = model_title(ids, token_type_ids=None, attention_mask=masks)

    pred = [torch.argmax(logit).cpu().detach().item() for logit in outputs.logits]
    
    
    title_logit += outputs.logits.cpu().tolist()
    title_pred += pred
    true = [label for label in labels.cpu().numpy()]
    accuracy = accuracy_score(true, pred)
    total_accuracy += accuracy

avg_accuracy = total_accuracy/len(title_dataloader)



model_body.eval() 
total_accuracy = 0

body_logit = []
body_pred = []

for batch in tqdm(body_dataloader) :
    batch = tuple(index.to(device2) for index in batch)
    ids, masks, labels = batch

    with torch.no_grad() :
        outputs = model_body(ids, token_type_ids=None, attention_mask=masks)

    pred = [torch.argmax(logit).cpu().detach().item() for logit in outputs.logits]
    
    body_logit += outputs.logits.cpu().tolist()
    body_pred += pred
    
    true = [label for label in labels.cpu().numpy()]
    accuracy = accuracy_score(true, pred)
    total_accuracy += accuracy

avg_accuracy = total_accuracy/len(body_dataloader)


result = pd.DataFrame({"title_logit" : title_logit, "title_pred" : title_pred, "body_logit" : body_logit, "body_pred" : body_pred})

result = pd.concat([df, result], axis = 1)


result.to_csv(f"result/{model_name}_{add_token}{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}", index = False)
