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


def preprocessing_for_automatching(df, predict_crimes, spell_chk = False) :

    train_ = df[df._id.isin(df.groupby("_id").categoryName.nunique()[df.groupby("_id").categoryName.nunique() == 1].index)]

    train_ = pd.concat([train_[train_.categoryName.isin(predict_crimes)], train_[~train_.categoryName.isin(predict_crimes)].sample(int(train_[train_.categoryName.isin(predict_crimes)].categoryName.value_counts().mean()))])
    
    pair = {x : y for x, y in zip(predict_crimes, range(len(predict_crimes)))}
    pair["그외"] = len(predict_crimes)

    train_["y"] = train_["categoryName"].replace(pair)
    train_.loc[~train_["y"].isin(range(len(predict_crimes))), "y"] = len(predict_crimes)

    print(train_.y.value_counts().sort_index())

    reFunc1 = lambda x : re.sub(string = x, pattern = r"\n+|\t+|\{+|\}+|#+|\"+|\'+|\<+|\>+|\=+|\++|\-+|~+|\:+|\&+|\!+|\u2028+", repl = " ")
    reFunc2 = lambda x : re.sub(string = x, pattern = r"\)+|\(+|-+|\.+|\s+", repl = " ")
    reFunc3 = lambda x : re.sub(string = x, pattern = r"\s{2,}", repl = " ")
    
    train_.title = train_.title.apply(reFunc1)
    train_.title = train_.title.apply(reFunc2)
    train_.title = train_.title.apply(reFunc3)
    
    train_.body = train_.body.apply(reFunc1)
    train_.body = train_.body.apply(reFunc2)
    train_.body = train_.body.apply(reFunc3)

    if spell_chk == True :
        train_.body = train_.body.apply(lambda x : spell_check(x))
        train_.title = train_.title.apply(lambda x : spell_check(x))
    
    return train_, pair


def if_not_file_exist(args) :
    # data load
    query_string = """
    WITH t1 AS (
        SELECT 
            _id
            , title
            , LENGTH(title) AS title_len
            , body
            , LENGTH(body) AS body_len
            , categories
            , REGEXP_EXTRACT(category, r'[a-zA-Z0-9]+') AS category
            , reject
            , viewCount
            , createdAt
        FROM `lawtalk-bigquery.raw.questions`
            , UNNEST(REGEXP_EXTRACT_ALL(categories, r'\\(\\'[a-zA-Z0-9]+\\'\\)')) AS category
    )
    , t2 AS (
        SELECT 
            t1.*
            , t2.name AS categoryName
        FROM t1
        JOIN `lawtalk-bigquery.raw.adcategories` AS t2
        ON (t1.category = t2._id)
    )
    SELECT * FROM t2
    """

    # data load
    logger.info("Data Loading to Bigquery")
    df = bigquery_to_pandas(query_string)

    logger.info("Bigquery Data Preprocessing")
    train_, pair = preprocessing_for_automatching(df, predict_crime)
    train_, val_ = train_test_split(train_, stratify = train_[args.y_col], test_size = 0.1, random_state = 1)

    with open(f"predict_model/label_{re.sub(string = ''.join(sorted(args.predict_crime)), pattern = '/', repl = '')}.json", 'w') as fp:
        json.dump(pair, fp)

    logger.info("Bigquery data")
    train_.reset_index(drop = True).to_csv(args.train_path, index = False, sep = "\t")
    val_.reset_index(drop = True).to_csv(args.val_path, index = False, sep = "\t")
    
    return


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

def test(test_dataloader, model, device):
    model.eval() 
    total_accuracy = 0
    for batch in test_dataloader :
        batch = tuple(index.to(device) for index in batch)
        ids, masks, labels = batch
        
        with torch.no_grad() :
            outputs = model(ids, token_type_ids=None, attention_mask=masks)
            
        pred = [torch.argmax(logit).cpu().detach().item() for logit in outputs.logits]
        true = [label for label in labels.cpu().numpy()]
        accuracy = accuracy_score(true, pred)
        total_accuracy += accuracy
    
    avg_accuracy = total_accuracy/len(test_dataloader)
    
    return avg_accuracy


def train(model, train_dataloader, test_dataloader, args) :
    model, device = build_model(args, model)
    
    optimizer = AdamW(model.parameters(), lr=2e-5, eps=1e-8)
    
    scheduler = get_linear_schedule_with_warmup(optimizer, num_warmup_steps=0, num_training_steps=len(train_dataloader)*args.epochs)
    
    random.seed(args.seed_val)
    np.random.seed(args.seed_val)
    torch.manual_seed(args.seed_val)
    torch.cuda.manual_seed_all(args.seed_val)
    model.zero_grad()
    
    
    acc_dict = {x : [] for x in ["epoch", "loss", "acc", "type", "datetime"]}
    best_test_acc = 0.0
    
    for epoch in range(0, args.epochs) :
        model.train()
        total_loss, total_accuracy = 0, 0
        print("-"*30)
        for step, batch in tqdm(enumerate(train_dataloader), total=len(train_dataloader)) :
            # if step % args.log_interval == 0 :
            #     print(f"Epoch : {epoch+1} in {args.epochs} / Step : {step}")
            
            batch = tuple(index.to(device) for index in batch)
            ids, masks, labels = batch
            outputs = model(ids, token_type_ids=None, attention_mask=masks, labels=labels)
            loss = outputs.loss
            total_loss += loss.item()
            pred = [torch.argmax(logit).cpu().detach().item() for logit in outputs.logits]
            true = [label for label in labels.cpu().numpy()]
            accuracy = accuracy_score(true, pred)
            total_accuracy += accuracy 
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), args.max_grad_norm)
            optimizer.step()
            scheduler.step()
            model.zero_grad()
            
        avg_loss = total_loss / len(train_dataloader)
        avg_accuracy = total_accuracy/len(train_dataloader)
        
        acc_dict["epoch"] += [epoch]
        acc_dict["loss"] += [avg_loss]
        acc_dict["acc"] += [avg_accuracy]
        acc_dict["type"] += ["train"]
        acc_dict["datetime"] += [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        
        logger.info(f" {epoch+1} Epoch Average train accuracy : {avg_accuracy} And Average train loss : {avg_loss}")
        
        
        acc = test(test_dataloader, model, device)
        
        logger.info(f" {epoch+1} Epoch Average test accuracy : {acc}")
        
        acc_dict["epoch"] += [epoch+1]
        acc_dict["loss"] += [0]
        acc_dict["acc"] += [acc]
        acc_dict["type"] += ["validation"]
        acc_dict["datetime"] += [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
        
        if acc > best_test_acc :
            best_test_acc = acc
            os.makedirs("model", exist_ok=True)
            torch.save(model, f"model/{args.fname}.pt")
            torch.save(model.state_dict(), f"model/{args.fname}_state.pt")
            logger.info(f"Saved model at {epoch + 1} And Best Average test accuracy = {best_test_acc}")
        
    return acc_dict


def add_tokens(tokenizer) :
#     tokens = pd.read_csv("../tokenizers/corpus/vocab_size32000/vocab.txt", sep = "\n", names = ["token"])
#     tokens = tokens[5:]
#     tokens = tokens[tokens.token.str.len() != 1]
#     tokens = tokens[~((tokens.token.str[:2] == "##") & (tokens.token.str.len() == 3))]
#     tokens = tokens[~tokens.token.str[-1:].isin(["은", "는", "이", "가", "을", "를", "에","데", "요"])]

#     tokens = tokens[(tokens.token.str[:2] != "##") & (~tokens.token.isin(pd.DataFrame(tokenizer.vocab.keys(), columns = ["token"])[(pd.DataFrame(tokenizer.vocab.keys(), columns = ["token"]).token.str[:2] == "##")].token.str[2:].values))]

#     tokenizer.add_tokens(list(tokens.token.values))

    tokenizer = DistilBertTokenizer.from_pretrained("./tokenizer/kcelectra_load")
    
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



if __name__ == "__main__" :
        
    predict_crime = ["명예훼손/모욕", "성범죄", "재산범죄", "폭행/협박", "이혼", "임대차"]
    model_name = "kcelectra"
    t_or_b = "title"
    add_token = ""
    
    max_len = 100 if t_or_b == "title" else 512
    batch_size = 32 if t_or_b == "title" else 8
    
    num_epochs = 15
    warmup_ratio = 0.1
    max_grad_norm = 1
    learning_rate =  5e-5
    
    # log_interval = 1000
    
        
    logger = get_logger(f"./log/{datetime.datetime.now().strftime('%Y%m%d')}_{model_name}_{add_token}{t_or_b}_{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}.log")
    
    logger.info(f"working process id = {os.getpid()}")
    
    
    # data path
    train_path = f"./predict_data/train_{re.sub(string = '_'.join(sorted(predict_crime)), pattern = '/', repl = '')}.csv"
    val_path = f"./predict_data/val_{re.sub(string = '_'.join(sorted(predict_crime)), pattern = '/', repl = '')}.csv"
    
    file_exist = len(glob(f"predict_data/train_{re.sub(string = '_'.join(sorted(predict_crime)), pattern = '/', repl = '')}.csv")) > 0

    base_ = f"{model_name}_{t_or_b}{add_token}_{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}"
    
    if file_exist != True : 
        if_not_file_exist(args)

    args = {"train_path" : train_path,
            "val_path" : val_path,
            "predict_crime" : predict_crime,
            "x_col" : t_or_b,
            "y_col" : "y",
            "num_labels" : len(predict_crime) + 1, 
            "max_len" : max_len,
            "batch_size" : batch_size,
            "epochs" : num_epochs,
            "warmup_ratio" : warmup_ratio,
            "max_grad_norm" : max_grad_norm,
            "seed_val" : 42,
            # "log_interval" : log_interval,
            "fname" : base_
           }
    
    parser = argparse.ArgumentParser()
    
    for a in args.items() :
        parser.add_argument(f"-{a[0]}", default=a[1], type=type(a[1]))
    
    args = parser.parse_args('')


    logger.info("Model Loading")
    # data tokenizing & data loaderizing
    
    model, tokenizer = get_model(model_name, add_token = add_token)
    
    logger.info("Start execute")
    execute(args, model, tokenizer)
