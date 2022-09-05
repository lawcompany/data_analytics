# -*- coding: utf-8 -*-
# 
# regression 용 참고 코드 : https://www.kaggle.com/code/idwivedi/huggingface-transformers-for-regression-colab/notebook
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

from transformers import BertTokenizer, BertForSequenceClassification, AdamW, get_linear_schedule_with_warmup, Trainer
from transformers import AutoTokenizer, AutoModel, DistilBertTokenizer

from keras.preprocessing.sequence import pad_sequences

from google.cloud import bigquery

from datasets import ClassLabel, Sequence, load_dataset
datasets = load_dataset("imdb")

warnings.filterwarnings("ignore ")


parser = argparse.ArgumentParser()

parser.add_argument("-data_path", default="train_data", type= str)
parser.add_argument("-x_col", default="x", type= str)
parser.add_argument("-y_col", default="uv", type= str)
parser.add_argument("-max_len", default=150, type= int)
parser.add_argument("-batch_size", default=32, type= int)
parser.add_argument("-epochs", default=15, type= int)
parser.add_argument("-warmup_ratio", default=0.1, type= float)
parser.add_argument("-max_grad_norm", default=1, type= int)
parser.add_argument("-seed_val", default=42, type= int)
parser.add_argument("-fname", default="kcelectra_regression", type= str)
parser.add_argument("-model_name", default="kcelectra_regression", type= str)

args = parser.parse_args('')


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
            create_bqstorage_client=True,
        )
    )
    
    return b


    
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



def max_path(args, keywords) :
    tmp_path = glob(f"{args.data_path}/{keywords}")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{8}").group(0), "%Y%m%d") if re.search(string = x, pattern = "\d{8}") != None else datetime.datetime.strptime("19920503", "%Y%m%d") for x in glob(f"{args.data_path}/{keywords}")])]
    
    return tmp_path

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
    dataloader = DataLoader(dataloader, sampler=RandomSampler(dataloader), batch_size=args.batch_size)
    return dataloader



def build_model(args, model):
    if torch.cuda.is_available():
        device = torch.device("cuda")
        try :
            logger.info(f"{torch.cuda.get_device_name(0)} available")
        except :
            print("no logger now")
        model = model.cuda()
    else:
        device = torch.device("cpu")
        try :
            logger.info("no GPU available")
        except :
            print("no logger now")
        model = model
    
    return model, device



def test(test_dataloader, model, device) :
    model.eval()
    rmses = []
    rmsles = []
    
    for batch in test_dataloader :
        batch = tuple(index.to(device) for index in batch)
        ids, masks, labels = batch
        
        with torch.no_grad() :
            # outputs = model(ids, token_type_ids=None, attention_mask=masks)
            output = model(ids,masks)
        output = output["logits"].squeeze(-1)

        output_ = output.to(torch.float32)
        labels_ = labels.to(torch.float32)

        rmse = loss_rmse(output_, labels_.to(device))
        rmsle = loss_rmsle(output_, labels_.to(device))
        
        rmses.append(rmse.item())
        rmsles.append(rmsle.item())

    rmses = np.mean(rmses)
    rmsles = np.mean(rmsles)
    
    return rmses, rmsles

def loss_rmse(output,target) :
    return torch.sqrt(nn.MSELoss()(output,target))
    
def loss_rmsle(output,target) :
    return torch.sqrt(nn.MSELoss()(torch.log(output + 1), torch.log(target + 1)))




model = BertForSequenceClassification.from_pretrained("beomi/KcELECTRA-base", num_labels = 1)
tokenizer = add_tokens()
model.resize_token_embeddings(len(tokenizer))


logger = get_logger(f"./log/{datetime.datetime.now().strftime('%Y%m%d')}_{args.model_name}.log")

train_path = max_path(args, "train_*")
val_path1 = max_path(args, "val1_*")
val_path2 = max_path(args, "val2_*")

train_ids, train_masks, train_labels = preprocess(train_path, tokenizer)
test_ids1, test_masks1, test_labels1 = preprocess(val_path1, tokenizer)
test_ids2, test_masks2, test_labels2 = preprocess(val_path2, tokenizer)

train_dataloader = build_dataloader(train_ids, train_masks, train_labels, args)
test_dataloader1 = build_dataloader(test_ids1, test_masks1, test_labels1, args)
test_dataloader2 = build_dataloader(test_ids2, test_masks2, test_labels2, args)

model, device = build_model(args, model)

optimizer = AdamW(model.parameters(), lr=2e-5, eps=1e-8)
scheduler = get_linear_schedule_with_warmup(optimizer, num_warmup_steps=0, num_training_steps=len(train_dataloader)*args.epochs)

random.seed(args.seed_val)
np.random.seed(args.seed_val)
torch.manual_seed(args.seed_val)
torch.cuda.manual_seed_all(args.seed_val)
model.zero_grad()

# 학습 진행 현황 확인용
acc_dict = {x : [] for x in ["epoch", "rmse", "rmsle","type", "datetime"]}
log_interval = 50

import gc
gc.collect()
torch.cuda.empty_cache()

print(f"train mean : {np.mean(train_labels)}, train std : {np.std(train_labels)}")
print(f"test1 mean : {np.mean(test_labels1)}, test1 std : {np.std(test_labels1)}")
print(f"test2 mean : {np.mean(test_labels2)}, test2 std : {np.std(test_labels2)}")

logger.info(f"modeling in process_id = {os.getpid()}")
logger.info(f"train mean : {np.mean(train_labels)}, train std : {np.std(train_labels)}")
logger.info(f"test1 mean : {np.mean(test_labels1)}, test1 std : {np.std(test_labels1)}")
logger.info(f"test2 mean : {np.mean(test_labels2)}, test2 std : {np.std(test_labels2)}")

best_test_acc = 10**10


for epoch in range(0, 10) :
    model.train()
    print("-"*30)
    rmses = []
    rmsles = []
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
        rmse = loss_rmse(output_, labels_.to(device))
        rmsle = loss_rmsle(output_, labels_.to(device))

        # total_loss += loss.item()
        rmses.append(rmse.item())
        rmsles.append(rmsle.item())
        allpreds.append(output.detach().cpu().numpy())
        alltargets.append(labels.detach().squeeze(-1).cpu().numpy())
        
        # loss를 rmse, rmsle 선택
        rmse.backward()
        
        torch.nn.utils.clip_grad_norm_(model.parameters(), args.max_grad_norm)
        
        if step % log_interval == 0 :
            print(f"Epoch : {epoch+1} in Step : {step} / rmse : {np.mean(rmses)}")
            print(f"Epoch : {epoch+1} in Step : {step} / rmsle : {np.mean(rmsles)}")


        optimizer.step()
        scheduler.step()
        model.zero_grad()

    allpreds = np.concatenate(allpreds)
    alltargets = np.concatenate(alltargets)
    rmses = np.mean(rmses)
    rmsles = np.mean(rmsles)

    acc_dict["epoch"] += [epoch]
    acc_dict["rmse"] += [rmses]
    acc_dict["rmsle"] += [rmsles]
    acc_dict["type"] += ["train"]
    acc_dict["datetime"] += [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    
    logger.info(f"{epoch+1} Epoch Average train rmse : {rmses}")
    logger.info(f"{epoch+1} Epoch Average train rmsle : {rmsles}")

    rmse1, rmsle1 = test(test_dataloader1, model, device)
    rmse2, rmsle2 = test(test_dataloader2, model, device)

    logger.info(f" {epoch+1} Epoch Average test1 rmse : {rmse1}, rmsle : {rmsle1}")
    logger.info(f" {epoch+1} Epoch Average test2 rmse : {rmse2}, rmsle : {rmsle2}")

    acc_dict["epoch"] += [epoch+1]
    acc_dict["rmse"] += [rmse1]
    acc_dict["rmsle"] += [rmsle1]
    acc_dict["type"] += ["validation1"]
    acc_dict["datetime"] += [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    
    acc_dict["epoch"] += [epoch+1]
    acc_dict["rmse"] += [rmse2]
    acc_dict["rmsle"] += [rmsle2]
    acc_dict["type"] += ["validation2"]
    acc_dict["datetime"] += [datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

    if rmse1 <= best_test_acc :
        best_test_acc = rmse1
        os.makedirs("model", exist_ok=True)
        torch.save(model, f"model/{args.fname}_{datetime.datetime.now().strftime('%Y%m%d')}.pt")
        torch.save(model.state_dict(), f"model/{args.fname}_state_{datetime.datetime.now().strftime('%Y%m%d')}.pt")
        logger.info(f"Saved model at {epoch + 1} And Best Average test rmse = {best_test_acc}")

        
pd.DataFrame(acc_dict).to_csv(f"./result/{datetime.datetime.now().strftime('%Y%m%d')}_{args.model_name}.csv", index = False)




