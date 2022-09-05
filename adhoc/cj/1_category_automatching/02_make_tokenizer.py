# -*- coding: utf-8 -*-
import os
import sys
import warnings
import copy

import re
import json
from glob import glob
from tqdm import tqdm 

import numpy as np
import pandas as pd

from transformers import AutoTokenizer, ElectraTokenizer, DistilBertTokenizer

tokenizer = AutoTokenizer.from_pretrained("beomi/KcELECTRA-base")
tokenizer_add = copy.deepcopy(tokenizer)
tokenizer_final = copy.deepcopy(tokenizer)

tokens = pd.read_csv("corpus/vocab_size32000/vocab.txt", sep = "\n", names = ["token"])
# special token 제거 & 한글자 & ##한글자 제거
tokens = tokens[5:]
tokens = tokens[tokens.token.str.len() != 1]
tokens = tokens[~((tokens.token.str[:2] == "##") & (tokens.token.str.len() == 3))]
# 아래의 글자로 끝나는 단어 제거 - 아마 있는 단어 뒤에 붙었을 가능성이 높아서
tokens = tokens[~tokens.token.str[-1:].isin(["은", "는", "이", "가", "을", "를", "에","데", "될"])]
# 기존의 ##'단어'가 존재하는데 새로 만든 vocab에 없을 경우 그냥 새로운 단어로 치환되는 것을 방지그래도 뭔가 이어지는 부분이 괜히 성능을 저하시킬 것 같아서
t = pd.DataFrame(tokenizer.vocab.keys(), columns = ["token"])
tokens = tokens[(tokens.token.str[:2] != "##") & (~tokens.token.isin(t[(t.token.str[:2] == "##")].token.str[2:].values))]

tokenizer
tokenizer_add = copy.deepcopy(tokenizer)

tokenizer_add.add_tokens(list(tokens.token.values))




samples = "모욕죄가 성립이 될까요?"
", ".join(tokenizer.tokenize(samples))
", ".join(tokenizer_add.tokenize(samples))

# %timeit ", ".join(tokenizer.tokenize(samples))
# %timeit ", ".join(tokenizer_add.tokenize(samples))


origin_model = "kcelectra"
add_model = f"{origin_model}_at32000"
load_model = f"{origin_model}_load"

tok_json = "tokenizer.json"
add_json = "added_tokens.json"
voc_txt = "vocab.txt"

tokenizer.save_pretrained(f"tokenizer/{load_model}")
tokenizer.save_pretrained(f"tokenizer/{load_model}")
tokenizer_add.save_pretrained(f"tokenizer/{add_model}")


json.loads(f"tokenizer/{add_model}/added_tokens.json")

def read_json(path) :
    with open(path) as json_file:
        tmp = json.load(json_file)
    return tmp

def write_json(path, data) :
    with open(path, 'w') as outfile:
        json.dump(data, outfile)
    return


def read_txt(path) :
    f = open(path, 'r')
    tmp = f.readlines()
    tmp = [re.sub("\n", "", i) for i in tmp]
    f.close()
    
    return tmp

def write_txt(path, data) :
    with open(path, "w") as file:
        file.writelines(data)
    return


orgin_token = read_json(f"tokenizer/{origin_model}/{tok_json}")
add_token = read_json(f"tokenizer/{add_model}/{add_json}")
final_token = read_json(f"tokenizer/{load_model}/{tok_json}")
load_vocab = read_txt(f"tokenizer/{load_model}/{voc_txt}")

new_vocab = {**orgin_token["model"]["vocab"], **add_token}
final_token["model"]["vocab"] = new_vocab

load_vocab = load_vocab + list(add_token.keys())
load_vocab = [i + "\n" for i in load_vocab]

write_json(f"tokenizer/{load_model}/{tok_json}", new_vocab)
write_txt(f"tokenizer/{load_model}/{voc_txt}", load_vocab)


%time tokenizer_add_load = DistilBertTokenizer.from_pretrained(f"./tokenizer/{load_model}")


'''test'''
samples = "통매음으로 고소 가능성이 있을까요?"
samples = "직장내에 성추행을 당해서 고소하려하는데, 증거가 불충분해서 법적효능이 있는지 궁금합니다"
samples = "모욕죄가 성립이 될까요?"
samples = "방송인의 학폭사실을 폭로하려는데 모욕죄나 명예훼손에 해당될까요?"
samples = "도촬 당했습니다. 몰카를 퍼뜨린다고 협박 당하고 있는데 해결방안이 있을까요?"
samples = "강제추행 피해자입니다. 가해자가 항소를 하였는데 어떻게 대응해야 하나요?"
samples = "전혀 기억나지 않은 전날 만취상태에서 제가 119구급대원 뺨을 1대 때렸다고 하네요."

samples = '''상가 매입후 기존 임차인이 보증금에서 3천을 빼주고 30만을 더 내신다고 하여 3천 빼드리고 계약서 재작성을 하였는데
한달후 기존 계약서로 대출을 받았다는것을 알았습니다
저는 상가 매도인과 임차인 둘다에게 얘기 들은적도 없구요
이후 임대료 관리비 연체로 인하여 보증금 다공제하고 나가셨는데 대출업체에서 저에게 3천을 물어내라고 하는데 이게 제잘못이 있나요?'''

%timeit ", ".join(tokenizer.tokenize(samples))
%timeit ", ".join(tokenizer_add.tokenize(samples))
%timeit ", ".join(tokenizer_add_load.tokenize(samples))


", ".join(tokenizer.tokenize(samples))
", ".join(tokenizer_add.tokenize(samples))
", ".join(tokenizer_add_load.tokenize(samples))

len(tokenizer)
len(tokenizer_add_load)

tokenizer(samples)
tokenizer_add(samples)
tokenizer_add_load(samples, return_token_type_ids = True)


