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

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from plotnine import *

warnings.filterwarnings("ignore")

pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 10)
pd.set_option('max_colwidth', -1)


predict_crime = ["명예훼손/모욕", "성범죄", "재산범죄", "폭행/협박", "이혼", "임대차"]

model_name = "kcelectra"
add_token = "a"


def read_json(path) :
    with open(path) as json_file:
        tmp = json.load(json_file)
    return tmp

def write_json(path, data) :
    with open(path, 'w') as outfile:
        json.dump(data, outfile)
    return


label = read_json(f"predict_data/label_{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}.json")

aresult = pd.read_csv(f"result/{model_name}_{add_token}{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}")

bresult = pd.read_csv(f"result/{model_name}_{re.sub(string = ''.join(sorted(predict_crime)), pattern = '/', repl = '')}")


aresult["title_label"] = aresult.title_pred.replace({y : x for x, y in label.items()})
bresult["title_label"] = bresult.title_pred.replace({y : x for x, y in label.items()})

aresult["body_label"] = aresult.body_pred.replace({y : x for x, y in label.items()})
bresult["body_label"] = bresult.body_pred.replace({y : x for x, y in label.items()})

aresult.loc[aresult.y == 6, "categoryName"] = "그외"
bresult.loc[aresult.y == 6, "categoryName"] = "그외"


bresult[["_id", "title", "body", "categoryName", "title_label", "body_label"]]

bresult[(bresult.categoryName != bresult.title_label) & (bresult.categoryName != bresult.body_label)][["_id", "title", "body", "categoryName", "title_label", "body_label"]]


aresult[["_id", "title", "body", "categoryName", "title_label", "body_label"]]

aresult[(aresult.categoryName != aresult.title_label) & (aresult.categoryName != aresult.body_label)][["_id", "title", "body", "categoryName", "title_label", "body_label"]]


acc1 = pd.read_csv("model/kcelectra_title_명예훼손모욕성범죄이혼임대차재산범죄폭행협박_acc.csv")
acc2 = pd.read_csv("model/kcelectra_titlea_명예훼손모욕성범죄이혼임대차재산범죄폭행협박_acc.csv")
acc3 = pd.read_csv("model/kcelectra_body_명예훼손모욕성범죄이혼임대차재산범죄폭행협박_acc.csv")
acc4 = pd.read_csv("model/kcelectra_bodya_명예훼손모욕성범죄이혼임대차재산범죄폭행협박_acc.csv")


(
    ggplot() +
    geom_line(data = acc1, mapping = aes(x = "epoch", y = "acc", group = "type", color = "type")) +
    # geom_line(data = acc1[acc1.type == "train"], mapping = aes(x = "epoch", y = "loss", group = "type"), color = "blue") +
    geom_point(data = pd.DataFrame(acc1.iloc[acc1[(acc1.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float)), mapping = aes(x = "epoch", y = "acc")) +
    geom_label(data = pd.DataFrame(acc1.iloc[acc1[(acc1.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float), labels = lambda x : f"{x.epoch.values[0]} : {x.acc.values[0]}"), mapping = aes(x = "epoch", y = "acc", label = "labels"), nudge_y =0.01) +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(family = "AppleGothic"))
)





(
    ggplot() +
    geom_line(data = acc3, mapping = aes(x = "epoch", y = "acc", group = "type", color = "type")) +
    # geom_line(data = acc1[acc1.type == "train"], mapping = aes(x = "epoch", y = "loss", group = "type"), color = "blue") +
    geom_point(data = pd.DataFrame(acc3.iloc[acc3[(acc3.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float)), mapping = aes(x = "epoch", y = "acc")) +
    geom_label(data = pd.DataFrame(acc3.iloc[acc3[(acc3.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float), labels = lambda x : f"{x.epoch.values[0]} : {x.acc.values[0]}"), mapping = aes(x = "epoch", y = "acc", label = "labels"), nudge_y =0.015) +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(family = "AppleGothic"))
)





(
    ggplot() +
    geom_line(data = acc2, mapping = aes(x = "epoch", y = "acc", group = "type", color = "type")) +
    # geom_line(data = acc1[acc1.type == "train"], mapping = aes(x = "epoch", y = "loss", group = "type"), color = "blue") +
    geom_point(data = pd.DataFrame(acc2.iloc[acc2[(acc2.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float)), mapping = aes(x = "epoch", y = "acc")) +
    geom_label(data = pd.DataFrame(acc2.iloc[acc2[(acc2.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float), labels = lambda x : f"{x.epoch.values[0]} : {x.acc.values[0]}"), mapping = aes(x = "epoch", y = "acc", label = "labels"), nudge_y =0.01) +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(family = "AppleGothic"))
)




(
    ggplot() +
    geom_line(data = acc4, mapping = aes(x = "epoch", y = "acc", group = "type", color = "type")) +
    # geom_line(data = acc1[acc1.type == "train"], mapping = aes(x = "epoch", y = "loss", group = "type"), color = "blue") +
    geom_point(data = pd.DataFrame(acc4.iloc[acc4[(acc4.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float)), mapping = aes(x = "epoch", y = "acc")) +
    geom_label(data = pd.DataFrame(acc4.iloc[acc4[(acc4.type == "validation")].acc.idxmax(), :]).T.assign(epoch = lambda x : x.epoch.astype(int), acc = lambda x : x.acc.astype(float), labels = lambda x : f"{x.epoch.values[0]} : {x.acc.values[0]}"), mapping = aes(x = "epoch", y = "acc", label = "labels"), nudge_y =0.015) +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(family = "AppleGothic"))
)