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

from plotnine import *

warnings.filterwarnings("ignore")


standwords_list = ["상담", "변호사", "설명"]
standpos_list = ["Noun", "Verb", "Adverb", "Adjective"]


if len([x for x in glob("words_dictionary/*_edit.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None]) == 0:
    dict_path = "words_dictionary/word_dictionary_base.csv"
else :
    dict_path = glob("words_dictionary/*_edit.csv")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}").group(0), "%Y-%m-%d") for x in glob("words_dictionary/*_edit.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None])]

word_dict = pd.read_csv(dict_path)

for i in glob("lawyer_words_before/*.csv") :
    tmp = pd.read_csv(i)
    tmp = pd.merge(tmp, word_dict.assign(word = lambda x : x.words)[["word", "final_words", "type"]], on = "word", how = "left")
    tmp.loc[tmp.final_words.isna() , "final_words"] = tmp.loc[tmp.final_words.isna() , "word"]
    tmp = tmp[tmp.type != "stopwords"]
    tmp = tmp[tmp.pos.isin(standpos_list)]
    tmp = tmp[~tmp.final_words.isin(standwords_list)]
    
    tmp.to_csv("lawyer_words_after/" + re.split(string = i , pattern = "lawyer_words_before/")[1], index = False)
    
    tmp["ranks"] = tmp.groupby("stand").ratio_diff.transform("rank", method = "first", ascending = False)
    tmp["rank_word"] = tmp["ranks"].astype(int).astype(str).str.zfill(4) + "_" + tmp.word

#     ggplot_grp = (
#         ggplot() +
#         geom_col(data = tmp, mapping = aes(x = "rank_word", y = "ratio_diff", fill = "stand"), position = "identity") +
#         coord_flip() +
#         facet_grid("stand~.", scales = "free") +
#         theme_bw() +
#         theme(legend_position= "none", figure_size = (10, 20), text = element_text(family = "AppleGothic"))
#     )

#     ggsave(ggplot_grp, "test_grp/" + re.sub(string = re.split(string = i , pattern = "lawyer_words_before/")[1], pattern = ".csv", repl = ".jpeg"))





