from glob import glob

import re
import datetime

import numpy as np
import pandas as pd


if len([x for x in glob("words_dictionary/*.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None]) == 0:
    dict_path = "words_dictionary/word_dictionary_base.csv"
else :
    dict_path = glob("words_dictionary/*.csv")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}").group(0), "%Y-%m-%d") for x in glob("words_dictionary/*.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None])]


word_dict = pd.read_csv(dict_path)

for i in word_dict[word_dict.type.isna()].iterrows() :
    print(i[1].words)
