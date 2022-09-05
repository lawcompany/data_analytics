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

import numpy as np
import pandas as pd

from bson.objectid import ObjectId

warnings.filterwarnings("ignore")


l = glob("lawyers_test/*.csv")
len(l)



result = [
    {
        "lawyer" : j.split("/")[1].split(".")[0], 
        "reviewData" : [
            {"text" : i[1]["final_words"] , "rate" : i[1]["ratio"]} for i in pd.read_csv(j).iterrows()
        ]
    }
for j in l]


with open("lawyers_json/test_result.json", "w", encoding='utf-8') as json_file:
    json.dump(result, json_file)
    
