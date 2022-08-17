import os
import sys
import warnings

from glob import glob
import random
import datetime
import re
from tqdm import tqdm

import pandas as pd
import numpy as np

from google.cloud import bigquery

import matplotlib as mpl
import matplotlib.pyplot as plt 
import matplotlib.font_manager as fm  

import gspread
from oauth2client.service_account import ServiceAccountCredentials


from plotnine import *

warnings.filterwarnings('ignore')


path = '/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf' 
font_name = fm.FontProperties(fname=path, size=10).get_name()
print(font_name)
plt.rc('font', family=font_name)
font = fm.FontProperties(fname=path, size=9)


pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 100)
pd.set_option('max_colwidth', -1)
pd.options.display.float_format = '{:.2f}'.format




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

def gs_append(url, credential_path = '../99_credential/lawtalk-bigquery-2bfd97cf0729.json', scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']) : 
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    
    sheet_content = sheet.get_all_values()
    
    df_ = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    return df_

theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())


advice_cloud = bigquery_to_pandas(read_bql("../bql/cloud_history.bql"))
advice_cloud = advice_cloud[advice_cloud.slug.notna()]
advice_cloud.review_cnt = advice_cloud.review_cnt.fillna(0)
advice_cloud.visibleCloudType = advice_cloud.visibleCloudType.fillna("normal")

before = advice_cloud[advice_cloud.createdAt.dt.date.isin(pd.date_range(datetime.datetime(2022, 4, 1), datetime.datetime(2022, 5, 1)).date)]

after = advice_cloud[advice_cloud.createdAt.dt.date.isin(pd.date_range(datetime.datetime(2022, 7, 3), datetime.datetime(2022, 8, 3)).date)]

after.groupby(["visibleCloudType", "review_cnt"]).slug.count().reset_index().corr()

before.groupby(["slug", "review_cnt"])._id.count().reset_index().corr()
after.groupby(["slug", "review_cnt"])._id.count().reset_index().corr()
before[before.review_cnt >= 50].groupby(["slug", "review_cnt"])._id.count().reset_index().corr()


def corr_plot(df) :
    tmp = (
        ggplot() +
        geom_point(data = df.groupby(["slug", "review_cnt"])._id.count().reset_index(), mapping = aes(x = "review_cnt", y = "_id")) +
        theme_bw()
    )
    
    print(tmp)
    
    return

corr_plot(before)
corr_plot(after)

corr_plot(after)


after[after.slug.isin(after.groupby(["visibleCloudType", "slug", "review_cnt"])._id.count().reset_index().slug.value_counts().loc[lambda x : x > 1].index)]

after[after.slug.isin(after.groupby(["visibleCloudType", "slug", "review_cnt"])._id.count().reset_index().slug.value_counts().loc[lambda x : x > 1].index)].sort_values(by = ["slug", "updateCloudAt"])

(
        ggplot() +
        geom_point(data = after.groupby(["visibleCloudType", "slug", "review_cnt"])._id.count().reset_index().rename(columns = {"_id" : "advice_cnt"}), mapping = aes(x = "review_cnt", y = "advice_cnt", color = "visibleCloudType"), alpha = 0.5) +
        theme_bw()
)

corr_plot(after[after.visibleCloudType == "normal"])
corr_plot(after[after.visibleCloudType == "review-research"])
corr_plot(after[after.visibleCloudType == "self-answer"])

after.groupby(["visibleCloudType", "review_cnt"]).slug.count().reset_index().corr()

after[after.visibleCloudType == "normal"].groupby(["visibleCloudType", "review_cnt"]).slug.count().reset_index().corr()
after[after.visibleCloudType == "review-research"].groupby(["visibleCloudType", "review_cnt"]).slug.count().reset_index().corr()
after[after.visibleCloudType == "self-answer"].groupby(["visibleCloudType", "review_cnt"]).slug.count().reset_index().corr()


after.groupby(["visibleCloudType", "review_cnt"]).lawyer.count().reset_index()

(
    ggplot() +
    geom_col(data = after.groupby(["visibleCloudType", "review_cnt"]).lawyer.unique().explode("lawyer").reset_index(), mapping = aes(x = "review_cnt", y = "lawyer", fill = "visibleCloudType")) +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)

(
    ggplot() +
    geom_histogram(data = after.groupby(["visibleCloudType", "review_cnt"]).lawyer.unique().reset_index().explode("lawyer"), mapping = aes(x = "review_cnt", fill = "visibleCloudType"), alpha = 0.4, position = "identity") +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)

(
    ggplot() +
    geom_histogram(data = after, mapping = aes(x = "review_cnt", fill = "visibleCloudType"), alpha = 0.3, position = "identity")
)



