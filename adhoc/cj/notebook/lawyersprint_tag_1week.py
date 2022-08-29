#!/usr/bin/env python
# coding: utf-8


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


def read_bql(file_name) :
    with open(file_name, "r") as f :
        bql = f.read()
    return bql


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




def get_gini(df, val_col, group_key = None, color = "red", plot_tf = True) :
    tmp = df.copy()
    tmp["ratio"] = tmp[val_col] / tmp[val_col].max()
    tmp["rnk"] = tmp.ratio.rank(method = "first")  / tmp.shape[0]
    tmp = tmp.sort_values("rnk")
    tmp["eq_line"] = [i / tmp.shape[0] for i in range(tmp.shape[0])]
    
    lorenz = (tmp.rnk * tmp.shape[0] * (tmp.ratio / tmp.shape[0])).sum()
    tri = ((tmp.rnk * tmp.shape[0]).max() * tmp.rnk.max()) / 2
    if group_key == None :
        col_plot = geom_col(mapping = aes(x = "rnk", y = "ratio"), fill = color, alpha = 0.6)
    else :
        col_plot = geom_col(mapping = aes(x = "rnk", y = "ratio", fill = group_key), alpha = 0.6)

    
    if plot_tf == True :
        print((
            ggplot(data = tmp) +
            col_plot +
            geom_line(mapping = aes(x = "rnk", y = "eq_line"), group = 1) +
            theme_bw() +
            theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
        ))
    
    return (tri - lorenz) / tri

def ad_active(df, d) :
    return (df.adorders_start_date <= d) & (df.adorders_end_date >= d) & ~((df.pauseHistory_startAt_date <= d) & (df.pauseHistory_endAt_date >= d)) & (df.adorders_status == 'apply') & (df.adpayments_status == 'paid')

def ad_active_dates(df, date_list) :
    return pd.concat([df[ad_active(df, d)] for d in date_list]).drop_duplicates()

def prefix_rnk(df, str_cols, y_cols, zfill_ns = 2) :
    return df[y_cols].rank(ascending = False).astype(int).astype(str).str.zfill(zfill_ns) + "_" + df[str_cols]


adorder_with_tag = bigquery_to_pandas(read_bql("../bql/adorder_with_tag.bql"))
advice = bigquery_to_pandas(read_bql("../bql/advice_query.bql"))

bdate_range = pd.date_range(datetime.date(2022, 8, 16), datetime.date(2022, 8, 22))
adate_range = pd.date_range(datetime.date(2022, 8, 23), datetime.date(2022, 8, 31))

bextract_advice = advice[(advice.adCategory_name != '기타') & (advice.createdAt.dt.date.isin(bdate_range.date))]
aextract_advice = advice[(advice.adCategory_name != '기타') & (advice.createdAt.dt.date.isin(adate_range.date))]

bextract_advice = pd.merge(bextract_advice, adorder_with_tag[["lawyer", "slug"]], left_on = "lawyer_id", right_on = "lawyer", how = "left")
aextract_advice = pd.merge(aextract_advice, adorder_with_tag[["lawyer", "slug", "tags"]], left_on = "lawyer_id", right_on = "lawyer", how = "left")

extract_advice = pd.concat([bextract_advice.assign(tp = "b"), aextract_advice.assign(tp = "a")])

extract_advice = extract_advice.assign(tags_exist = lambda x : np.where(x.tags.notna(), "t", "n"))

i = extract_advice[extract_advice.tp == "a"].groupby("adCategory_name").lawyer_id.nunique().sort_values(ascending = False).index[0]

chk1 = extract_advice[extract_advice.adCategory_name == i]

chk1.groupby("tp").lawyer_id.nunique()

chk1[chk1.tp == "a"].groupby(["slug", "tags_exist"])._id.count().sort_values(ascending = False).reset_index()
chk1[chk1.tp == "b"].groupby(["slug", "tags_exist"])._id.count().sort_values(ascending = False).reset_index().head()


get_gini(chk1[chk1.tp == "b"].groupby(["slug", "tags_exist"])._id.count().sort_values(ascending = False).reset_index(), "_id", group_key = "tags_exist")

get_gini(chk1[chk1.tp == "a"].groupby(["slug", "tags_exist"])._id.count().sort_values(ascending = False).reset_index(), "_id", group_key = "tags_exist")



get_gini(chk1[chk1.tp == "a"].groupby(["slug", "tags_exist"])._id.count().sort_values(ascending = False).reset_index(), "_id")



tmp = chk1[chk1.tp == "a"].groupby(["slug", "tags_exist"])._id.count().sort_values(ascending = False).reset_index()
tmp = chk1[chk1.tp == "b"].groupby(["slug", "tags_exist"])._id.count().sort_values(ascending = False).reset_index()



tmp["ratio"] = tmp[val_col] / tmp[val_col].max()
tmp["rnk"] = tmp.ratio.rank(method = "first")  / tmp.shape[0]
tmp = tmp.sort_values("rnk")
tmp["eq_line"] = [i / tmp.shape[0] for i in range(tmp.shape[0])]

lorenz = (tmp.rnk * tmp.shape[0] * (tmp.ratio / tmp.shape[0])).sum()
tri = ((tmp.rnk * tmp.shape[0]).max() * tmp.rnk.max()) / 2
if group_key == None :
    col_plot = geom_col(mapping = aes(x = "rnk", y = "ratio"), fill = color, alpha = 0.6)
else :
    col_plot = geom_col(mapping = aes(x = "rnk", y = "ratio", fill = group_key), alpha = 0.6)


if plot_tf == True :
    print((
        ggplot(data = tmp) +
        col_plot +
        geom_line(mapping = aes(x = "rnk", y = "eq_line"), group = 1) +
        theme_bw() +
        theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
    ))


# In[87]:


get_gini(chk1[chk1.tp == "b"].groupby("lawyer_id")._id.count().reset_index(), "_id", color = "blue")


# In[85]:


get_gini(chk1[chk1.tp == "a"].groupby("lawyer_id")._id.count().reset_index(), "_id")


# In[82]:


print("{} 분야 불평등도 = {}".format(i, get_gini(extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index(), "_id")))

print("-"*50)
print("\n"*2)


# In[20]:


(
    ggplot() +
    geom_col(data = tmp_, mapping = aes(x = "rnk_category", y = "value", fill = "variable")) +
    facet_grid("variable~.", scales = "free_y") +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)


# In[ ]:





# ### 7월 1 ~ 15일까지의 분야별 유료 상담 불평등도

# In[22]:


for idx, i in enumerate(extract_advice.groupby("adCategory_name")._id.count().sort_values(ascending = False).index[:10]) :
    
    
    # 해당 분야 광고 주
    chk1 = extract_adorders[extract_adorders.adCategory_name == i]
    
    # 해당 분야 유료 상담 변호사
    chk2 = extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index()
    
    # 해당 분야 광고 주 중 유료 상담 변호사
    chk3 = chk1[chk1.lawyer_id.isin(chk2.lawyer_id)]
    
    print("{}.{} 분야 총 광고 변호사 수 / 상담 변호사 수 / 광고 변호사 중 유료 상담 변호사 = {} / {} / {}".format(idx+1, i, chk1.lawyer_id.nunique(), chk2.lawyer_id.nunique(), chk3.lawyer_id.nunique()))
    
    print("{} 분야 총 상담 수 = {}".format(i, extract_advice[extract_advice.adCategory_name == i].shape[0]))
    
    print("{} 분야 불평등도 = {}".format(i, get_gini(extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index(), "_id")))
    
    print("-"*50)
    print("\n"*2)
    


# In[23]:


date_range = pd.date_range(datetime.date(2022, 8, 23), datetime.date(2022, 8, 30))


# In[24]:


# extract_adorders = ad_active_dates(adorders, date_range)
extract_advice = advice[(advice.adCategory_name != '기타') & (advice.createdAt.dt.date.isin(date_range.date))]


# In[25]:


tmp = pd.merge(ad_active_dates(adorders, date_range).groupby("adCategory_name").slug.nunique().sort_values(ascending = False).reset_index().rename(columns = {"slug" : "lawyer_cnt"}), advice[(advice.adCategory_name != '기타') & (advice.createdAt.dt.date.isin(date_range.date))].groupby("adCategory_name")._id.count().sort_values(ascending = False).reset_index().rename(columns = {"_id" : "advice_cnt"}), on = "adCategory_name")

tmp["ratio"] = tmp.advice_cnt / tmp.lawyer_cnt
tmp["rnk_category"] = prefix_rnk(tmp, "adCategory_name", "ratio")
tmp_ = pd.melt(tmp.drop(columns = "adCategory_name"), id_vars = "rnk_category")


# In[26]:


(
    ggplot() +
    geom_col(data = tmp_, mapping = aes(x = "rnk_category", y = "value", fill = "variable")) +
    facet_grid("variable~.", scales = "free_y") +
    theme_bw() +
    theme(figure_size = (10, 7), text = element_text(angle = 90, fontproperties = font), axis_title=element_blank())
)


# ### 7월 1 ~ 15일까지의 분야별 유료 상담 불평등도

# In[27]:


for idx, i in enumerate(extract_advice.groupby("adCategory_name")._id.count().sort_values(ascending = False).index[:10]) :
    
    
    # 해당 분야 광고 주
    chk1 = extract_adorders[extract_adorders.adCategory_name == i]
    
    # 해당 분야 유료 상담 변호사
    chk2 = extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index()
    
    # 해당 분야 광고 주 중 유료 상담 변호사
    chk3 = chk1[chk1.lawyer_id.isin(chk2.lawyer_id)]
    
    print("{}.{} 분야 총 광고 변호사 수 / 상담 변호사 수 / 광고 변호사 중 유료 상담 변호사 = {} / {} / {}".format(idx+1, i, chk1.lawyer_id.nunique(), chk2.lawyer_id.nunique(), chk3.lawyer_id.nunique()))
    
    print("{} 분야 총 상담 수 = {}".format(i, extract_advice[extract_advice.adCategory_name == i].shape[0]))
    
    print("{} 분야 불평등도 = {}".format(i, get_gini(extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index(), "_id")))
    
    print("-"*50)
    print("\n"*2)
    


# In[ ]:





# - 위 집계 기준은 15일 동안 기준이며 상담완료가 아닌 예약 완료 기준(중간에 취소 등이 포함)
# - 위 측정기간에서는 상위 11개(건수는 150건, 유료 상담 변호사는 50명 정도 이상) 적절한 불평등도를 확인할 수 있을 것으로 보이나 추후 22일 릴리즈 이후에 탐색을 하면서 적절한 기준을 잡아갈 것
# 
# - 한계 : 기존에 공유했지만 지니 계수에서 강조 태그 선택한 변호사의 편향에 따라 불평등이 심화/해소됬다고 보여질 수 있음.
#     - 상위/하위 변호사들이 미 선택 : 실제 효과와 상관없이 불평등 해소
#     - 중간 변호사 변호사들이 미 선택 : 실제 효과와 상관없이 불평등 심화
#     - 결론적으로는 세세한 부분은 실제 배포 후 데이터를 탐색하면서 효과에 대한 의미를 탐색해야할 것

# In[12]:


i = extract_advice.groupby("adCategory_name")._id.count().sort_values(ascending = False).index[0]

# 해당 분야 광고 주
chk1 = extract_adorders[extract_adorders.adCategory_name == i]

# 해당 분야 유료 상담 변호사
chk2 = extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index()

# 해당 분야 광고 주 중 유료 상담 변호사
chk3 = chk1[chk1.lawyer_id.isin(chk2.lawyer_id)]

tmp = extract_advice[extract_advice.adCategory_name == i].groupby("lawyer_id")._id.count().reset_index().assign(tp = lambda x : np.where(x._id.between(2, 22), "mid", "outer"))

print("{} 분야 총 광고 변호사 수 / 상담 변호사 수 / 광고 변호사 중 유료 상담 변호사 = {} / {} / {}".format(i, chk1.lawyer_id.nunique(), chk2.lawyer_id.nunique(), chk3.lawyer_id.nunique()))

print("{} 분야 총 상담 수 = {}".format(i, extract_advice[extract_advice.adCategory_name == i].shape[0]))

print("{} 분야 불평등도 = {}".format(i, get_gini(tmp, "_id", "tp")))


# In[13]:


get_gini(tmp[tmp.tp == "outer"], "_id", color = "blue")


# In[14]:


get_gini(tmp[tmp.tp == "mid"], "_id")

