#!/usr/bin/env python
# coding: utf-8

'''
# gcloud composer environments run cj-airflow	 \
#    --location us-central1	 \
#    backfill

# gcloud composer environments run cj-airflow	 \
#    --location us-central1	\
#    clear -- keyword_review4 \
#    --reset_dagruns -s 2022-04-27 -e 2022-04-28

# airflow 환경 접속하는 gcloud
gcloud composer environments describe planning-data-2 \
    --location us-central1

gcloud container clusters get-credentials projects/lawtalk-bigquery/locations/us-central1/clusters/us-central1-planning-data-2-6f03343b-gke \
 --zone us-central1

kubectl -n composer-2-0-10-airflow-2-2-3-d343e2c4 \
exec -it airflow-worker-f4pxl -c airflow-worker -- /bin/bash

# pip Requirements
gcloud composer environments update cj-airflow \
--update-pypi-packages-from-file requirements/requirements.txt \
--location us-central1


# airflow 환경에 data import

gcloud composer environments storage data import --source=/99_credential --environment=cj-airflow --location=us-central1 --destination=./


# 데이터 조회 mongoDB query

db.reviewclouds.find({
    'createdAt' :  { 
        $gt:ISODate('2022-05-09 00:00:00.000Z') }
    
    }
)


# 후기 분석형 선택 변호사 mongoDB 쿼리

db.getCollection('lawyers').find({
    visibleCloudType: 'review-research',
    'flag.activation': true,
    'flag.holdWithdrawal': false,
    'flag.advice': true,
    'writeRate.percentage': { $gte: 100 },
})

'''

import os
import sys
import warnings

'''
dag 별로 파이썬 가상환경이 아니라, composer 전체 환경에 파이썬이 설치되어있다보니
위에 방법처럼 설치하는 방법도 있지만. 왜인지 pip install이 되지 않아서 아래처럼 실행할 때 설치를 해주고 있습니다.
'''
os.system("sudo python3 -m pip install -U gspread")
os.system("sudo python3 -m pip install -U konlpy")
# os.system("sudo python3 -m pip install -U pip")

import argparse

import json
import re
from glob import glob
import random
import time
import datetime
from datetime import timedelta
from collections import Counter
import requests

import numpy as np
import pandas as pd

from google.cloud import bigquery

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

import pendulum

from google.cloud import storage

import gspread
from oauth2client.service_account import ServiceAccountCredentials



# from pykospacing import Spacing
# from hanspell import spell_checker
from konlpy.tag import Okt

KST = pendulum.timezone("Asia/Seoul")


def chk_executedate(execution_date : datetime.datetime) :
    '''
    실행 날짜를 확인하는 함수
    execution_date : dag의 실행일자를 인자로 받음 -- datetime.datetime
    Return : None
    '''
    print(execution_date.strftime("%Y-%m-%d %H:%M"))
    
    return


def print_files_in_dir(root_dir : str, prefix : str) :
    '''
    파일 tree를 확인하는 함수(처음에 composer 디렉토리 구조를 이해하지 못해서 생성/필요 없음.) 
    Return : None
    '''
    files = os.listdir(root_dir)
    
    if ".ipynb_checkpoints" in files : files.remove(".ipynb_checkpoints")
    if "logs" in files : files.remove("logs")
    if "log" in files : files.remove("log")
        
    for file in files :
        path = os.path.join(root_dir, file)
        print(prefix + path)
        if os.path.isdir(path):
            print_files_in_dir(root_dir = path, prefix = prefix + "    ")
    return


def chk_gspread() :
    '''
    google spread sheet에 권한 및 수정 테스트를 위한 함수(사용 안함)
    Return : None
    '''
    
    url = "https://docs.google.com/spreadsheets/d/1YPE56LBIodmUJstnEWVlGjVe-oj5x3nJgFkpZ1KjxhI/edit?usp=sharing"
    credential_path = 'gcs/data/99_credential/lawtalk-bigquery-2bfd97cf0729.json'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    
    sheet_content = sheet.get_all_values()
    r = sheet.append_rows([["test1", "테스트1"], ["test2", "테스트2"]])
    
    print("spread sheet update!")
    
    
    return r



def bigquery_to_pandas(query_string) :
    '''
    bigquery에서 판다스로 옮기는 함수
    query_string : bigquery query -- str
    Return : bigquery 결과 데이터프레임 -- pd.DataFrame
    '''

    credential_path = 'gcs/data/99_credential/lawtalk-bigquery-2bfd97cf0729.json'
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



def gs_append(df, url = "https://docs.google.com/spreadsheets/d/1P_sFwDbiHL-yszUyxlcJ-DjFjUtFS3vR9S7zWCfPMyk/edit#gid=0", credential_path = 'gcs/data/99_credential/lawtalk-bigquery-2bfd97cf0729.json', scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']) : 
    '''
    google spreadsheet에 추가된 단어를 추가하는 함수
    df : 업로드할 gcs bucket name -- pd.DataFrame
    source_file_name : 올려야할 composer 내에서 생성한 파일 이름 -- str
    destination_blob_name : 업로드할 Gcs bucket에 들어갈 파일 이름 -- str
    Return : None
    '''
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    
    sheet_content = sheet.get_all_values()
    
    df_ = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    df = df[~df.words.isin(df_.words)]
    
    if sheet_content == [] :
        r = sheet.append_row(df.columns.to_list())
    # r = sheet.append_rows(df.values.tolist())
    
    return df_


def read_dict() :
    '''
    후기뷰 사전 읽어오는 함수
    확인용 url = https://docs.google.com/spreadsheets/d/1P_sFwDbiHL-yszUyxlcJ-DjFjUtFS3vR9S7zWCfPMyk/edit#gid=0
    '''
    url = "https://docs.google.com/spreadsheets/d/1P_sFwDbiHL-yszUyxlcJ-DjFjUtFS3vR9S7zWCfPMyk/edit#gid=0"
    credential_path = 'gcs/data/99_credential/lawtalk-bigquery-2bfd97cf0729.json'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_path, scope)
    
    gc = gspread.authorize(credentials)
    doc = gc.open_by_url(url)
    
    sheet_name = re.search(string = str(doc.get_worksheet(0)), pattern = r"\'(.*)\'").group(0)[1:-1]
    sheet = doc.worksheet(sheet_name)
    sheet_content = sheet.get_all_values()
    
    df_ = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    return df_


def tokenize_okt(text, all_body = False):
    '''
    text를 받아 형태소 분석을 하는 함수
    text : 형태소 분석을 할 문장 -- str
    all_body : 형태소가 붙은 사전을 return 할 것인가에 대한 boolean -- boolean
    Return : 형태소 분석 결과 및 사전
    '''
    okt_pos = Okt().pos(text, norm=True, stem=True)
    okt_filtering = [x for x,y in okt_pos if y in ["Verb", 'Adverb', "Adjective", "Noun"]]
    if all_body == True :
        word_and_pos = pd.DataFrame(okt_pos, columns = ["word", "pos"]).drop_duplicates()
        word_and_pos = word_and_pos[word_and_pos.pos.isin(["Verb", 'Adverb', "Adjective", "Noun"])]
        return(okt_filtering, word_and_pos)
    
    return(okt_filtering)


def extract_n_gram(x, word, n = 5) :
    '''
    기준 단어(현재 '변호사', '설명', '상담')에 대해서 n-gram을 실시함.
    n : n-gram에서 주변 몇개의 단어를 까져올 것인지 default = 5 -- int
    '''
    return re.findall(pattern = "[^\s]*? " * n + f"[^\s]*{word}[^\s]*?" + " [^\s]*?" * (n + 1), string = x)


def upload_gcs(bucket_name, source_file_name, destination_blob_name) :
    '''
    gcs buckect에 파일을 업로드하는 함수
    bucket_name : 업로드할 gcs bucket name -- str
    source_file_name : 올려야할 composer 내에서 생성한 파일 이름 -- str
    destination_blob_name : 업로드할 Gcs bucket에 들어갈 파일 이름 -- str
    Return : None
    '''
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    
    print(f"upload {bucket_name} {source_file_name} to {destination_blob_name}")
    
    return


def send_to_slack(text) :
    '''
    slack bot에 진행 메세지 보내기
    text : 메세지 내용 -- str
    '''
    url = "https://hooks.slack.com/services/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"
    requests.post(url, headers={'Content-type': 'application/json'}, json = {"text" : text})
    
    return


def start_message(execution_date : datetime.datetime) :
    '''
    시작 알람 메세지 슬랙으로 보내는 task
    execute_date : dag 실행일자 -- datetime.datetime
    '''
    execution_date = execution_date + datetime.timedelta(days = 16)
    print(f"execute date = {execution_date}")
    
    send_to_slack(f'{execution_date.strftime("%Y-%m-%d %H:%M")} 분석형 후기뷰 프로세스 시작')
    
    return


def make_dataset(execution_date : datetime.datetime) :
    '''
    dag 실행일자 기준 후기가 50개 이상인 변호사들과 최근 50개의 후기 데이터를 추출
    execution_date : dag 실행일자 -- datetime.datetime
    '''
    execution_date = execution_date + datetime.timedelta(days = 16)
    print(f"execute date = {execution_date}")
    
    send_to_slack(f"{execution_date.strftime('%Y-%m-%d %H:%M')} 변호사 분석형 후기뷰 데이터셋 프로세스 시작")
    
    y = str(execution_date.year).zfill(4)
    m = str(execution_date.month).zfill(2)
    d = str(execution_date.day).zfill(2)

    # 기준단어는 아래의 3개의 단어
    standwords_list = ["상담", "변호사", "설명"]
    # 사용하는 품사는 명사, 동사, 부사, 형용사
    standpos_list = ["Noun", "Verb", "Adverb", "Adjective"]
    
    sql = '''
    WITH t1 AS(
    SELECT 
        lawyer
        , createdAt
        , updatedAt
        , review
        , adCategory
        , JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d\s,]+\)|True|False|\[.*\]', "''"), '$.body') AS review_body
        FROM `lawtalk-bigquery.raw.advice` 
        WHERE 1 = 1
            AND status = 'complete'
            AND JSON_EXTRACT(REGEXP_REPLACE(review, r'datetime.datetime\([\d\s,]+\)|True|False|\[.*\]', "''"), '$.body') IS NOT NULL
            AND EXTRACT(DATE FROM createdAt) <= DATE('{}-{}-{}')
            -- AND EXTRACT(DATE FROM createdAt) <= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
            -- AND EXTRACT(DATE FROM updatedAt) >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 MONTH)
    )
    , t2 AS (
    SELECT 
        t1.lawyer
        , t1.createdAt
        , t1.updatedAt
        , t1.review
        , t1.adCategory
        , REGEXP_REPLACE(review_body, r"''|\\"", '') AS review_body
        , RANK() OVER(PARTITION BY lawyer ORDER BY createdAt DESC) AS advice_rnk
        FROM t1
        WHERE REGEXP_REPLACE(review_body, r"''|\\"", '') <> ''
    )
    , select_lawyer AS (
        SELECT
            DISTINCT lawyer
        FROM t2
        WHERE advice_rnk >= 50

    )
    , t3 AS (
    SELECT
        t2.*
    FROM t2
    INNER JOIN select_lawyer
    USING (lawyer)
    )
    , t4 AS (
    SELECT
        t3.*
        , J1.name
        , J1.slug
    FROM t3
    LEFT JOIN
    `lawtalk-bigquery.raw.lawyers` AS J1
    ON (t3.lawyer = J1._id)
    )
    SELECT 
        * 
    FROM t4
    WHERE advice_rnk <= 50
    ORDER BY lawyer, createdAt DESC
    ;
    '''.format(y, m, d)
    
    # read data
    advice = bigquery_to_pandas(sql)

    # 각 내용에 필요없는 특수문자를 제거합니다.
    advice["review_body"] = advice["review_body"].astype(str).apply(lambda x: re.sub(r"\\n|\\t|~|\:|\)|\(|&|!| ", repl = " ", string = x))
    advice["review_body"] = advice["review_body"].astype(str).apply(lambda x: re.sub(r" {2,}|\\s{2,}", repl = " ", string = x))
    advice.review_body = advice.apply(lambda x : x.review_body.replace(x["name"], ""), axis = 1)

    # task 별로 데이터를 전달하기 어렵기 때문에 gcs에 저장하여 연결
    os.makedirs("gcs/data/extract_data", exist_ok = True)
    advice.to_csv(f'gcs/data/extract_data/{y}{m}{d}.csv', index = False)
    
    print(f'save file at : gcs/data/extract_data/{y}{m}{d}.csv')
    
    send_to_slack(f"추출 데이터셋 처리 완성")
    
    return




def make_lawyer_data(execution_date : datetime.datetime) :
    '''
    
    '''
    execution_date = execution_date + datetime.timedelta(days = 16)
    print(f"execute date = {execution_date}")
    
    y = str(execution_date.year).zfill(4)
    m = str(execution_date.month).zfill(2)
    d = str(execution_date.day).zfill(2)
    
    standwords_list = ["상담", "변호사", "설명"]
    standpos_list = ["Noun", "Verb", "Adverb", "Adjective"]
    
    advice = pd.read_csv(f'gcs/data/extract_data/{y}{m}{d}.csv')
    
    send_to_slack(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} 변호사 분석형 후기뷰 데이터처리 진행 시작")
    
    stand_1 = " ".join(advice.review_body.apply(lambda x : " ".join(extract_n_gram(x, standwords_list[0], 5))).values)
    stand_2 = " ".join(advice.review_body.apply(lambda x : " ".join(extract_n_gram(x, standwords_list[1], 5))).values)
    stand_3 = " ".join(advice.review_body.apply(lambda x : " ".join(extract_n_gram(x, standwords_list[2], 5))).values)
    
    word_count, pos_list = tokenize_okt(stand_1 + stand_2 + stand_3, all_body = True)
    pos_list.drop_duplicates(subset = "word", inplace = True)
    total_counter = pd.merge(pd.Series(Counter(word_count)).reset_index().rename(columns = {"index" : "word", 0 : "cnt"}), pos_list, on = "word", how = "outer")
    
    word_count1 = advice.groupby("lawyer").review_body.apply(lambda x : " ".join(extract_n_gram(" ".join(x), standwords_list[0], 5))).reset_index()
    word_count2 = advice.groupby("lawyer").review_body.apply(lambda x : " ".join(extract_n_gram(" ".join(x), standwords_list[1], 5))).reset_index()
    word_count3 = advice.groupby("lawyer").review_body.apply(lambda x : " ".join(extract_n_gram(" ".join(x), standwords_list[2], 5))).reset_index()
    
    word_count1 = pd.DataFrame([*word_count1.apply(lambda x : Counter(tokenize_okt(x.review_body)), axis = 1)], word_count1.lawyer).stack().rename_axis(["lawyer",'word']).reset_index(1, name='cnt').reset_index().assign(stand = standwords_list[0])
    word_count2 = pd.DataFrame([*word_count2.apply(lambda x : Counter(tokenize_okt(x.review_body)), axis = 1)], word_count2.lawyer).stack().rename_axis(["lawyer",'word']).reset_index(1, name='cnt').reset_index().assign(stand = standwords_list[1])
    word_count3 = pd.DataFrame([*word_count3.apply(lambda x : Counter(tokenize_okt(x.review_body)), axis = 1)], word_count3.lawyer).stack().rename_axis(["lawyer",'word']).reset_index(1, name='cnt').reset_index().assign(stand = standwords_list[2])
    
    word_count = pd.concat([word_count1, word_count2, word_count3])
    word_count = pd.merge(word_count, pos_list, on = "word")
    
    word_count = pd.merge(word_count, advice.groupby("lawyer").agg(body_count = ("review_body", "count")).reset_index(), on = "lawyer")

    dict_update = word_count.drop_duplicates(subset = "word")

    dict_update = dict_update.assign(words = lambda x : x.word, final_words = "", type = "", update_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M"), updateLawyer = "base", cut_date = "-".join([str(y), m, d]))[["words", "final_words", "type", "pos", "update_time", "updateLawyer", "cut_date"]].drop_duplicates(subset = "words")

    os.makedirs("gcs/data/share_dict", exist_ok = True)

    dict_path = glob("gcs/data/share_dict/*.csv")[np.argmax([datetime.datetime.strptime(re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}").group(0), "%Y-%m-%d") for x in glob("gcs/data/share_dict/*.csv") if re.search(string = x, pattern = "\d{4}-\d{2}-\d{2}") != None])]

    dict_downdate = pd.read_csv(dict_path)

    dict_update = pd.concat([dict_downdate, dict_update]).drop_duplicates("words")
    dict_update.to_csv(f"gcs/data/share_dict/{y}-{m}-{d}.csv", index = False)

    gs_dict = gs_append(dict_update[dict_update.cut_date == f"{y}-{m}-{d}"])
    # gs_dict = pd.DataFrame(sheet_content[1:], columns = sheet_content[0])
    
    send_to_slack(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} 전체 데이터셋 구성") 
    
    os.makedirs(f"gcs/data/lawyer_{y}{m}{d}", exist_ok = True)
    
    gs_dict = read_dict()
    gs_dict = gs_dict[gs_dict["type"] != "stopwords"]
    gs_dict = gs_dict[gs_dict["type"] != "questionable"]
    gs_dict = gs_dict[gs_dict["type"] != ""]
    gs_dict.loc[gs_dict["final_words"] == "", "final_words"] = gs_dict.loc[gs_dict["final_words"] == "", "words"].values

    for idx, t in enumerate(advice.lawyer.unique()) : 

        word_count.reset_index(drop = True, inplace = True)
        word_count.loc[word_count["lawyer"] == t, "target_lawyer"] = 1
        word_count.loc[word_count["lawyer"] != t, "target_lawyer"] = 0

        print("-"*50)
        print(f"현재 변호사 _id : {advice[advice.lawyer == t].lawyer.unique()[0]}")
        print(f"현재 변호사 성함 : {advice[advice.lawyer == t].name.unique()[0]}")

        # target 변호사와 아닌 변호사를 구분하여 리뷰 갯수와 모든 후기를 연결
        review_group = word_count.groupby(["target_lawyer", "stand", "word"]).agg(body = ("cnt", sum)).reset_index()

        result = review_group.pivot(index = ["stand", "word"], columns = "target_lawyer", values = ["body"]).reset_index()
        result.columns = ["stand", "word", "nontarget", "target"]
        
        
        result["target_ratio"] = result["target"] / 50
        result["nontarget_ratio"] = result["nontarget"] / (50 * advice.lawyer.nunique() - 1)

        result["ratio_diff"] = result["target_ratio"] - result["nontarget_ratio"]

        result = pd.merge(result, pos_list, on = "word", how = "left")
        result = result[result.pos.isin(standpos_list)]


        tmp = result.copy()

        tmp = pd.merge(tmp, gs_dict.assign(word = lambda x : x.words)[["word", "final_words", "type"]], on = "word", how = "inner")
        tmp.loc[tmp.final_words.isna() , "final_words"] = tmp.loc[tmp.final_words.isna() , "word"]
        tmp = tmp[tmp.pos.isin(standpos_list)]
        tmp = tmp[~tmp.final_words.isin(standwords_list)]

        gtmp = tmp.groupby("final_words").ratio_diff.sum().reset_index()
        gtmp["ranks"] = gtmp.ratio_diff.rank(method = "first", ascending = False)
        gtmp["rank_word"] = gtmp["ranks"].astype(int).astype(str).str.zfill(4) + "_" + gtmp.final_words

        gtmp = gtmp.sort_values("ranks")
        gtmp = gtmp.head(10)

        if gtmp.ratio_diff.min() <= 0 :
            send_to_slack(f"{advice[advice.target_lawyer == 1].name.unique()[0]} 변호사님은 음수값을 보정합니다.")
            gtmp.ratio_diff = (-1 * gtmp.ratio_diff.min()) + 0.0005

        gtmp.to_csv(f"gcs/data/lawyer_{y}{m}{d}/{t}.csv", index = False)

        send_to_slack(f"{advice[advice.lawyer == t].name.unique()[0]} 변호사 데이터 구성 완료, 진행사항 : {idx + 1} / {advice.lawyer.nunique()}")
    
    send_to_slack("모든 변호사 데이터셋이 만들어졌습니다.")
        
    return

def import_gcs(execution_date : datetime.datetime) :
    '''
    composer는 현재 Task 실행시마다 ip가 유동적으로 바뀌므로, cloud function(AWS lambda 와 비슷)을 사용해 for_review_update BUCKET에 올리면
    trigger를 통해서 MongoDB API(이수연님 개발)에 request를 보내어 운영db에 적재
    '''
    
    execution_date = execution_date + datetime.timedelta(days = 16)
    
    send_to_slack(f"{execution_date.strftime('%Y-%m-%d %H:%M')} for_review_update BUCKET에 변호사 데이터를 옮깁니다.")
    
    print(f"execute date = {execution_date}")

    
    y = str(execution_date.year).zfill(4)
    m = str(execution_date.month).zfill(2)
    d = str(execution_date.day).zfill(2)
    
    lawyer_data = glob(f"gcs/data/lawyer_{y}{m}{d}/*")
    # lawyer_data = glob(f"lawyer_{y}{m}{d}/*")
    
    for l in lawyer_data :
        time.sleep(2)
        upload_gcs("for_review_update", l, os.path.join(*l.split("/")[2:]))
        # upload_gcs("for_review_update", l, l)
    
    return



with DAG(

    dag_id="keyword_review_new",
    description ="keyword with laywer review",
    start_date = datetime.datetime(2022, 6, 28, tzinfo = KST),
    # 매일 15일 혹은 말일에 돌도록 되어있음.
    schedule_interval = '0 0 15,L * *',
    # schedule_interval = '0 0 11 13,26 * ? *',
    tags=["cj_kim","keyword_review"],
    default_args={
        "owner": "cj_kim", 
        "retries": 100,
        "retry_delay": timedelta(minutes=1),
    }
) as dag:

    start = DummyOperator(
        task_id="start"
    )
    
    
    # [START howto_operator_bash]
    # ip_chk = BashOperator(
    #     task_id='ip_check',
    #     bash_command="curl ifconfig.io",
    #     dag=dag,
    # )
    
    # pip_update = BashOperator(
    #     task_id='pip_update',
    #     bash_command="sudo python3 -m pip install -U pip",
    #     dag=dag,
    # )
    
    # print_dir = PythonOperator(task_id='print_files_in_dir',
    #                     provide_context=True,
    #                     python_callable=print_files_in_dir,
    #                     # templates_dict={execute_date : {{ ds }}}, 
    #                     op_kwargs = {"root_dir": "./", "prefix" : ""}, 
    #                     dag=dag)
    
    # execute_date_test = PythonOperator(task_id='execute_date_test',
    #                     provide_context=True,
    #                     python_callable=chk_executedate,
    #                     templates_dict={"execute_date" : "{{ ds }}"}, 
    #                     # op_kwargs = {"root_dir": "./", "prefix" : ""}, 
    #                     dag=dag)

    # bigquery_test = PythonOperator(task_id='bigquery_chk',
    #                     provide_context=True,
    #                     python_callable=chk_bigquery,
    #                     dag=dag)
    
    # gspread_test = PythonOperator(task_id='gspread_test',
    #                 provide_context=True,
    #                 python_callable=chk_gspread,
    #                 dag=dag)
    
    # 위의 test task들은 사실 무의미하고 아래의 실제 테스크만 돌아가면 됨.
    start_message = PythonOperator(task_id='start_message',
                        provide_context=True,
                        python_callable=start_message,
                        templates_dict={"execute_date" : "{{ ds }}"}, 
                        # op_kwargs = {"root_dir": "./", "prefix" : ""}, 
                        dag=dag)
    
    
    make_dataset = PythonOperator(task_id='make_dataset',
                        provide_context=True,
                        python_callable=make_dataset,
                        templates_dict={"execute_date" : "{{ ds }}"}, 
                        # op_kwargs = {"root_dir": "./", "prefix" : ""}, 
                        dag=dag)
    
    make_lawyer_data = PythonOperator(task_id='make_lawyer_data',
                        provide_context=True,
                        python_callable=make_lawyer_data,
                        templates_dict={"execute_date" : "{{ ds }}"}, 
                        # op_kwargs = {"root_dir": "./", "prefix" : ""}, 
                        dag=dag)
    
    import_gcs = PythonOperator(task_id='import_gcs',
                        provide_context=True,
                        python_callable=import_gcs,
                        templates_dict={"execute_date" : "{{ ds }}"}, 
                        # op_kwargs = {"root_dir": "./", "prefix" : ""}, 
                        dag=dag)

    
    # 시작 -> 시작메세지 출력 -> 타겟 변호사 데이터셋 로드 -> n-gram을 통하여 데이터셋 구성 -> mongoDB에 넣을 수 있도록 cloud function과 연결되어 있는 gcs에 업로드
    start >> start_message >> make_dataset >> make_lawyer_data >> import_gcs
    
    # start >> ip_chk >> execute_date_test >> start_message >> make_dataset >> make_lawyer_data >> import_gcs
    # start >> execute_date_test >> start_message >> make_dataset >> make_lawyer_data >> import_gcs
    # start >> execute_date_test
