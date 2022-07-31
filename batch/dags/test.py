## test insert from lja
## fjksalfjlskajfldsa
import os
import sys
import warnings

os.system("sudo python3 -m pip install gspread")
os.system("sudo python3 -m pip install konlpy")

import json
import re
from glob import glob
import random
import time
import datetime
from datetime import timedelta
from collections import Counter
import requests

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


with DAG(
    dag_id="test",
    description ="test",
    start_date = datetime.datetime(2022, 5, 16, tzinfo = KST),
    schedule_interval = '0 0 15,L * *',
    # schedule_interval = '0 0 11 13,26 * ? *',
    tags=["cj_kim","keyword_review"],
    default_args={
        "owner": "cj_kim",
        "retries": 30,
        "retry_delay": timedelta(minutes=1),
    }
) as dag:

    start = DummyOperator(
        task_id="start"
    )


    # [START howto_operator_bash]
    ip_chk = BashOperator(
        task_id='ip_check',
        bash_command="curl ifconfig.io",
        dag=dag,
    )
