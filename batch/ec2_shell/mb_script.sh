#!/bin/bash

# for crontab
. ~/.profile
export PATH=/home/ubuntu/google-cloud-sdk/bin:$PATH

if [ $# -ne 1 ]
then
        v_target_dt=`date +"%Y-%m-%d"`
else
        v_target_dt=$1
fi

#set variable
#passwd=$(<"/home/ubuntu/data-analysis/shell/.mbdbpasswd")
passwd=`cat /home/ubuntu/data-analysis/shell/.mbdbpasswd`
project_id='lawtalk-bigquery'
data_set='for_shareholder'
table_name='f_mb_monica_lawyer'

#service db connecting & get lawyer_cnt
mongo -u dataTeam5 -p $passwd --authenticationDatabase admin --quiet 3.37.119.165:27017/lawyers < /home/ubuntu/data-analysis/shell/script.js > /home/ubuntu/data-analysis/shell/data/result.out

value="${v_target_dt}, `cat /home/ubuntu/data-analysis/shell/data/result.out`"

# add batch_date & create csv file
echo ${value} > /home/ubuntu/data-analysis/shell/data/f_mb_monica_lawyer_${v_target_dt}.csv

# bq delete query
bq --project_id ${project_id} query --use_legacy_sql=false \
        "delete from \`${data_set}.${table_name}\` where b_date=date('${v_target_dt}')"

# data loading to table
bq --project_id ${project_id} load --format=csv ${data_set}.${table_name} /home/ubuntu/data-analysis/shell/data/f_mb_monica_lawyer_${v_target_dt}.csv

#deleted old file
find /home/ubuntu/data-analysis/shell/data/f_mb_monica_lawyer* -mtime +30 -exec rm {} \;

exit 0
