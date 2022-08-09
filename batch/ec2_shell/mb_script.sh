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

# log
LOG_DIR="/home/ubuntu/data-analysis/shell/log"
log_file="${LOG_DIR}/`basename $0`.log.${v_target_dt}"
all_log="${LOG_DIR}/`basename $0`.log.${v_target_dt}*"

#:<<'END'
if [ -f "$log_file" ]
then
    #echo "ls -al $all_log | wc -l"
    count=`ls -al $all_log | wc -l`
    #echo $count
    if [ $count -ne 0 ]
    then
        mv ${log_file} ${log_file}_${count}
    fi
fi

# init log file
cat /dev/null > ${log_file}

#exec &>> ${log_file}
echo "`basename $0`: START TIME : " `date` >> ${log_file}
#END

#set variable
#passwd=$(<"/home/ubuntu/data-analysis/shell/.mbdbpasswd")
passwd=`cat /home/ubuntu/data-analysis/shell/.mbdbpasswd`
project_id='lawtalk-bigquery'
data_set='for_shareholder'
table_name='f_mb_monica_lawyer'

#service db connecting & get lawyer_cnt
mongo -u dataTeam5 -p $passwd --authenticationDatabase admin --quiet 3.37.119.165:27017/lawyers < /home/ubuntu/data-analysis/shell/script.js > /home/ubuntu/data-analysis/shell/data/result.out >> ${log_file}

value="${v_target_dt}, `cat /home/ubuntu/data-analysis/shell/data/result.out`"

# add batch_date & create csv file
echo ${value} > /home/ubuntu/data-analysis/shell/data/f_mb_monica_lawyer_${v_target_dt}.csv

# bq delete query
bq --project_id ${project_id} query --use_legacy_sql=false \
        "delete from \`${data_set}.${table_name}\` where b_date=date('${v_target_dt}')" >> ${log_file}

# data loading to table
bq --project_id ${project_id} load --format=csv ${data_set}.${table_name} /home/ubuntu/data-analysis/shell/data/f_mb_monica_lawyer_${v_target_dt}.csv >> ${log_file}


#:<<'END'
result=`grep -ri error ${log_file}`

if [ ${#result} -gt 0 ]
then
    curl -X POST --data-urlencode "payload={\"username\": \"[ERROR] $v_target_dt all_lawyer mongodb etl\", \"text\": \" $result \", \"icon_emoji\": \":imp:\"}" https://hooks.slack.com/services/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
fi
#END


#deleted old file
find /home/ubuntu/data-analysis/shell/data/f_mb_monica_lawyer* -mtime +30 -exec rm {} \;
find /home/ubuntu/data-analysis/shell/log/* -mtime +14 -exec rm {} \;

echo "`basename $0`: END TIME : " `date`
exit 0
