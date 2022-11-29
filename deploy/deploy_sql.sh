#!/bin/sh
#set -x
project_id=$1
gcp_service_account_secret=$2
stg_dataset=$3

#obtain deployment start time
deployment_start_time=`date + '%s%N' | cut -c -13`
deployment_status=0

bq_deploy(){
    #replacing project ids depending on the environment variable
    sed -i "s/gcp-wow-xxx/$project_id/g" "$file_entry"
    sed -i "s/movies_stg/$stg_dataset/g" "$file_entry"

    query="$(cat $file_entry)"
    bq query --batch --use_legacy_sql=false "${query//<project_id>/$project_id}"
}

bq_check_deployment_status(){
    echo "Deployment start time: $deployment_start_time"

    #get BigQuery jobs triggered since deployment start time
    bq ls --j --min_creation_time $deployment_start_time -n 500 --project_id="$project_id" > job_status.txt

    #check for job failure looking for FAILURE keyword in the logs
    cat job_status.txt | head -2
    cat job_status.txt | grep "FAILURE"

    for job in `cat job_status.txt | grep "FAILURE"`
    do
      deployment_status=1
      echo "Set deployment status as failed"

      #exit the job with error set to 1
      exit 1
    done
}

#get service account secret into a temp .json file
gcloud beta secrets versions access --secret="$gcp_service_account_secret" latest > /root/temp_key.json

#activate the service account which will then be used to deploy the sql objects
gcloud auth activate-service-account service_account_name@gcp-wow-xxx.iam.gserviceaccount.com --key-file=/root/temp_key.json --project="$project_id"

#loop through all the views for deployment
for file_entry in $(cat views_deployment_order.txt)
do
  file_entry="./views/$file_entry"
  echo "File: $file_entry"

  #call bq_deploy
  bq_deploy
done

#monitor deployment status
bq_check_deployment_status