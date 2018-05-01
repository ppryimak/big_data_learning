#!/bin/bash

PATH_TO_FILE=$1
DATE=$2
WORK_DIR=/root/work

echo "Starting processing created file by path $PATH_TO_FILE on $DATE"

echo "Cleaning rawarea first"
bash $WORK_DIR/hdp-scripts/folder_hdfs.sh remove /rawarea/status

spark-submit --class "com.bigdata.example.CsvToParquet" $WORK_DIR/spark/scala-spark-jobs-1.0.0.jar > 2>&1 | tee log.txt
