#!/bin/bash

WORK_DIR=/root/work
SPARK_DIR=/spark
USER=root
HOST=sandbox.hortonworks.com
PORT=2222

mvn clean package

ssh $USER@$HOST -p $PORT "rm -rf ${WORK_DIR}${SPARK_DIR}/*; "

scp -P $PORT ./target/spark-uber.jar  $USER@$HOST:$WORK_DIR$SPARK_DIR

scp -P $PORT ./target/classes/*.sh $USER@$HOST:$WORK_DIR$SPARK_DIR

ssh $USER@$HOST -p $PORT "chmod +x ${WORK_DIR}${SPARK_DIR}/*.sh; "
