#!/bin/bash

WORK_DIR=/root/work
HADOOP_MR_DIR=/hadoop-map-reduce
USER=root
HOST=sandbox.hortonworks.com
PORT=2222

mvn clean package

ssh $USER@$HOST -p $PORT "rm -rf ${WORK_DIR}${HADOOP_MR_DIR}/*; "

scp -P $PORT ./target/hadoop-map-reduce-uber.jar  $USER@$HOST:$WORK_DIR$HADOOP_MR_DIR

scp -P $PORT ./target/classes/*.sh $USER@$HOST:$WORK_DIR$HADOOP_MR_DIR

ssh $USER@$HOST -p $PORT "chmod +x ${WORK_DIR}${HADOOP_MR_DIR}/*.sh; "

