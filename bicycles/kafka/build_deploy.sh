#!/bin/bash

WORK_DIR=/root/work
KAFKA_DIR=/kafka
USER=root
HOST=sandbox.hortonworks.com
PORT=2222

mvn clean package

ssh $USER@$HOST -p $PORT "rm -rf ${WORK_DIR}${KAFKA_DIR}/*; "

scp -P $PORT ./target/kafka-uber.jar  $USER@$HOST:$WORK_DIR$KAFKA_DIR

scp -P $PORT ./target/classes/*.sh $USER@$HOST:$WORK_DIR$KAFKA_DIR

ssh $USER@$HOST -p $PORT "chmod +x ${WORK_DIR}${KAFKA_DIR}/*.sh; "

