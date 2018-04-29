#!/bin/bash

WORK_DIR=/root/work
HDFS_DIR=/hdfs
USER=root
HOST=sandbox.hortonworks.com
PORT=2222

mvn package

scp -P $PORT ./target/hdfs-inotify-uber.jar  $USER@$HOST:$WORK_DIR/$HDFS_DIR

scp -P $PORT ./target/classes/*.sh $USER@$HOST:$WORK_DIR/$HDFS_DIR

ssh $USER@$HOST -p $PORT "chmod +x ${WORK_DIR}${HDFS_DIR}/*.sh; "

