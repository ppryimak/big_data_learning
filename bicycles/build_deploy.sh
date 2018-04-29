#!/usr/bin/env bash

WORK_DIR=/root/work
HDFS_SCRIPTS_DIR=/hdp-scripts
USER=root
HOST=sandbox.hortonworks.com
PORT=2222

ssh $USER@$HOST -p $PORT "rm -rf ${WORK_DIR}${HDFS_SCRIPTS_DIR}; mkdir ${WORK_DIR}${HDFS_SCRIPTS_DIR};"

scp -P $PORT .$HDFS_SCRIPTS_DIR/*.sh $USER@$HOST:$WORK_DIR/$HDFS_SCRIPTS_DIR

ssh $USER@$HOST -p $PORT "chmod +x ${WORK_DIR}${HDFS_SCRIPTS_DIR}/*; "
