#!/bin/bash

mvn package

scp -P 2222 ./target/hdfs-inotify-uber.jar root@sandbox.hortonworks.com:/root/work/hdfs

scp -P 2222 ./target/classes/created.sh root@sandbox.hortonworks.com:/root/work/hdfs
