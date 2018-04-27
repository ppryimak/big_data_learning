#!/bin/bash

mvn package

scp -P 2222 ./target/scala-spark-jobs-*.jar root@sandbox.hortonworks.com:/root/work/spark
