#!/bin/bash

mvn package

scp -P 2222 ./target/spark-*.jar root@sandbox.hortonworks.com:/root/work/spark
