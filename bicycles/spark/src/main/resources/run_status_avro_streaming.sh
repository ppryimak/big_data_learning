#!/usr/bin/env bash

spark-submit --class "com.bigdata.streaming.SparkStatusAvroStreaming" /root/work/spark/spark-uber.jar status123 sandbox-hdp.hortonworks.com:6667 30