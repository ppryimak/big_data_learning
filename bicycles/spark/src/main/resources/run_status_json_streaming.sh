#!/usr/bin/env bash

spark-submit --class "com.bigdata.streaming.SparkStatusJsonStreaming" /root/work/spark/spark-uber.jar status3 sandbox-hdp.hortonworks.com:6667 30