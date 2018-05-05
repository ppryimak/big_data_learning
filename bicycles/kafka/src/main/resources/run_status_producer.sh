#!/usr/bin/env bash

#usage java -jar kafka-uber.jar com.bigdata.StatusProducer events topic brokers pause
java -jar kafka-uber.jar com.bigdata.StatusProducer 50 status sandbox-hdp.hortonworks.com:6667 30