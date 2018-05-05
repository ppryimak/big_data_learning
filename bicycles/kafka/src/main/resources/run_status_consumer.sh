#!/usr/bin/env bash

#usage java -jar kafka-uber.jar com.bigdata.StatusProducer brokers groupId topic
java -jar kafka-uber.jar com.bigdata.StatusConsumer sandbox-hdp.hortonworks.com:6667 group.id status