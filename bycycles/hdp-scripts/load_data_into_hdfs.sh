#!/bin/bash

echo "Start loading data into hdfs"

hadoop distcp file:///opt/dataset/test.csv /hotinput
echo "Data is loaded"
hadoop fs -ls /hotinput
