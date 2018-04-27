#!/bin/bash

HDFS_FOLDER='/bikein'
files=(/opt/data/*.csv)

 
# find total number of files in an array
echo "Total files in array : ${#files[*]}"
total=${#files[*]}

# Use for loop iterate through an array
# $f stores current value 
for f in "${files[@]}"
do
	echo "Copying $f "
	hadoop distcp file://$f $HDFS_FOLDER
	echo "End "
done

hadoop fs -ls $HDFS_FOLDER
