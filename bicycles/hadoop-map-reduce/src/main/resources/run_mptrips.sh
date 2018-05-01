#!/usr/bin/env bash

WORK_DIR=/root/work
INPUT_FILE=/bikein/trip.csv
OUTPUT_FOLDER=/mapreduce-output/trip

echo "Cleaning $OUTPUT_FOLDER first"
bash $WORK_DIR/hdp-scripts/raw_area.sh remove $OUTPUT_FOLDER

hadoop jar hadoop-map-reduce-uber.jar com.bigdata.MPTripsDriver  $INPUT_FILE $OUTPUT_FOLDER
