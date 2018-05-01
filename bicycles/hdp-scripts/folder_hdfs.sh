#!/bin/bash

ACTION=$1
FOLDER=$2

if [ -z "$2" ]
  then
    echo "No argument supplied for folder - exit"
    exit
fi

if [ "$ACTION" = "create" ]; then
	echo "Creating folder $FOLDER"
	hadoop fs -mkdir ${FOLDER}
	echo Folder is created
elif [ "$ACTION" = "remove" ]; then
      	echo "Removing folder $FOLDER"
	hadoop fs -rm -r $FOLDER
	echo Folder is removed

else
  echo "Unsupported oparation"
fi

hadoop fs -ls /
