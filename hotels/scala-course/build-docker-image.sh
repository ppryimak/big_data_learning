#!/bin/bash

APP_NAME=play-scala-server                                      # Name of the app
VERSION=0.0.1                                                   # App version
USERNAME=5storm5                                                # Docker Hub username
HDP_IP=192.168.121.4                                            # ip of your hdp-sandbox

echo "stopping container $APP_NAME"
docker stop ${APP_NAME} && docker rm ${APP_NAME}


echo "building $APP_NAME"
sbt playUpdateSecret clean dist                                       # Update app secret and build Play app

cd ./server/target/universal                                    # Go to directory where application zip is located
rm -rf ./tmp ./dist                                             # Deleted existing directories if those exist

unzip ./${APP_NAME}*.zip -d ./tmp                               # Extract application zip
mv ./tmp/${APP_NAME}* ./dist                                    # Move application files into dist folder
rm -rf ./tmp                                                    # Remove temp directory
cd ../../..                                                     # Go back to application root

echo "building docker image $USERNAME/$APP_NAME:$VERSION "
docker build -t ${USERNAME}/${APP_NAME}:${VERSION} .            # Build docker image

echo "running docker image $USERNAME/$APP_NAME:$VERSION "
docker run --name ${APP_NAME} -p 8080:8080  \
    --add-host=sandbox.hortonworks.com:192.168.121.4 \
    --add-host=sandbox-hdp.hortonworks.com:192.168.121.4 \
    -d ${USERNAME}/${APP_NAME}:${VERSION}

