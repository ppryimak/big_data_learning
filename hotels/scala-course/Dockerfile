# image is based on OpenJDK image version 8-jdk-alpine
FROM openjdk:8-jdk-alpine

# install and update bash
RUN apk update && apk add bash

# copy target/universal/dist directory to /app directory in your image
COPY ./server/target/universal/dist /app

# make port 8080 visible
EXPOSE 8080

# run /app/bin/play-scala-server executable in port 8080
CMD ["/bin/bash", "/app/bin/play-scala-server", "-Dhttp.port=8080"] 
