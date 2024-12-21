#!/bin/bash
SERVICE_NAME="schedule_trigger"

docker stop $SERVICE_NAME
docker rm $SERVICE_NAME
docker build -t $SERVICE_NAME -f trigger_dockerfile .
docker run -d --name=$SERVICE_NAME $SERVICE_NAME
