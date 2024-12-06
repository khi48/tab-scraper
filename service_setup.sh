#!/bin/bash
SERVICE_NAME="docker-compose-mongodb.service"

# not sure if this will fail on fresh installation
sudo systemctl stop $SERVICE_NAME
sudo systemctl disable $SERVICE_NAME
sudo rm /etc/systemd/system/$SERVICE_NAME

sudo cp ./$SERVICE_NAME /etc/systemd/system/
sudo sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl start $SERVICE_NAME 
sudo systemctl status $SERVICE_NAME