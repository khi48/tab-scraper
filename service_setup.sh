#!/bin/bash
SERVICE_NAME="docker-compose-mongodb.service"

sudo cp ./$SERVICE_NAME /etc/systemd/system/
sudo sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl start $SERVICE_NAME 
sudo systemctl status $SERVICE_NAME 