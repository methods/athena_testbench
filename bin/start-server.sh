#!/bin/bash

docker-compose up &>server_logs.log &
echo "Test server started, logging to $PWD/server_logs.log"
