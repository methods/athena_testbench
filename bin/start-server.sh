#!/bin/bash

# vars
logfile=${PWD}/server_logs.log
server_started_msg="======== SERVER STARTED ========"
max_time_to_wait_for_server_to_start=30

# start presto server and docker network
echo "Starting test server, server logs found in ${logfile}"
docker-compose up &>${logfile} &
server_pid=$!

# exit success if server_started_msg found in logs
exit_if_server_started () {
    if [ $(cat ${logfile} | grep -c "${server_started_msg}") -gt 0 ]
    then
        echo "Server running!"
        exit 0
    fi
}

# exit failure if server_pid no longer responding
exit_if_server_crashed () {
    if ! kill -0 $server_pid > /dev/null 2>&1
    then
        echo "Server failed to start!"
        exit 1
    fi
}


# loop and poll exit functions until max timeout
i=0
while [ $i -lt $max_time_to_wait_for_server_to_start ]
do
    exit_if_server_started
    exit_if_server_crashed
    sleep 1
    let i=i+1
done

echo "Could not verify server had started within ${max_time_to_wait_for_server_to_start} seconds"
echo "Check logs at ${logfile} for error messages"
