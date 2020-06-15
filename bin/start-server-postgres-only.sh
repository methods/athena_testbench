#!/bin/bash

# vars
logfile=${PWD}/postgres_server_logs.log
server_started_msg=" database system is ready to accept connections"
max_time_to_wait_for_server_to_start=30

# start postgres container
echo "Starting test postgres server, server logs found in ${logfile}"
docker-compose up postgresql &>${logfile} &
server_pid=$!

# exit success if server_started_msg found TWICE in logs
exit_if_server_started () {
    if [ $(cat ${logfile} | grep -c "${server_started_msg}") -eq 2 ]
    then
        echo "Postgres server running!"
        exit 0
    fi
}

# exit failure if server_pid no longer responding
exit_if_server_crashed () {
    if ! kill -0 $server_pid > /dev/null 2>&1
    then
        echo "Postgres server failed to start!"
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

echo "Could not verify postgres server had started within ${max_time_to_wait_for_server_to_start} seconds"
echo "Check logs at ${logfile} for error messages"
