#!/bin/bash
while true
do
    sleep 60
    port=`ps -ef | grep postgres | grep "segment resource manager" | awk -F 'port' '{print $2}' | awk -F ',' '{print $1}'`
    su gpadmin -c "source /home/gpadmin/hawq/greenplum_path.sh && echo '\q' | psql -p $port template1"
    if [ $? == 0 ]
    then
        echo "success"
    else
        echo "failed"
        exit
    fi
done
