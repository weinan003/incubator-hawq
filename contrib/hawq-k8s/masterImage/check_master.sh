#!/bin/bash
while true
do
    sleep 60
    su gpadmin -c "source /home/gpadmin/hawq/greenplum_path.sh && echo '\q' | psql postgres"
    if [ $? == 0 ]
    then
        echo "success"
    else
        echo "failed"
        exit
    fi
done
