#!/bin/bash
/usr/sbin/sshd
sysctl -p

#Make master accept all incoming connections
echo "host    all         all         0.0.0.0/0               trust" >> /home/gpadmin/hawq-data-directory/segmentdd/pg_hba.conf

#install nfs utils
yum install -y nfs-utils rpcbind

#start master
su gpadmin -c "source /home/gpadmin/hawq/greenplum_path.sh  && hawq start segment && ps -ef | grep postgres"

#check segmen is alive or not, if segment is down restart the docker container.
/home/gpadmin/check_segment.sh
