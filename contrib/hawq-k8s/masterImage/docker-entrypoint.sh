#!/bin/bash
/usr/sbin/sshd
sysctl -p

#Make master accept all incoming connections
echo "host    all         all         0.0.0.0/0               trust" >> /home/gpadmin/hawq-data-directory/masterdd/pg_hba.conf

#start master
su gpadmin -c "source /home/gpadmin/hawq/greenplum_path.sh && hawq start master  && ps -ef | grep postgres"

#install nfs utils
yum install -y nfs-utils rpcbind

#check master is alive or not, if master is down then restart docker container.
/home/gpadmin/check_master.sh
