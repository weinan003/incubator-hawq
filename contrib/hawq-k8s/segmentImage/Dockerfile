#pull base image
FROM wengyanqing/hawq:7.2

MAINTAINER amy

#ssh sshd
RUN /usr/sbin/sshd


#set password
RUN (echo "changeme";sleep 1;echo "changeme") | passwd root
RUN (echo "changeme";sleep 1;echo "changeme") | passwd gpadmin

#copy hawq-site.xml to docker image
COPY ./hawq-site.xml /home/gpadmin/hawq/etc/

#check segment is alive
COPY ./check_segment.sh /home/gpadmin/
RUN chmod +x /home/gpadmin/check_segment.sh

#reset keernel sysctl
COPY ./docker-entrypoint.sh /bin/docker-entrypoint.sh
RUN chmod +x /bin/docker-entrypoint.sh
ENTRYPOINT ["/bin/docker-entrypoint.sh"]

