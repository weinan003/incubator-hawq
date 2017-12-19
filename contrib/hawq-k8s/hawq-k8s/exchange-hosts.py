import sys, os
import commands


def get_segment_pods():
    command_segment_hostnames = "kubectl get pods | grep hawq-segment | awk -F ' ' '{print $1}'"
    segment_hostnames = commands.getoutput(command_segment_hostnames)
    return segment_hostnames.split("\n")

def get_segment_IP_hostname():
    seg_hostnames = get_segment_pods()
    seg_dict = {}
    seg_IP_list = []
    seg_hostname_list = []
    for seg in seg_hostnames:
        command_segment_IP = "kubectl describe pods %s | grep IP | awk -F ' ' '{print $2}'" % seg
        segment_IP = commands.getoutput(command_segment_IP)
        seg_IP_list.append(segment_IP)
        seg_hostname_list.append(seg)
        #seg_dict[str(segment_IP)] = str(seg)
        print "segment IP , segment name: \n" , segment_IP, seg
    return seg_IP_list, seg_hostname_list

def get_master_hostname():
    command_master_hostname = "kubectl get pods | grep hawq-master | awk -F ' ' '{print $1}'"
    master_hostname = commands.getoutput(command_master_hostname)
    print master_hostname
    return master_hostname

def get_master_IP():
    master_hostname = get_master_hostname()
    command_master_IP = "kubectl describe pods %s | grep IP | awk -F ' ' '{print $2}'" % master_hostname
    master_IP = commands.getoutput(command_master_IP)
    return master_IP

def rm_segetc_from_masteretc():
    master_hostname = get_master_hostname()
    command_rm_segetc = "kubectl exec -it %s -- su root -c 'cp /etc/hosts /etc/hosts_bak && sed -in '/segment/d' /etc/hosts_bak && cat /etc/hosts_bak > /etc/hosts'" % master_hostname
    rm_segetc = commands.getoutput(command_rm_segetc)
    command_check_result = "kubectl exec -it %s -- cat /etc/hosts" % master_hostname
    check_result = commands.getoutput(command_check_result)
    print "check rm_segetc_from_masteretc :" , check_result

"""
def generate_master_etc_dic()
    master_hostname = get_master_hostname()
    command_get_etc_hosts = "kubectl exec -it %s -- cat /etc/hosts" % master_hostname
    get_etc_hosts = commands.getoutput(command_get_etc_hosts).split("\n")
    etc_hosts_dic = {}
    for ehost in get_etc_hosts:
        if ehost.find("hawq_segment") != -1 :
           print ehost
           etc_hosts_dic[str(ehost.split('\t')[0])] = str(ehost.split('\t')[1])
    print etc_hosts_dic
    return etc_hosts_dic
"""         

def generate_etc_hosts():
    rm_segetc_from_masteretc()
    master_hostname = get_master_hostname()
    seg_IP_list, seg_hostname_list = get_segment_IP_hostname()
    for i in range(0, len(seg_IP_list)):
        etc_host = seg_IP_list[i] + '\t' + seg_hostname_list[i]
        command_add_ect_host = "kubectl exec -it %s -- su root -c 'echo \"%s\" >> /etc/hosts'" % (master_hostname, etc_host)
        add_etc_host = commands.getoutput(command_add_ect_host)
        print "generate_etc_hosts check each etc host" , etc_host
    command_check_etc_hosts = "kubectl exec -it %s -- cat /etc/hosts" % master_hostname
    check_etc_hosts = commands.getoutput(command_check_etc_hosts)
    return check_etc_hosts
    print "check master /etc/hosts: \n" , check_etc_hosts
   
def exchange_segment_etc(master_etc_hosts):
    seg_hostnames = get_segment_pods()
    for seg in seg_hostnames:
        command_exchange_hosts = "kubectl exec -it %s -- su root -c 'echo \"%s\" > /etc/hosts'" % (seg, master_etc_hosts)
        exchange_hosts = commands.getoutput(command_exchange_hosts)
        command_check_etc_host = "kubectl exec -it %s -- cat /etc/hosts" % seg
        check_etc_host = commands.getoutput(command_check_etc_host)
        print "check segment /etc/hosts: \n", check_etc_host

def restart_cluster():
    master_hostname = get_master_hostname()
    seg_hostnames = get_segment_pods()
    command_restart_master = "kubectl exec -it %s -- su gpadmin -c 'source /home/gpadmin/hawq/greenplum_path.sh && hawq stop master -M immediate -a && hawq start master -a'" % master_hostname
    restart_master = commands.getoutput(command_restart_master)
    print restart_master
    for seg in seg_hostnames:
        command_restart_segment = "kubectl exec -it %s -- su gpadmin -c 'source /home/gpadmin/hawq/greenplum_path.sh && hawq stop segment -M immediate -a && hawq start segment -a'" % seg
        restart_segment = commands.getoutput(command_restart_segment)
        print restart_segment

def update_master_address():
    # hard code
    master_IP = get_master_IP()
    seg_IP_list, seg_hostname_list =  get_segment_IP_hostname()
    for seg in seg_hostname_list:
        command_del_master_address = "kubectl exec -it %s -- sed -in '/hawq_master_address_host/{n;d}' /home/gpadmin/hawq/etc/hawq-site.xml" % seg
        command_add_master_address = "kubectl exec -it %s -- sed -in '25i\\\t\\\t<value>%s</value>' /home/gpadmin/hawq/etc/hawq-site.xml" % (seg, master_IP)
        del_master_address = commands.getoutput(command_del_master_address)
        add_master_address = commands.getoutput(command_add_master_address)

if __name__ == '__main__':

    master_etc_hosts = generate_etc_hosts()
    exchange_segment_etc(master_etc_hosts)
    update_master_address()
    restart_cluster()

