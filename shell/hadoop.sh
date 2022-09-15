#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input.start、stop"
    exit ;
fi
case $1 in
"start")
        echo " =================== 启动 hadoop集群 ==================="
         sh zk.sh start
         ssh root@node1 "/opt/hadoop/sbin/start-all.sh"
         ssh root@node1 "/opt/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver"
;;
"stop")
        echo " =================== 关闭 hadoop集群 ==================="
        ssh root@node1 "/opt/hadoop/sbin/mr-jobhistory-daemon.sh stop historyserver"
        ssh root@node1 "/opt/hadoop/sbin/stop-all.sh"
;;
*)
    echo "Input Args Error..."
;;
esac

