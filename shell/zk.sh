#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input.start、stop、status"
    exit ;
fi

case $1 in
"start")

        ssh root@node1 "xcall /opt/zookeeper/bin/zkServer.sh start"
;;
"stop")

        ssh root@node1 "xcall /opt/zookeeper/bin/zkServer.sh stop"

;;
"status")
       ssh root@node1 "xcall /opt/zookeeper/bin/zkServer.sh status"
;;
*)
    echo "Input Args Error..."
;;
esac