#!/bin/bash
if [ $# -lt 1 ]
then
    echo "No Args Input.start、stop、status"
    exit ;
fi

case $1 in
"start")

        echo "=====================  $i  ======================="
        ssh $i "source /etc/profile && /opt/zookeeper/bin/zkServer.sh start"

;;
"stop")

        ssh $i "source /etc/profile && /opt/zookeeper/bin/zkServer.sh stop"

;;
"status")
        for i in node1 node2 node3
    do
        echo "=====================  $i  ======================="
        ssh $i "source /etc/profile && /opt/zookeeper/bin/zkServer.sh status"
    done
;;
*)
    echo "Input Args Error..."
;;
esac