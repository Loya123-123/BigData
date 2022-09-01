#!/bin/bash

case $1 in
"start"){
    for i in node1 node2 node3
    do
        echo " --------启动 $i Kafka-------"
        ssh $i "/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties "
    done
};;
"stop"){
    for i in node1 node2 node3
    do
        echo " --------停止 $i Kafka-------"
        ssh $i "/opt/kafka/bin/kafka-server-stop.sh"
    done
};;
esac

