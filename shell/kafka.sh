#!/bin/bash

case $1 in
"start"){

        echo " --------启动 Kafka-------"
        ssh root@node1 "xcall /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties "
};;
"stop"){

        echo " --------停止 Kafka-------"
        ssh root@node1 "xcall /opt/kafka/bin/kafka-server-stop.sh"

};;
esac

