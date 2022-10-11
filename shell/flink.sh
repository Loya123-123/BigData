#!/bin/bash

case $1 in
"start"){

        echo " --------启动 flink-------"
        ssh root@node1 "/opt/flink/bin/start-cluster.sh"
};;
"stop"){

        echo " --------停止 flink-------"
        ssh root@node1 "/opt/kafka/bin/stop-cluster.sh"

};;
esac

