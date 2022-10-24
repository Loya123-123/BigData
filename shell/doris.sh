#!/bin/bash

case $1 in
"start"){

        echo " --------启动 flink-------"
#        ssh root@node1 "/opt/flink/bin/start-cluster.sh"
        /data/doris/fe/bin/start_fe.sh --daemon
        /data/doris/be/bin/start_be.sh --daemon
        /data/doris/apache-doris-dependencies-1.2.1-bin-x86_64/apache_hdfs_broker/bin/start_broker.sh --daemon
};;
"stop"){

        echo " --------停止 flink-------"

        /data/doris/fe/bin/stop_fe.sh --daemon
        /data/doris/be/bin/stop_be.sh --daemon
        /data/doris/apache-doris-dependencies-1.2.1-bin-x86_64/apache_hdfs_broker/bin/stop_broker.sh --daemon
};;
esac

