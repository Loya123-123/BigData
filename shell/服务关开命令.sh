# hdfs 初始化
hdfs namenode -format


# HA 格式化
hdfs --daemon start journalnode

hdfs namenode -bootstrapStandby
 

hdfs --daemon start namenode
 

# hdfs
# datanode 启动
hadoop-daemon.sh start datanode
# namenode 启动
hadoop-daemon.sh start namenode




# 注意：NameNode和ResourceManger如果不是同一台机器，不能在NameNode上启动 YARN，应该在ResouceManager所在的机器上启动YARN。
# hdfs 开启
sbin/start-dfs.sh
# hdfs 关闭
sbin/stop-dfs.sh

# node 2 yarn 开启
sbin/start-yarn.sh
# node 2 yarn 关闭
sbin/stop-yarn.sh

start-all.sh

stop-all.sh

# 日志服务开启
mr-jobhistory-daemon.sh start historyserver

mr-jobhistory-daemon.sh stop historyserver

hadoop-daemons.sh start

yarn-daemon.sh start resourcemanager/nodemanager

# hive 服务
nohup hive --service metastore 2>&1 &
nohup hive --service hiveserver2 2>&1 &

beeline -u jdbc:hive2://node1:10000 -n root

# zeppelin


# spark

/opt/spark/sbin/start-all.sh 

/opt/spark/sbin/stop-all.sh

/opt/spark/sbin/start-history-server.sh

/opt/spark/sbin/stop-history-server.sh



/opt/spark/sbin/start-thriftserver.sh \
--hiveconf hive.server2.thrift.port=10000 \
--hiveconf hive.server2.thrift.bind.host=node1 \
--master local[2]


# hbase 
/opt/hbase/bin/start-hbase.sh

/opt/hbase/bin/stop-hbase.sh

# flink
# flink 启动命令
/opt/flink/bin/start-cluster.sh
# flink 关闭命令
/opt/flink/bin/stop-cluster.sh


# zookeeper  单节点操作
# 启动 zookeeper 服务
zkServer.sh start

# 停止zookeeper服务 
zkServer.sh stop

# zookeeper 状态
zkServer.sh status

# kafka 
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties

bin/kafka-topics.sh --bootstrap-server 10.239.14.120:9092 --create --partitions 1 --replication-factor 3 --topic first
bin/kafka-topics.sh --bootstrap-server 10.239.14.120:9092 --describe --topic first
bin/kafka-topics.sh --bootstrap-server node1:9092 --alter --topic first --partitions 3
bin/kafka-console-producer.sh --bootstrap-server 10.239.14.120:9092 --topic first
bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic first

bin/kafka-server-stop.sh
 

# hudi
# Hudi支持Spark 2.x版本，建议使用2.4.4+版本的Spark。

# 本篇文章Fayson主要介绍如何基于CDH6.3.2版本编译Hudi




create view "test"(id varchar primary key,"info1"."name" varchar, "info2"."address" varchar);
 create 'test_number','info';
put 'test_number','1001','info:number',Bytes.toBytes(1000); 
scan 'test_number',{COLUMNS => 'info:number:toLong'};

