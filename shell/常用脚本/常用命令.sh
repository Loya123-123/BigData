# hive命令

curl -G "node1:12321/executor?action=activate" && echo

# hdfs 

hdfs dfs -text /yinjz/sparkOutput/part*

# yarn
yarn application
# 查看yarn正在运行的任务列表
yarn application -list
# kill掉yarn正在运行的任务
yarn application -kill application_1592962175770_0004
# 查找yarn已经完成的任务列表
yarn application -appStates finished -list
# 查找yarn所有任务列表
yarn application -appStates ALL -list



## 退出安全模式
hadoop dfsadmin -safemode leave

# spark

SPARK_HOME=/opt/spark
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512m \
--driver-cores 1 \
--executor-memory 512m \
--num-executors 2 \
--executor-cores 1 \
--class com.yinjz.sparkstudy.WordCount \
hdfs://mycluster/spark/app/Spark_Study-1.0-SNAPSHOT.jar \
hdfs://mycluster//yinjz/input/words.txt hdfs://mycluster/yinjz/outputsparkjar

# kafka
# 立即删除某个topic下的数据

./kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic CRMLog --config cleanup.policy=delete

# flink
#提交命令 bin/flink 的说明（1.11及以上版本）：
#1、flink命令，后面可以跟上一些动作： run、run-application、list、stop、cancel等
#2、run动作：
#    1） Generic Cli模式：（-t yarn-per-job，新版本 yarn-per-job的写法）
#            -t
#            -D参数名=参数值
#            以指定yarn队列为例：
#                -Dyarn.application.queue=队列名
#    2） yarn cluster模式：（-m yarn-cluster，其实就是 yarn-per-job老版本的写法）
#            以指定yarn队列为例：
#                -yqu 指定yarn队列
#    3） default模式：（比如说 standalone模式）
#            -m 指定Jobmanager（地址：web端口） => hadoop1:8081
#3、run-application动作（Application模式）
#    指定参数，也是使用 -D参数名=参数值
#        以指定yarn队列为例：
#            -Dyarn.application.queue=队列名
#参数列表官网连接：https://ci.apache.org/projects/flink/flink-docs-release-1.13/deployment/config.html#yarn



# 建表语句
CREATE TABLE events (
  user STRING,
  url STRING,
  ts BIGINT,
  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS time_ltz - INTERVAL '5' SECOND
  ) WITH (   ... )
    ;