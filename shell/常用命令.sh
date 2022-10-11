# hive命令

curl -G "node1:12321/executor?action=activate" && echo
 

hive --service metastore

bin/hive --service hiveserver2

nohup hive --service metastore 2>&1 &
nohup hive --service hiveserver2 2>&1 &

beeline -u jdbc:hive2://node1:10000 -n root

load data local inpath '/opt/module/hive/datas/test.txt' into table test;

CREATE TABLE hive_hbase_emp_table( empno int,
   ename string,
   job string,
   mgr int,
   hiredate string,
   sal double,
   comm double,
   deptno int
)STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:co mm,info:deptno")
TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");

CREATE EXTERNAL TABLE relevance_hbase_emp( empno int,
   ename string,
   job string,
   mgr int,
   hiredate string,
   sal double,
   comm double,
deptno int )
STORED BY
'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:ename,info:job,info:mgr,info:hiredate,info:sal,info:co mm,info:deptno")
TBLPROPERTIES ("hbase.table.name" = "hbase_emp_table");


create table if not exists student(
id int, name string
)
row format delimited fields terminated by '\t'

load data local inpath '/root/data_test/student.txt' into table default.student;


load data local inpath "/root/data_test/video" into table gulivideo_ori;


insert into table gulivideo_orc select * from gulivideo_ori;

create table gulivideo_ori(
  videoId string,
   uploader string,
   age int,
   category array<string>,
   length int,
   views int,
   rate float,
   ratings int,
   comments int,
   relatedId array<string>)
row format delimited fields terminated by "\t"
collection items terminated by "&"
stored as textfile;


create table gulivideo_user_orc(
   uploader string,
   videos int,
   friends int)
row format delimited
fields terminated by "\t"
stored as orc
tblproperties("orc.compress"="SNAPPY");

# hdfs 

hdfs dfs -text /yinjz/sparkOutput/part*



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