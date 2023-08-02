package com.yinjz.flink.CDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkSQLCDC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//                "'url' = 'jdbc:mysql://localhost:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC'," +

        //2.使用FLINKSQL DDL模式构建CDC 表
        tableEnv.executeSql("create table test (" +
                "id String primary Key ," +
                "name String " +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'hostname' = 'localhost'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '12345678'," +
                "'database-name' = 'test'," +
                "'table-name' = 'test' )");
        String sql = "select id,name from test" ;
        //3.查询数据并转换为流输出
        Table table = tableEnv.sqlQuery(sql);

        table.printSchema();
        tableEnv.toChangelogStream(table).print(" ");
        //4.启动
        env.execute(" ");

    }

}
