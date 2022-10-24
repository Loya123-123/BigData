package com.yinjz.flink.TableSQL_11;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

public class TopNExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " `user` STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        // 普通Top N，选取当前所有用户中浏览量最大的2个
//
//        Table topNResultTable = tableEnv.sqlQuery("SELECT user, cnt, row_num " +
//                "FROM (" +
//                "   SELECT *, ROW_NUMBER() OVER (" +
//                "      ORDER BY cnt DESC" +
//                "   ) AS row_num " +
//                "   FROM (SELECT user, COUNT(url) AS cnt FROM clickTable GROUP BY user)" +
//                ") WHERE row_num <= 2");
//
//        tableEnv.toChangelogStream(topNResultTable).print("top 2: ");

        // 窗口Top N，选取当前所有用户中浏览量最大的2个
        String sqlQueryWindow = "SELECT user, COUNT(url) AS cnt ,window_start , window_end" +
                " FROM TABLE ( " +
                "TUMBLE(TABLE  clickTable ,DESCRIPTOR(et),INTERVAL '10' SECOND ))" +
                " GROUP BY user ,window_start , window_end" ;

        Table topNResultTableWindow = tableEnv.sqlQuery(
                "SELECT user, cnt, row_num,window_end " +
                "FROM (" +
                "   SELECT *, ROW_NUMBER() OVER (" +
                "      ORDER BY cnt DESC" +
                "   ) AS row_num " +
                "   FROM (" + sqlQueryWindow + ")" +
                ") WHERE row_num <= 2");

        tableEnv.toChangelogStream(topNResultTableWindow).print("win top 2: ");

        env.execute();
    }
}
