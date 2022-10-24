package com.yinjz.flink.operator_5;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.yinjz.flink.test.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.addSink(
                JdbcSink.sink(
                        "INSERT INTO clicks (user, url) VALUES (?, ?)",
                        (statement, r) -> {
                            statement.setString(1, r.user);
                            statement.setString(2, r.url);
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/test")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("12345678")
                                .build()
                )
        );
        env.execute();
    }
}

