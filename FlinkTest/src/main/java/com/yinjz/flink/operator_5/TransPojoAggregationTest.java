package com.yinjz.flink.operator_5;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransPojoAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./cart", 3000L),
                new Event("Mary", "./fav", 4000L),
                new Event("Bob", "./cart", 10000L),
                new Event("Mary", "./cart", 30000L),
                new Event("Mary", "./fav", 4000L)
        );

        stream.keyBy(new KeySelector<Event,String> (){
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        })
//                .max("timestamp")
                .maxBy("timestamp")
                .print(" maxBy :");

        stream.keyBy(e -> e.user)
//                 .maxBy("timestamp")
                .max("timestamp")
                // 指定字段名称
                .print("max:");

        env.execute();
    }
}

