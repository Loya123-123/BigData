package com.yinjz.flink.operator_5;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.239.14.120:9092");

        DataStreamSource<String> stream = env.readTextFile("FlinkTest/input/clicks.csv");

        stream.addSink(new FlinkKafkaProducer<>(
                        "first",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}
