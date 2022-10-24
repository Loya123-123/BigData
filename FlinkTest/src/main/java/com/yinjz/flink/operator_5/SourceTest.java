package com.yinjz.flink.operator_5;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

public class SourceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("FlinkTest/input/clicks.csv");

        // 2. 从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3. 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        // 4. 从Socket文本流读取
        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);

//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");
//        stream3.print("3");
//        stream4.print("4");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.239.14.120:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test5");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> kafkaSteam = env.addSource(new FlinkKafkaConsumer<String>("first", new SimpleStringSchema(), properties));
        kafkaSteam.print();
        env.execute();
    }
}
