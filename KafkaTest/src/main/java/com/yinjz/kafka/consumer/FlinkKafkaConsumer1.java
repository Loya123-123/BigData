package com.yinjz.kafka.consumer;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkKafkaConsumer1 {
    public static void main(String[] args) throws Exception {
// 0 初始化 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 1 kafka 消费者配置信息
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "node1:9092,node2:9092,node3:9092");
// 2 创建 kafka 消费者
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>(
                "first", new SimpleStringSchema(), properties
        );

// 3 消费者和 flink 流关联
        env.addSource(kafkaConsumer).print();
// 4 执行
        env.execute();
    }
}