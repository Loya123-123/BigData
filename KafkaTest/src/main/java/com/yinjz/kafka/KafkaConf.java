package com.yinjz.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Properties;

public class KafkaConf {


    public static Properties Kafka_conf() {

        // 1. 创建 kafka 生产者的配置对象
        Properties properties = new Properties();
        // 2. 给 kafka 配置对象添加配置信息:bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.239.14.120:9092");
        // key,value 序列化(必须):key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        return properties ;
    }
    public static  KafkaProducer KafkaProducerConf(Properties properties) {

        // 3. 创建 kafka 生产者对象
        KafkaProducer<Object, String> kafkaProducer = new KafkaProducer<>(properties);
        return kafkaProducer ;
    }
    public static KafkaConsumer KafkaConsumerConf(Properties properties) {
        // 3. 创建 kafka 生产者对象
        KafkaConsumer<Object, String> KafkaConsumer = new KafkaConsumer<>(properties);
        return KafkaConsumer ;
    }


}
