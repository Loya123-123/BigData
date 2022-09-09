package com.yinjz.kafka.producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.yinjz.kafka.KafkaConf.KafkaProducerConf;
import static com.yinjz.kafka.KafkaConf.Kafka_conf;

public class CustomProducerAcks {

    public static void main(String[] args) {

        Properties properties = Kafka_conf();
        // acks
        properties.put(ProducerConfig.ACKS_CONFIG,"1");

        // 重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);

        KafkaProducer kafkaProducer = KafkaProducerConf(properties);
        // 2 发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","yinjzack"+i));
        }

        // 3 关闭资源
        kafkaProducer.close();
    }
}
