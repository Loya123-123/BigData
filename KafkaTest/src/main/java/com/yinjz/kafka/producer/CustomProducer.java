package com.yinjz.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static com.yinjz.kafka.KafkaConf.KafkaProducerConf;
import static  com.yinjz.kafka.KafkaConf.Kafka_conf;

public class CustomProducer {
    public static void main(String[] args) {
        Properties properties = Kafka_conf();
        KafkaProducer kafkaProducer = KafkaProducerConf(properties);
        // 4. 调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "yinjz " + i));
        }
        // 5. 关闭资源
        kafkaProducer.close();
    }
}