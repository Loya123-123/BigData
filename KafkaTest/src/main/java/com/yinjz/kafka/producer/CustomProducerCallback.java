package com.yinjz.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static com.yinjz.kafka.KafkaConf.KafkaProducerConf;
import static com.yinjz.kafka.KafkaConf.Kafka_conf;

public class CustomProducerCallback {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = Kafka_conf();
        KafkaProducer kafkaProducer = KafkaProducerConf(properties);

// 4. 调用 send 方法,发送消息
        for (int i = 0; i < 5; i++) {
// 添加回调
            kafkaProducer.send(new ProducerRecord<>("first", "yinjz " + i), new Callback() {
                // 该方法在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata,
                                         Exception exception) {
                    if (exception == null) {
// 没有异常,输出信息到控制台
                        System.out.println(" 主 题 : " +
                                metadata.topic() + "->" + "分区:" + metadata.partition());
                    } else {
// 出现异常打印
                        exception.printStackTrace();
                    }
                }
            });
// 延迟一会会看到数据发往不同分区
            Thread.sleep(2);
        }
// 5. 关闭资源
        kafkaProducer.close();
    }
}

