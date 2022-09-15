package com.yinjz.springbootbigdata.Controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/***
 * Kafka 模板用来向 kafka 发送数据
 */
@RestController
public class ProducerController {
    @Autowired
    KafkaTemplate<String, String> kafka;

    @RequestMapping("/yinjz")
    public String data(String msg) {
        kafka.send("first", msg);
        return "ok";
    }
}