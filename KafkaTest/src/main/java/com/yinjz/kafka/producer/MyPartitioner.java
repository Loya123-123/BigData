package com.yinjz.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
/**
 * 1. 实现接口Partitioner
 * 2. 实现 3 个方法:partition,close,configure * 3. 编写 partition 方法,返回分区号
 */
public class MyPartitioner implements Partitioner {
    /**
    * 返回信息对应的分区 * @param topic
    * @param key
    * @param keyBytes * @param value
    * @param valueBytes
    * @param cluster
    * @return
    主题
    消息的 key
    消息的 key 序列化后的字节数组 消息的 value
    消息的 value 序列化后的字节数组 集群元数据可以查看分区信息
*/
    @Override
    public int   partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 获取数据 atguigu  hello
        String msgValues = value.toString();

        int partition;

        if (msgValues.contains("yinjz")){
            partition = 0;
        }else {
            partition = 1;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
