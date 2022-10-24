package com.yinjz.flink.operator_5;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransPhysicalPatitioningTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 1. 随机分区
//        stream.shuffle().print("shuffle").setParallelism(4);
//
//        // 2. 轮询分区
//        stream.rebalance().print("rebalance").setParallelism(4);
//
//        // 3. rescale重缩放分区
        // 这里使用了并行数据源的富函数版本
//        env.addSource(new RichParallelSourceFunction<Integer>() {
//                    @Override
//                    public void run(SourceContext<Integer> sourceContext) throws Exception {
//                        for (int i = 1; i <= 8; i++) {
//                            // 将奇数发送到索引为1的并行子任务
//                            // 将偶数发送到索引为0的并行子任务
//                            if ( i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                                sourceContext.collect(i);
//                            }
//                        }
//                    }
//
//                    @Override
//                    public void cancel() {
//
//                    }
//                })
//                .setParallelism(2)
//                .rescale()
//                .print().setParallelism(4);

//        // 4. 广播
//        stream.broadcast().print("broadcast").setParallelism(4);
//
//        // 5. 全局分区
//        stream.global().print("global").setParallelism(4);
//
//        // 6. 自定义重分区
//        // 将自然数按照奇偶分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);

        env.execute();
    }
}
