package com.yinjz.flink.TimeWindow_6;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.yinjz.flink.operator_5.ClickSource;
import com.yinjz.flink.operator_5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class WindowAggregate_PVUV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 所有数据设置相同的key，发送到同一个分区统计PV和UV
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPvUv())
                .print();

        env.execute();
    }
    // 自定义 HashSet 去重 Long 计数
    private static class AvgPvUv implements AggregateFunction<Event,Tuple2<HashSet<String>,Long>, Tuple2<Integer,Long>> {
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            // 初始化累加器
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(Event event, Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
            // 属于本窗口的数据来一条累加一次，并返回累加器
            hashSetLongTuple2.f0.add(event.user);
            return Tuple2.of(hashSetLongTuple2.f0,hashSetLongTuple2.f1 + 1);
        }

        @Override
        public Tuple2<Integer,Long> getResult(Tuple2<HashSet<String>, Long> hashSetLongTuple2) {
            // 窗口闭合时，增量聚合结束，将计算结果发送到下游
            return Tuple2.of(hashSetLongTuple2.f0.size(), hashSetLongTuple2.f1);
//            return (double) hashSetLongTuple2.f1 / hashSetLongTuple2.f0.size();
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
            return null;
        }
    }

}

