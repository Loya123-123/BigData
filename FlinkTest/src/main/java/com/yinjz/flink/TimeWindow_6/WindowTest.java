package com.yinjz.flink.TimeWindow_6;

import com.yinjz.flink.operator_5.ClickSource;
import com.yinjz.flink.operator_5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;


public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        stream.map(new MapFunction<Event, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user,1L);
                    }
                })
                .keyBy(data -> data.f0)
//                .countWindow(10,2) // 滑动计数
                // 滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 滑动窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                // 时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.hours(2)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return Tuple2.of(stringLongTuple2.f0,stringLongTuple2.f1 + t1.f1);
                    }
                })
                .print("开窗测试： ") ;


                env.execute();
    }
}
