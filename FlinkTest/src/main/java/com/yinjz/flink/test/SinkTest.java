package com.yinjz.flink.test;

import com.yinjz.flink.operator_5.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
        eventDataStreamSource.print();
        env.execute();
    }
}
