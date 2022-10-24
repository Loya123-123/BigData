package com.yinjz.flink.State_9;

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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample_Checkpoint {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 每隔多长时间做一次检查点
        env.enableCheckpointing(10000L);
        // 创建检查点
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(""));
        //

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 配置存储检查点到 JobManager 堆内存
        checkpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        // 检查点模式    AT_LEAST_ONCE、EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 下一个检查点和上一个检查点间隔500毫秒
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 超时时间
        checkpointConfig.setCheckpointTimeout(60000);
        // 最大并发检查点数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 开启外部持久化存储
        // DELETE_ON_CANCELLATION: 在作业取消的时候会自动删除外部检查点, 但是如果是作业失败退出,则会保留检查点。
        // RETAIN_ON_CANCELLATION: 作业取消的时候也会保留外部检查点
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 不对齐检查点 要求检查点模式(CheckpointingMode)必须为 exctly-once,并且并发的检查点个数为 1。
        checkpointConfig.enableUnalignedCheckpoints();


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        // 定义当前类的属性，批量
        private final int threshold;
        //
        private transient ListState<Event> checkpointedState;
        // 缓存数据
        private List<Event> bufferedElements;
        //
        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Event element: bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(element);
                }
                System.out.println("==========输出完毕=========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    Types.POJO(Event.class));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 如果是从故障中恢复，就将ListState中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
