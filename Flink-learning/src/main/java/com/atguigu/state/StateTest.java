package com.atguigu.state;

import com.atguigu.process_function.EventTimeTimerTest;
import com.atguigu.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new EventTimeTimerTest.CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long
                                    recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMapFunction())
                .print();
    }

    public static class MyFlatMapFunction extends RichFlatMapFunction<Event, String> {
        // 声明状态
        private transient ValueState<Event> state;

        @Override
        public void open(Configuration config) {
            // 在 open 生命周期方法中获取状态
            ValueStateDescriptor<Event> descriptor = new ValueStateDescriptor<>(
                    "my state", // 状态名称
                    Event.class // 状态类型
            );
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 更新状态
            System.out.println(state.value());
            state.update(value);
            System.out.println("value" + state.value());
        }
    }
}
