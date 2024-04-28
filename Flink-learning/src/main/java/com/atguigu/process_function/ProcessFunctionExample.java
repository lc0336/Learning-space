package com.atguigu.process_function;

import com.atguigu.source.Event;
import com.atguigu.source_function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

// ProcessFunction 数据处理
public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
        );

        SingleOutputStreamOperator<String> process = stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if (value.user.equals("Mary")) {
                    out.collect(value.user);
                } else if (value.user.equals("Bob")) {
                    out.collect(value.user);
                    out.collect(value.user);
                }
                System.out.println("timestamp---" + ctx.timestamp());
                System.out.println(ctx.timerService().currentWatermark());
            }
        });

        process.print();
        env.execute();
    }
}