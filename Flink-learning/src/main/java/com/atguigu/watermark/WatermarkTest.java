package com.atguigu.watermark;

import com.atguigu.source.Event;
import com.atguigu.source_function.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.time.Duration;

public class WatermarkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //周期时间为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置
//        env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.addSource(new ClickSource())
                // 插入水位线的逻辑模拟
                .assignTimestampsAndWatermarks(
                        // 针对乱序流插入水位线，延迟时间设置为 5s
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 抽取时间戳的逻辑
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );


        eventSingleOutputStreamOperator.print();

        env.execute();
    }
}