package com.atguigu.transformation;

import com.atguigu.source.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFunctionLambdaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //map 函数使用 Lambda 表达式，返回简单类型，不需要进行类型声明
        DataStream<String> stream1 = clicks.map(event -> event.url);
        stream1.print();

        // flatMap 使用 Lambda 表达式，抛出异常
        DataStream<String> stream2 = clicks.flatMap((Event event, Collector<String>
                out) -> {
            out.collect(event.url);
        }).returns(Types.STRING);
        stream2.print();

        env.execute();
    }
}
