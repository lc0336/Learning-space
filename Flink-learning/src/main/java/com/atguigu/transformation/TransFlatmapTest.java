package com.atguigu.transformation;

import com.atguigu.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmapTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        stream.flatMap(new MyFlatMap()).print();

        stream.flatMap((Event value, Collector<String> out) -> {
                    if (value.user.equals("Mary")) {
                        out.collect(value.user);
                    } else if (value.user.equals("Bob")) {
                        out.collect(value.user);
                        out.collect(value.url);
                    }
                })
                .returns(new TypeHint<String>() {}).print();
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }
}
