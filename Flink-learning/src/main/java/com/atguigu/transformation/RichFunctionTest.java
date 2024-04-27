package com.atguigu.transformation;

import com.atguigu.source.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// 一个常见的应用场景就是，如果我们希望连接到一个外部数据库进行读写操作，那么将连
// 接操作放在 map()中显然不是个好选择——因为每来一条数据就会重新连接一次数据库；所以
// 我们可以在 open()中建立连接，在 map()中读写数据，而在 close()中关闭连接

public class RichFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );

        // 将点击事件转换成长整型的时间戳输出
        clicks.map(new RichMapFunction<Event, Long>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println(" 索 引 为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务开始");
                    }

                    @Override
                    public Long map(Event value) throws Exception {
                        return value.timestamp;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println(" 索 引 为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
                    }
                })
                .print();
        env.execute();
    }
}