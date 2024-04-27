package com.atguigu.transformation;

import com.atguigu.source.Event;
import com.atguigu.source_function.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 最简单的重分区方式就是直接“洗牌”。通过调用 DataStream 的.shuffle()方法，将数据随
// 机地分配到下游算子的并行任务中去。
public class ShuffleTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经洗牌后打印输出，并行度为 4
        stream.shuffle().print("shuffle").setParallelism(2);
        env.execute();
    }
}
