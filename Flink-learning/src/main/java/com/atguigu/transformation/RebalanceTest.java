package com.atguigu.transformation;

import com.atguigu.source.Event;
import com.atguigu.source_function.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经轮询重分区后打印输出，并行度为 4
        stream.rebalance().print("rebalance").setParallelism(3);
        env.execute();
    }
}
