package com.atguigu.transformation;


import com.atguigu.source.Event;
import com.atguigu.source_function.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// 广播（broadcast）
//这种方式其实不应该叫做“重分区”，因为经过广播之后，数据会在不同的分区都保留一
//份，可能进行重复处理。可以通过调用 DataStream 的 broadcast()方法，将输入数据复制并发送
//到下游算子的所有并行任务中去。

public class BroadcastTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 经广播后打印输出，并行度为 4
        stream.broadcast().print("broadcast").setParallelism(4);
        env.execute();
    }
}