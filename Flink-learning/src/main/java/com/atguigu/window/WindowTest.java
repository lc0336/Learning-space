package com.atguigu.window;

import com.atguigu.source.Event;
import com.atguigu.source_function.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                .countWindow(1); //滑动窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))); /滑动窗口
//                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8))); //滚动窗口
//                .window(TumblingEventTimeWindows.of(Time.hours(1))); // 滚动事件窗口


        env.execute();
    }
}
