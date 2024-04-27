package com.atguigu.source_function;

import com.atguigu.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 不同数据源数据
public class SourceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//
//        // 1.从文件中读取数据
//        DataStreamSource<String> stream1 = env.readTextFile("D:\\data\\idea\\learning-space\\Flink-learning\\input\\clicks.txt");
//        stream1.print();
//
//        //2.从集合中读取
//
//        ArrayList<Integer> nums = new ArrayList<>();
//        nums.add(1);
//        nums.add(2);
//        DataStreamSource<Integer> stream2 = env.fromCollection(nums);
//
//        ArrayList<Event> clicks = new ArrayList<>();
//        clicks.add(new Event("Mary", "./home", 1000L));
//        clicks.add(new Event("Bob", "./cart", 2000L));
//        DataStream<Event> stream3 = env.fromCollection(clicks);
//
//        stream2.print();
//        stream3.print();
//
//
//        // 3， 从元素中读取
//        DataStreamSource<Event> stream4 = env.fromElements(new Event("Mary", "./home", 1000L),
//                new Event("Mary", "./home", 1000L));
//        stream4.print();
//
//        //4. 从socket
//        DataStreamSource<String> stream5 = env.socketTextStream("localhost", 777);
//        stream5.print();

        //有了自定义的 source function，调用 addSource 方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("SourceCustom");

        env.execute();
    }
}
