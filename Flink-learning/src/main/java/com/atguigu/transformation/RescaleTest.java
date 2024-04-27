package com.atguigu.transformation;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// 重缩放分区（rescale）
//重缩放分区和轮询分区非常相似。当调用 rescale()方法时，其实底层也是使用 Round-Robin
//算法进行轮询，但是只会将数据轮询发送到下游并行任务的一部分中。也就
//是说，“发牌人”如果有多个，那么 rebalance 的方式是每个发牌人都面向所有人发牌；而 rescale
//的做法是分成小团体，发牌人只给自己团体内的所有人轮流发牌。

public class RescaleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 这里使用了并行数据源的富函数版本
        // 这样可以调用 getRuntimeContext 方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 0; i < 8; i++) {
                            // 将奇数发送到索引为 1 的并行子任务
                            // 将偶数发送到索引为 0 的并行子任务
                            if ((i + 1) % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i + 1);
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                })
                .setParallelism(2)
                .rescale()
                .print().setParallelism(4);
        env.execute();
    }
}