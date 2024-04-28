package com.atguigu.checkpoint;

import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointConfigDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 配置存储检查点到 JobManager 堆内存
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        // 配置存储检查点到文件系统
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://namenode:40010/flink/checkpoints"));

        // 启用检查点，间隔时间 1 秒
        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置精确一次模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 最小间隔时间 500 毫秒
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 超时时间 1 分钟
        checkpointConfig.setCheckpointTimeout(60000);
        // 同时只能有一个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 开启检查点的外部持久化保存，作业取消后依然保留
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 启用不对齐的检查点保存方式
        checkpointConfig.enableUnalignedCheckpoints();
        // 设置检查点存储，可以直接传入一个 String，指定文件系统的路径
        checkpointConfig.setCheckpointStorage("hdfs://my/checkpoint/dir");

        // 保存点
        env.setDefaultSavepointDirectory("hdfs:///flink/savepoints");

        env.execute();
    }
}
