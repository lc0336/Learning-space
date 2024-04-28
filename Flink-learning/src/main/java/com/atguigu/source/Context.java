package com.atguigu.source;

import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.util.OutputTag;

public abstract class Context {
    public abstract Long timestamp();
    public abstract TimerService timerService();
    public abstract <X> void output(OutputTag<X> outputTag, X value);
}