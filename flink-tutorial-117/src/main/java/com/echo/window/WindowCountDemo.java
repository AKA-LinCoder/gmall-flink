package com.echo.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class WindowCountDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<Integer> streamSource1 = environment.fromElements(1, 2, 3,4,5,6,7,8);
        KeyedStream<Integer, Integer> keyedStream = streamSource1.keyBy(value -> value);

        WindowedStream<Integer, Integer, GlobalWindow> countWindow = keyedStream.countWindow(5);//滚动窗口，窗口长度5条数据
        WindowedStream<Integer, Integer, GlobalWindow> integerIntegerGlobalWindowWindowedStream = keyedStream.countWindow(4, 2);//每经过一个步长，就会有一个窗口触发
        countWindow.process(new ProcessWindowFunction<Integer, Integer, Integer, GlobalWindow>() {
            @Override
            public void process(Integer integer, ProcessWindowFunction<Integer, Integer, Integer, GlobalWindow>.Context context, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                System.out.println(context.window().maxTimestamp());
                collector.collect(integer);
            }
        }).print();

        environment.execute();
    }
}
/**
 * 窗口什么时候触发，输出
 *  时间进展>=窗口的最大时间戳（end-1ms）
 *
 *  窗口是怎么划分的
 *  start = 当前时间向下取整,窗口长度的整数倍
 *  end = start+窗口长度
 *  窗口左闭右开
 *  窗口的生命周期
 *      创建：属于本窗口的第一条数据来的时候创建，放入一个singleton单例的集合中
 *      销毁（关窗）：时间进展>=窗口的最大时间戳（end-1ms）+允许迟到时间(默认0)
 */