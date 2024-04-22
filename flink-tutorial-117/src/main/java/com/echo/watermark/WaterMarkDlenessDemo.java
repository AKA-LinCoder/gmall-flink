package com.echo.watermark;

import com.echo.partition.MyPartition;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMarkDlenessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        // 自定义分区器，数据%分区数，只输入奇数，都只会去往map的一个子任务
        SingleOutputStreamOperator<Integer> watermarks = environment.fromElements("1", "3", "5","11").partitionCustom(new MyPartition(), new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        }).map(r -> Integer.parseInt(r))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner((r, ts) -> r * 1000L)
                        //设置空闲等待时间（处理时间）
                        .withIdleness(Duration.ofSeconds(5)));
        //分成两组,奇数一组，偶数一组，开10s的事件时间滚动窗口
        watermarks.keyBy(r->r%2).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
            @Override
            public void process(Integer integer, ProcessWindowFunction<Integer, String, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<String> collector) throws Exception {
                long startTs = context.window().getStart();
                long endTs = context.window().getEnd();
                String s1 = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss:SSS");
                String s2 = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss:SSS");
                long count = iterable.spliterator().estimateSize();
                collector.collect("key  = " + integer+ "的窗口["+s1+","+s2+")包含"+count+"条数据");
            }
        }).print();
        environment.execute("");
    }
}
