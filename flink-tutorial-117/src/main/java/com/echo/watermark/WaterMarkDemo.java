package com.echo.watermark;

import com.echo.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/***
 * 有序水位线设置
 */
public class WaterMarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> dataStreamSource = environment.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 1),
                new WaterSensor("s1", 3L, 2),
                new WaterSensor("s1", 4L, 2),
                new WaterSensor("s1", 5L, 2),
                new WaterSensor("s1", 6L, 2),
                new WaterSensor("s1", 7L, 2),
                new WaterSensor("s1", 8L, 2),
                new WaterSensor("s1", 9L, 2),
                new WaterSensor("s1", 10L, 2),
                new WaterSensor("s1", 20L, 2));
        //TODO 第一步 定义watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                //升序的watermark,没有等待时间
                .<WaterSensor>forMonotonousTimestamps()
                // 指定时间戳分配器，从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                ///l 一般是固定的，用不上
                System.out.println("数据="+waterSensor+",recordTs="+l);
                ///返回的时间戳要毫秒
                return waterSensor.getTs() * 1000L;
            }
        });
        ///TODO 第二步
        SingleOutputStreamOperator<WaterSensor> watermarksDS = dataStreamSource.assignTimestampsAndWatermarks(watermarkStrategy);


        /**
         * 这种展示不了效果，因为数据是同时来的，得用socket才能看出效果
         */
        watermarksDS.keyBy(waterSensor -> waterSensor.getId())
                //TODO 第三步，指定使用事件时间语义的窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                                long startTs = context.window().getStart();
                                long endTs = context.window().getEnd();
                                String s1 = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss:SSS");
                                String s2 = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss:SSS");
                                long count = iterable.spliterator().estimateSize();
                                collector.collect("key  = " + s+ "的窗口["+s1+","+s2+")包含"+count+"条数据");
                            }
                        });
        //        process.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
//        process.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));
        environment.execute();

    }
}
//
//1.获取环境
//2。获取source
//3。计算
//4。输出
//5 执行