package com.echo.watermark;

import com.echo.bean.WaterSensor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 乱序水位线
 */
public class WaterMarkOutIfOrderNessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> source = environment.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 1L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 22L, 2),
                new WaterSensor("s2", 22L, 2),
                new WaterSensor("s2", 12L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s1", 3L, 3));
        //TODO 设置水位线策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.
        //设置最大乱序时间
                <WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor waterSensor, long l) {
                System.out.println("data = "+waterSensor);
                return waterSensor.getTs()*1000L;
            }
        });
        //TODO 绑定
        SingleOutputStreamOperator<WaterSensor> watermarksDS = source.assignTimestampsAndWatermarks(watermarkStrategy);
        //TODO 设置窗口大小
        WindowedStream<WaterSensor, String, TimeWindow> window = watermarksDS.keyBy(waterSensor -> waterSensor.getId()).window(TumblingEventTimeWindows.of(Time.seconds(10)));
        window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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

        environment.execute();

    }
}
/**
 * 内置watermark的原理
 *  都是周期性生成的： 默认200ms
 *  升序的有序流：watermark = 当前最大的事件时间-1ms
 *  乱序流：watermark = 当前最大的事件时间-延迟时间-1ms
 */
