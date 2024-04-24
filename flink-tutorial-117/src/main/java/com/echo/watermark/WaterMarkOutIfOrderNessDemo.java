package com.echo.watermark;

import com.echo.bean.WaterSensor;
import com.echo.process.ProcessKeyedDemo;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 乱序水位线
 */
public class WaterMarkOutIfOrderNessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> source = ProcessKeyedDemo.getData(environment);
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

        OutputTag<WaterSensor> waterSensorOutputTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));

        //TODO 设置窗口大小
        WindowedStream<WaterSensor, String, TimeWindow> window = watermarksDS.keyBy(waterSensor -> waterSensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //允许推迟2S关窗
                .allowedLateness(Time.seconds(2))
                //关窗后的迟到数据，放入测输出流
                .sideOutputLateData(waterSensorOutputTag);
        SingleOutputStreamOperator<String> process = window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {
                long startTs = context.window().getStart();
                long endTs = context.window().getEnd();
                String s1 = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss:SSS");
                String s2 = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss:SSS");
                long count = iterable.spliterator().estimateSize();
                collector.collect("key  = " + s + "的窗口[" + s1 + "," + s2 + ")包含" + count + "条数据");
            }
        });
        //主流输出
        process.print();
        SideOutputDataStream<WaterSensor> sideOutput = process.getSideOutput(waterSensorOutputTag);
        //测流输出
        sideOutput.printToErr();

        environment.execute();

    }
}
/**
 * 内置watermark的原理
 *  都是周期性生成的： 默认200ms
 *  升序的有序流：watermark = 当前最大的事件时间-1ms
 *  乱序流：watermark = 当前最大的事件时间-延迟时间-1ms
 *
 * 乱序：数据的顺序乱了，出现时间小的比时间大的晚来
 * 迟到：数据的时间戳<当前的watermark
 *
 *  窗口允许迟到 =>推迟关窗时间，在关窗之前。迟到的数据来了。还能被窗口计算，来一条迟到数据触发一次计算
 *  =>关窗后迟到数据不会被计算
 *
 *
 *  如果watermark等待3s，窗口允许迟到2S，为什么不知道watermark等待5S，或者允许迟到5S
 *  -》 watermark等待时间不会设太大，影响计算延迟
 *  -》 窗口允许迟到，是对大部分迟到数据的处理，尽量让结果准确，如果只设置允许5s，那么会导致频繁重新输出
 *
 *  TODO 设置经验
 *  1 watermark等待时间，设置一个不算特别大的，一般是秒级，在乱序和延迟取舍
 *  2 设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端小部分迟到的数据放到侧输出流
 *
 */
