package com.echo.process;

import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 定时器
 * keyed才有
 * 事件时间定时器，通过watermark来触发
 * 在process中获取当前watermark,显示的是上一次的watermark，因为process还没有接收到这条数据对应生成的watermark
 *
 */
public class ProcessKeyedDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> source = getData(environment);
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((value, ts) -> value.getTs() * 1000L);
        SingleOutputStreamOperator<WaterSensor> watermarksDS = source.assignTimestampsAndWatermarks(watermarkStrategy);
        KeyedStream<WaterSensor, String> keyedStream = watermarksDS.keyBy(WaterSensor::getId);
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context context, Collector<String> collector) throws Exception {


                //定时器
                TimerService timerService = context.timerService();
                //注册定时器
                timerService.registerEventTimeTimer(5000L);

                /**
                 * 在processElement获取当前watermark时，获取到的是上一个值，因为process方法具有单独性，即数据进来了，但是对应的watermark还在后面等待进入
                 */
                long watermark = timerService.currentWatermark();

                collector.collect(waterSensor.getId());
            }

            /**
             * 时间进展到定时器注册的时间，调用该方法
             * @param timestamp 当前时间进展
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println("现在时间是"+timestamp);
            }
        }).print();
        environment.execute();
    }

    public static DataStreamSource<WaterSensor> getData(StreamExecutionEnvironment environment) {
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
        return source;
    }
}
