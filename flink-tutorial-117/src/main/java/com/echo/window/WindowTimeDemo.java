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
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class WindowTimeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果有N个并行度，最大值为a
        //例如 2，100，就会从0和50开始，每个并行度生成50个
        environment.setParallelism(2);
        /**
         * 数据生成器参数
         *  第一个：输入类型固定是long
         *  第二个：自动生成的数字序列最大值
         *  第三个：限速策略
         *  第四个：返回类型
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {
                        return "number" + aLong;
                    }
                }, 100, RateLimiterStrategy.perSecond(10), Types.STRING
        );
        DataStreamSource<String> dataStreamSource = environment.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "");
        KeyedStream<String, Integer> keyedStream = dataStreamSource.keyBy(value -> value.hashCode());
        WindowedStream<String, Integer, TimeWindow> window = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)));

        WindowedStream<String, Integer, TimeWindow> timeWindowWindowedStream = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<String>() {
            @Override
            public long extract(String s) {
                ///在这里可以自定义间隔时间，每条来的数据都会更新间隔时间
                return 0;
            }
        }));



        SingleOutputStreamOperator<String> process1 = timeWindowWindowedStream.process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
            /**
             *
             * @param integer 分组的key
             * @param context 上下文
             * @param iterable 存的数据
             * @param collector 采集器
             * @throws Exception
             */
            @Override
            public void process(Integer integer, ProcessWindowFunction<String, String, Integer, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {

                //通过context可以拿到window等信息
                collector.collect(iterable.spliterator().estimateSize() + "");

            }
        });
        process1.print();

        environment.execute();
    }
}
