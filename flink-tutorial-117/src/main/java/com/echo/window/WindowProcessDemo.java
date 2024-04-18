package com.echo.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class WindowProcessDemo {
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
                }, 100, RateLimiterStrategy.perSecond(1), Types.STRING
        );
        DataStreamSource<String> dataStreamSource = environment.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "");
        KeyedStream<String, Integer> keyedStream = dataStreamSource.keyBy(value -> value.hashCode());
        WindowedStream<String, Integer, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        ///TODO 指定窗口分配器，指定用哪一种窗口
        AllWindowedStream<String, TimeWindow> windowAll = dataStreamSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));//滚动窗口，窗口长度10s
        //TODO 指定窗口汗水，数据处理逻辑

        SingleOutputStreamOperator<String> apply = windowAll.apply(new AllWindowFunction<String, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
                if(iterable.iterator().next().equals("number1")){

                    collector.collect(iterable.iterator().next());
                }

            }
        });
        apply.print();
        SingleOutputStreamOperator<Integer> process = windowAll.process(new ProcessAllWindowFunction<String, Integer, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<String, Integer, TimeWindow>.Context context, Iterable<String> iterable, Collector<Integer> collector) throws Exception {

                if(iterable.iterator().next().equals("number2")){
                    collector.collect(996);
                }
            }
        });
        process.print();


        SingleOutputStreamOperator<String> process1 = window.process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
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
