package com.echo.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowReduceiDemo {
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
        ///TODO 指定窗口分配器，指定用哪一种窗口
        AllWindowedStream<String, TimeWindow> windowAll = dataStreamSource.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));//滚动窗口，窗口长度10s
        //TODO 指定窗口汗水，数据处理逻辑

        //增量聚合：来一条数据处理一条数据，窗口触发的时候输出计算结果
//        windowAll.reduce();
        //相同key的第一条数据来的时候不会调用reduce方法啊
        //增量聚合，来一条数据就会计算一次，但是不会输出结果
        //在窗口触发的时候，才会输出窗口的最终计算结果
        SingleOutputStreamOperator<String> reduce = windowAll.reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String s, String t1) throws Exception {
                return "reduce了";
            }
        });
        reduce.print();
//        windowAll.aggregate();
        //全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
//        windowAll.process();


        environment.execute();
    }
}
