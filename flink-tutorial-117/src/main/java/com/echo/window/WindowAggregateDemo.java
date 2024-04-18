package com.echo.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 第一条数据来创建窗口，创建累加器
 * 来一条计算一条
 * 窗口输出时调用get R e su l t
 *
 */
public class WindowAggregateDemo {
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

        /**
         *  第一个参数，输入数据的类型
         *  第二个参数，中间计算结果的类型
         *  第三个参数，输出的类型
         */
        SingleOutputStreamOperator<String> aggregate = windowAll.aggregate(new AggregateFunction<String, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器");
                return 0;
            }

            @Override
            public Integer add(String s, Integer integer) {
                System.out.println("调用add/聚合逻辑");
                return 22;
            }

            @Override
            public String getResult(Integer integer) {
                System.out.println("获取最终结果，窗口触发时输出");
                return integer.toString();
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                System.out.println("这个方法只有会话窗口会用到");
                return null;
            }
        });
        aggregate.print();
//        windowAll.aggregate();
        //全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
//        windowAll.process();


        environment.execute();
    }
}
