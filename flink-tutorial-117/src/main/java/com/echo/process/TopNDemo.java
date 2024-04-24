package com.echo.process;

import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> streamSource = getData(environment);
        SingleOutputStreamOperator<WaterSensor> watermarksDS = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((r, ts) -> r.getTs() * 1000L));

        //TODO 按照vc分组，开窗，聚合(增量计算+全量打标签)
        //开窗聚合后，就是普通的流，没有了窗口信息，需要自己打上窗口的标记 windowEnd
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowGG = watermarksDS.keyBy(value -> value.getVc())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<WaterSensor, Integer, Integer>() {
                               @Override
                               public Integer createAccumulator() {
                                   return 0;
                               }

                               @Override
                               public Integer add(WaterSensor waterSensor, Integer integer) {
                                   return integer + 1;
                               }

                               @Override
                               public Integer getResult(Integer integer) {
                                   return integer;
                               }

                               @Override
                               public Integer merge(Integer integer, Integer acc1) {
                                   return null;
                               }
                           },
                        /**
                         * 第一个参数：输入类型=增量函数的输出
                         * 第二个参数：输出类型
                         * 第三个参数：key的类型
                         * 第四个 窗口类型
                         */
                        new ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>() {
                            @Override
                            public void process(Integer integer, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> iterable, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {

                                //迭代器里面只有一条数据，next一次即可
                                Integer count = iterable.iterator().next();
                                collector.collect(Tuple3.of(integer, count, context.window().getEnd()));
                            }
                        });

        //TODO 开窗聚合后，就是普通的流，没有了窗口信息，需要自己打上窗口的标记 windowEnd,保证同一个窗口时间范围的结果，到一起去
        KeyedStream<Tuple3<Integer, Integer, Long>, Long> keyedStream = windowGG.keyBy(s -> s.f2);
        keyedStream.process(new TopN(2)).print();



        environment.execute();

    }


    public static class TopN extends  KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>{
        //存不同窗口的统计结果，key=windowEnd,value = list数据
        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;
        //要取的top数量
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            dataListMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> integerIntegerLongTuple3, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context context, Collector<String> collector) throws Exception {
            //进入这个方法，只是一条数据，要排序，得到齐才能处理
            //TODO 存hashmap
            Long windowEnd = integerIntegerLongTuple3.f2;
            if(dataListMap.containsKey(windowEnd)){
                //包含vc,不是该vc的第一条，直接添加到list中
                List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
                dataList.add(integerIntegerLongTuple3);
            }else {
                //是第一条
                List<Tuple3<Integer, Integer, Long>> dataList = new ArrayList<>();
                dataList.add(integerIntegerLongTuple3);
                dataListMap.put(windowEnd,dataList);
            }
            //TODO 注册定时器，windowEnd+1ms即可，同一个窗口范围，应该同时输出，只不过是一条一条调用processElement,只需要延迟1ms接即可
            context.timerService().registerEventTimeTimer(windowEnd+1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ///定时器触发，同一个窗口范围的计算结果就完整了，开始排序，取topN
            Long windowEnd = ctx.getCurrentKey();
            //排序
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            dataList.sort(new Comparator<Tuple3<Integer, Integer, Long>>() {
                @Override
                public int compare(Tuple3<Integer, Integer, Long> o1, Tuple3<Integer, Integer, Long> o2) {
                    return o2.f1-o1.f1;
                }
            });
            //取topN
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("++++++++++++++");
            for (int i = 0; i < Math.min(threshold, dataList.size()); i++) {
                Tuple3<Integer, Integer, Long> tuple3s = dataList.get(i);
                stringBuffer.append("TOP"+(1+i)+"\n");
                stringBuffer.append("vc="+tuple3s.f0+"\n");
                stringBuffer.append("count="+tuple3s.f1+"\n");
                stringBuffer.append("窗口结束时间"+tuple3s.f2+"\n");
                stringBuffer.append("++++++++++++++");
            }
            //用完的list,及时清理，节省资源
            dataList.clear();

            out.collect(stringBuffer.toString());

        }
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
