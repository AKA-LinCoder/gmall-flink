package com.echo.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 只支持事件时间
 * 指定上界下界的偏移
 * process中，只能处理join上的数据
 * 两条流关联后的watermark，以两条流中最小的为准
 * 如果当前数据的事件时间《当前的watermark，就是迟到数据，主流的process不会处理
 * between之后可以将迟到数据输出到侧输出流
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = environment.fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                        Tuple2.of("c", 4))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L));


        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> ds2 = environment.fromElements(
                        Tuple3.of("a", 1,1),
                        Tuple3.of("a", 11,1),
                        Tuple3.of("b", 12,7),
                        Tuple3.of("c", 14,9))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer,Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L));
        //TODO interval join
        KeyedStream<Tuple2<String, Integer>, String> keyBy = ds1.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> keyBy1 = ds2.keyBy(r -> r.f0);
        OutputTag<Tuple2<String, Integer>> tuple2OutputTag = new OutputTag<>("late-left", Types.TUPLE(Types.STRING,Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> tuple3OutputTag = new OutputTag<>("late-right", Types.TUPLE(Types.STRING,Types.INT,Types.INT));
        SingleOutputStreamOperator<String> process = keyBy.intervalJoin(keyBy1)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(tuple2OutputTag)
                .sideOutputRightLateData(tuple3OutputTag)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context context, Collector<String> collector) throws Exception {

                        //两条流的数据匹配上才会
                        collector.collect(stringIntegerTuple2 + "=======" + stringIntegerIntegerTuple3);

                    }
                });
        process.print();
        process.getSideOutput(tuple2OutputTag).printToErr();
        process.getSideOutput(tuple3OutputTag).printToErr();


        environment.execute();

    }
}
