package com.echo.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {
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
        //TODO window join
        ds1.join(ds2)
                        .where(r->r.f0)
                                .equalTo(l->l.f0)
                                        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                                                    @Override
                                                    public String join(Tuple2<String, Integer> stringIntegerTuple2, Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3) throws Exception {
                                                        //关联上的数据，才会走到这里
                                                        //1。同一个区间（时间范围）的才会匹配
                                                        return stringIntegerTuple2+"======="+stringIntegerIntegerTuple3;
                                                    }
                                                }).print();


        environment.execute();

    }
}
