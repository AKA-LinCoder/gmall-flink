package com.echo.flinktutorial1_17;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 算子并行度优先级>整体设置并行度优先级>提交时指定>配置参数
 * 算子之间的传输关系：一对一 ，重分区
 * 算子串在仪器的条件： 一对一，并行度相同
 */
public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = environment.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDs = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strings = s.split(" ");
                for (String string : strings) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(string, 1);
                    collector.collect(tuple2);
                }
            }
        });
        flatMapDs.keyBy(value->value.f0).sum(1).print();
        environment.execute("");

    }
}
