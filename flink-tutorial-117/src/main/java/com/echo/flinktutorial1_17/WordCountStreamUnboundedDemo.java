package com.echo.flinktutorial1_17;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
