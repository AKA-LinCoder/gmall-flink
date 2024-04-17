package com.echo.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 合并数据流，一次能合并多条数据流，只要数据格式相同
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> streamSource = environment.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> streamSource1 = environment.fromElements(4, 5, 6);
        DataStream<Integer> union = streamSource.union(streamSource1);
        union.print();
        environment.execute();
    }
}
