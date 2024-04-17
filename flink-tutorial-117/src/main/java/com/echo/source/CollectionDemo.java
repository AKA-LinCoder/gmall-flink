package com.echo.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //从集合读取数据
        DataStreamSource<Integer> streamSource = environment.fromCollection(Arrays.asList(1, 2, 34));
        DataStreamSource<Integer> streamSource1 = environment.fromElements(1, 2, 45);
        streamSource.print();
        streamSource1.print();
        environment.execute();
    }
}
