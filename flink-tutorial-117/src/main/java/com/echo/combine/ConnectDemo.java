package com.echo.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 同床异梦
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> streamSource = environment.fromElements(1, 2, 3, 4);
        DataStreamSource<String> streamSource1 = environment.fromElements("v","4","8");
        ConnectedStreams<Integer, String> connectedStreams = streamSource.connect(streamSource1);
        SingleOutputStreamOperator<String> streamOperator = connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return integer.toString();
            }

            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        });
        streamOperator.print();
        environment.execute();
    }
}
