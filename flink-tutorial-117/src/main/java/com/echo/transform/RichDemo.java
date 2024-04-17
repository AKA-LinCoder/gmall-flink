package com.echo.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> dataStreamSource = environment.fromElements(1, 2, 3);
        SingleOutputStreamOperator<String> streamOperator = dataStreamSource.map(new RichMapFunction<Integer, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println(getRuntimeContext().getNumberOfParallelSubtasks());
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public String map(Integer integer) throws Exception {
                return (integer+1) +"";
            }
        });
        streamOperator.print();
        environment.execute();
    }
}
