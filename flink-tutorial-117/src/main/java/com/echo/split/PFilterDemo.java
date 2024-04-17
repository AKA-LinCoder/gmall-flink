package com.echo.split;

import com.echo.partition.MyPartition;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
 分流： 奇数，偶数拆分成不同流
 */
public class PFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStreamSource<Integer> streamSource = environment.fromElements(1,2,3,4,5,6);

        /**
         * 使用filter缺点：同有关数据要被处理两次
         */
        streamSource.filter(value->value %2 ==0).print();
        streamSource.filter(value -> value %2==1).print();

//        streamSource.

        environment.execute();
    }
}
