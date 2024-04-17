package com.echo.aggreagte;

import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class keyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> dataStreamSource = environment.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s3", 1L, 1));
        ///按照id分组
        //只是对数据重分区，不是转化算子
        //keyby是对数据分组，保证相同key的数据在同一个分区
        //分区 一个子任务，可以理解成一个分区
        //一个分区中可能有多个子任务
        dataStreamSource.keyBy(waterSensor -> waterSensor.getId()).print();
        KeyedStream<WaterSensor, String> keyedStream = dataStreamSource.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });
        //传位置索引的，适用于tuple
        //max/maxby: max 只会取比较字段的最大值，非比较字段保留第一次的值
        //maxby,会取比较字段的最大值，同时非比较字段也取最大值
        keyedStream.maxBy("vc");
        keyedStream.sum("vc").print();
        //reduce 两两聚合
        // 第一条数据不会聚合
        // 前提有分区，然后第一条不进，相同区的第二条开始进入reduce方法，进行计算操作并返回
        keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                //第一个参数之前的计算结果
                //第二个参数：现在来的数据
                return null;
            }
        });
        environment.execute();
    }
}
