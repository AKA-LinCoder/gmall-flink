package com.echo.transform;

import com.echo.bean.WaterSensor;
import com.echo.function.MyMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> dataStreamSource = environment.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 1L, 1), new WaterSensor("s3", 1L, 1));
        //map 一进一出
        //方式1： lambda表达式
        dataStreamSource.map(element->element.vc+1).print();
        //方式2：匿名实现类
        dataStreamSource.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        }).print();
        //方式3：定义一个类

        dataStreamSource.map(new MyMapFunction()).print();


        environment.execute();
    }



}
