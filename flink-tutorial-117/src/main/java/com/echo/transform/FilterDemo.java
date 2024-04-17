package com.echo.transform;

import com.echo.bean.WaterSensor;
import com.echo.function.MyFilter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> dataStreamSource = environment.fromElements(new WaterSensor("s1", 1L, 1), new WaterSensor("s2", 1L, 1), new WaterSensor("s3", 1L, 1));
        dataStreamSource.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return Objects.equals(waterSensor.getId(), "s1");
            }
        }).print();
        dataStreamSource.filter(waterSensor -> waterSensor.getId().equals("s2")).print();
        dataStreamSource.filter(new MyFilter("s2"));
        environment.execute();
    }
}
