package com.echo.transform;

import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlaMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<WaterSensor> dataStreamSource = environment.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s3", 1L, 1));
        //flapMap :一进多出
        dataStreamSource.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {

                if(waterSensor.getId().equals("s1")){
                    collector.collect(waterSensor.getVc() + "");
                } else if (waterSensor.getId().equals("s2")) {
                    collector.collect(waterSensor.getTs().toString());
                    collector.collect(waterSensor.getVc().toString());
                }
            }
        }).print();

        environment.execute();
    }
}
