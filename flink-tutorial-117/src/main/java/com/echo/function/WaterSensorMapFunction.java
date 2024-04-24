package com.echo.function;


import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String,WaterSensor> {

    @Override
    public WaterSensor map(String s) throws Exception {
        String[] parts = s.split(",");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Invalid input string format");
        }
        String field1 = parts[0];
        Long field2 = Long.parseLong(parts[1]);
        int field3 = Integer.parseInt(parts[2]);
        return new WaterSensor(field1, field2, field3);
    }
}