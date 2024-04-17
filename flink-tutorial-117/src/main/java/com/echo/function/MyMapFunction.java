package com.echo.function;

import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class  MyMapFunction implements MapFunction<WaterSensor,String> {

        @Override
        public String map(WaterSensor waterSensor) throws Exception {
            return waterSensor.getId();
        }
    }