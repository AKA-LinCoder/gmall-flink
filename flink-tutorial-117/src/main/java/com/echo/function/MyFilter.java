package com.echo.function;

import com.echo.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class MyFilter implements FilterFunction<WaterSensor> {

    public String id;

    public MyFilter(String id){
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor o) throws Exception {
        return id.equals(o.getId());
    }
}
