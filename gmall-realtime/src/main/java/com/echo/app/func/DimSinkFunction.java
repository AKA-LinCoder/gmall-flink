package com.echo.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        super.invoke(value, context);
    }
}
