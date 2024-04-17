package com.echo.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //流批处理
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
    }
}
