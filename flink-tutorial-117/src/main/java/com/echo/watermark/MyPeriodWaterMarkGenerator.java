package com.echo.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class MyPeriodWaterMarkGenerator<T> implements WatermarkGenerator<T> {


    ///乱序等待时间
    private long delayTs;
    ///用来保存当前为止的最大的事件时间
    private long maxTs;

    public MyPeriodWaterMarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + this.delayTs +1;
    }

    /**
     *
     * @param t
     * @param l 提取到的数据的时间
     * @param watermarkOutput
     */
    @Override
    public void onEvent(T t, long l, WatermarkOutput watermarkOutput) {

        maxTs = Math.max(maxTs, l);

    }

    /**
     * 周期性调用，用于发射生成watermark
     * @param watermarkOutput
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

        watermarkOutput.emitWatermark(new Watermark(maxTs-delayTs-1));
    }
}
