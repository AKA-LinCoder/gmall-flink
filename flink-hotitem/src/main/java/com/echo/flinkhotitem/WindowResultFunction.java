package com.echo.flinkhotitem;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用于输出窗口的结果
 */
public class WindowResultFunction implements AllWindowFunction<Long,ItemViewCount,TimeWindow>{




    @Override
    public void apply(TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
        Long count = iterable.iterator().next();
        collector.collect(ItemViewCount.of(1,timeWindow.getEnd(),count));

    }
}


