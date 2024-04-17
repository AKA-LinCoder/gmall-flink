package com.echo.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区
 */
public class MyPartition implements Partitioner<String> {
    @Override
    public int partition(String o, int numPartitions) {
        return Integer.parseInt(o)%numPartitions;
    }
}
