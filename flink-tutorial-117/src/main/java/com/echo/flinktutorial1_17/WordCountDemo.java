package com.echo.flinktutorial1_17;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataSet 实现wordCount：读文件有界流
 */
public class WordCountDemo {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        //处理有限的、静态的数据集
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //处理无限的、动态的数据流
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取数据
        DataSource<String> lineDS = env.readTextFile("flink-tutorial-117/input/word.txt");
        // 切分转换
        FlatMapOperator<String, Tuple2<String, Integer>> flatMapDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //3.1 按照空格切分
                String[] strings = s.split(" ");
                //3.2 将单词转换为(word,1)
                for (String word : strings) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    collector.collect(tuple2);
                }

            }
        });
        // 按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = flatMapDS.groupBy(0);
        // 各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);
        // 输出
        sum.print();

    }
}
