package com.echo.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 连接两个流，输出能根据ID匹配上的数据
 */
public class ConnectKeyByemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);
        //多并行度下，需要设置keyby才能正常运行，才能保证key相同的到一块
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyBy = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);
        /**
         * 两条流，不一定谁的数据先来
         * 两条流，有数据来，存到一个变量中
         * 每条流有数据来的时候，除了存变量中，去另一条流存到变量查找是否有匹配上的
         */
        SingleOutputStreamOperator<String> process = keyBy.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            //定义hashmap来存数据
            Map<Integer,List<Tuple2<Integer,String>>> s1Cache = new HashMap<>();

            Map<Integer,List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> integerStringTuple2, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                Integer id = integerStringTuple2.f0;
                if(!s1Cache.containsKey(id)){
                    //如果key不存在，就put到变量中
                    ArrayList<Tuple2<Integer, String>> list = new ArrayList<>();
                    list.add(integerStringTuple2);
                    s1Cache.put(id,list);
                }else{
                    //不是第一条数据
                    s1Cache.get(id).add(integerStringTuple2);
                }
                if(s2Cache.containsKey(id)){
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        collector.collect("s1:"+integerStringTuple2 +">>>>"+"s2:"+s2Element);
                    }
                }

            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> integerStringIntegerTuple3, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                Integer id = integerStringIntegerTuple3.f0;
                if(!s2Cache.containsKey(id)){
                    //如果key不存在，就put到变量中
                    ArrayList<Tuple3<Integer, String, Integer>> list = new ArrayList<>();
                    list.add(integerStringIntegerTuple3);
                    s2Cache.put(id,list);
                }else{
                    //不是第一条数据
                    s2Cache.get(id).add(integerStringIntegerTuple3);
                }
                if(s1Cache.containsKey(id)){
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        collector.collect("s1:"+s1Element +">>>>"+"s2:"+integerStringIntegerTuple3);
                    }
                }
            }
        });
        process.print();
        env.execute();

    }
}
